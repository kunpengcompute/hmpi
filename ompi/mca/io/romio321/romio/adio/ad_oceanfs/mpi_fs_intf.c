#include "mpi_fs_intf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include "ad_oceanfs_pub.h"
#include "adio.h"


#ifdef DEBUG
#define ASSERT(f) assert(f)
#else
#define ASSERT(f) ((void)0)
#endif

void* mpi_zalloc(uint32_t n)
{
    if (n <= 0) {
        return NULL;
    }

    char* p = malloc(n);
    if (p == NULL) {
        return NULL;
    }
    memset(p, 0, n);
    return p;
}

#define mpi_free(p) \
    do { \
        free(p); \
        (p) = NULL; \
    } while (0)

#define atomic_t int
#define atomic_inc(v) __sync_fetch_and_add(v, 1)
#define atomic_dec_and_test(v) (__sync_fetch_and_sub(v, 1) == 1)
#define atomic_set(v, i) ((*(v)) = (i))


#define CHECK_NULL_POINTER(x, ret) \
    do { \
        if ((x) == NULL) \
        { \
            return (ret); \
        } \
    }while (0)

#define TRUE 1
#define FALSE 0

#ifndef MIN
    #define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif
#ifndef MAX
    #define MAX(a, b) ((a) < (b) ? (b) : (a))
#endif


#define MAX_VIEW_READ_SIZE (4 * 1024 * 1024)
#define MPI_FD_HANDLE_HASHTBL_SIZE 64
// IOCTL命令字，同时也定义在odc.h里
#define ODCS_IOC_MPI_VIEW_READ _IOWR('S', 101, mpi_fs_view_read_t)
#define ODCS_IOC_MPI_PREFETCH _IOWR('S', 102, mpi_fs_view_read_t)
#define ODCS_IOC_IOCTL_MPI_GROUP_ID    _IOWR('S', 103, mpi_fs_group_id_t)
#define ODCS_IOC_IOCTL_MPI_SET_GROUP_ID    _IOWR('S', 111, mpi_fs_group_id_t)

static pthread_mutex_t g_init_mpi_fh_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_init_mpi_fh_pid = 0;

typedef struct mpi_fs_group_id {
    int fd;
    uint64_t group_id;
} mpi_fs_group_id_t;

typedef enum mpi_fh_state {
    MPI_FH_STATE_INUSE,
    MPI_FH_STATE_DELETE
} mpi_fh_state_t;

typedef struct mpi_fs_view {
    off_t offset; // filetype 开始位置，相对于文件开头的偏移
    // 或文件批量读的起始位置
    u32 count; // filetype中连续块（block）的数量
    // 或文件批量读数据块个数
    u32 *blocklens; // 存储每个连续块长度（bytes）的数组，数组长度为count
    // 或文件批量读各数据块长度
    off_t *blockoffs; // 存储每个连续块开始位置偏移（bytes）的数组，数组长度为count
    // 或文件批量读各数据块偏移
    off_t ub_off; // filetype结束位置，其值为相对于文件开头的偏移
    // 文件批量读时，此参数无效
    char data[0]; // blocklens + blockoffs 的数据
} mpi_fs_view_t;

typedef struct mpi_fs_view_read {
    off_t offset;
    uint32_t readLen;
    uint32_t readRangeLen;
    uint32_t readRangeCount;
    uint32_t *blocklens;
    off_t *blockoffs;
    char data[0];
    // read buffer + read range buffer
    // read range buffer 
    // blocklens[count]
    // blockoffs[count]
} mpi_fs_view_read_t;

typedef struct list_head {
        struct list_head *next, *prev;   /**< 前驱后驱指针  */
} list_head_t;

typedef struct mpi_list_with_lock_s {
    list_head_t list;
} mpi_list_with_lock;

typedef struct mpi_fh_hashtbl {
    mpi_list_with_lock ht[MPI_FD_HANDLE_HASHTBL_SIZE];
} mpi_fh_hashtbl_t;

static mpi_fh_hashtbl_t g_mpi_fh_hashtbl;

/** \brief 初始化链表头
    \param[in] ptr 链表结构指针
*/
#define INIT_LIST_HEAD(ptr) \
    do { \
        (ptr)->next = (ptr); \
        (ptr)->prev = (ptr); \
    } while (0)

/** \brief 通过链表地址获得结构体指针
    \param[in] ptr 链表结构指针
    \param[in] type 结构体类型
    \param[in] member 结构体中链表所表示的字段
*/
#define list_entry(ptr, type, member) \
    ((type *)(void *)((char *)(ptr) - offsetof(type, member)))

static void list_add_tail(list_head_t *new_head, list_head_t *head)
{
    /* 添加到链表尾 */
    list_head_t *prev = head->prev;
    list_head_t *next = head;
        
    /* 添加到链表 */
    next->prev = new_head;
    new_head->next = next;
    new_head->prev = prev;
    prev->next = new_head;
}

/** \brief 删除指定链表元素并初始化
    \param[in] entry 待删除并初始化的链表元素
*/
static void list_del_init(struct list_head *entry)
{
    /* 从链表删除 */
    list_head_t *prev = entry->prev;
    list_head_t *next = entry->next;
    
    next->prev = prev;
    prev->next = next;
    
    INIT_LIST_HEAD(entry); 
}

typedef struct mpi_fd_handle {
    list_head_t node;
    int fd;
    pthread_mutex_t lock;
    atomic_t ref_count;
    mpi_fh_state_t state;
    mpi_fs_view_t *view;
} mpi_fh_handle_t;

static void init_mpi_hashtbl(mpi_list_with_lock *ht, int hash_size)
{
    if (ht == NULL) {
        return;
    }

    int i;
    for (i = 0; i < hash_size; i++) {
        INIT_LIST_HEAD(&ht[i].list);
    }
}

static int init_mpi_fh_table(void)
{
    pthread_mutex_lock(&g_init_mpi_fh_lock);
    if (getpid() == g_init_mpi_fh_pid) {
        pthread_mutex_unlock(&g_init_mpi_fh_lock);
        return 0;
    }

    init_mpi_hashtbl(g_mpi_fh_hashtbl.ht, MPI_FD_HANDLE_HASHTBL_SIZE);

    g_init_mpi_fh_pid = getpid();

    pthread_mutex_unlock(&g_init_mpi_fh_lock);

    return 0;
}

static int insert_mpi_fh_table(int fd)
{
    int ret = init_mpi_fh_table();
    if (ret != 0) {
        return ret;
    }

    if (fd < 0) {
        return -1;
    }

    mpi_fh_handle_t *fh = (mpi_fh_handle_t *)mpi_zalloc(sizeof(mpi_fh_handle_t));
    if (fh == NULL) {
        return -1;
    }

    fh->fd = fd;
    atomic_set(&fh->ref_count, 1);
    fh->state = MPI_FH_STATE_INUSE;
    pthread_mutex_init(&fh->lock, NULL);
    INIT_LIST_HEAD(&fh->node);

    int bucket = fd % MPI_FD_HANDLE_HASHTBL_SIZE;
    pthread_mutex_lock(&g_init_mpi_fh_lock);
    list_add_tail(&fh->node, &g_mpi_fh_hashtbl.ht[bucket].list);
    pthread_mutex_unlock(&g_init_mpi_fh_lock);

    return 0;
}

int mpi_fs_open(const char *pathname, int flags, mode_t mode)
{
    mode &= ~S_IFMT; // 去掉filetype位
    mode |= S_IFREG; // 设置为普通文件

    int fd = open(pathname, flags, mode);
    if (fd >= 0) {
        if (insert_mpi_fh_table(fd) != 0) {
            close(fd);
            return -1;
        }
    }

    return fd;
}

int mpi_fs_pread(int fd, void *buf, size_t count, off_t offset)
{
    return (int)pread(fd, buf, count, offset);
}

int mpi_fs_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    return (int)pwrite(fd, buf, count, offset);
}

int mpi_fs_stat(const char *pathname, struct stat *buf)
{
    return stat(pathname, buf);
}

static mpi_fh_handle_t *find_mpi_fh_in_ht(int fd, int is_del)
{
    mpi_fh_handle_t *fh = NULL;
    list_head_t *pos = NULL;
    list_head_t *next = NULL;

    int bucket = fd % MPI_FD_HANDLE_HASHTBL_SIZE;
    pthread_mutex_lock(&g_init_mpi_fh_lock);

    list_head_t* head = &g_mpi_fh_hashtbl.ht[bucket].list;
    for (pos = head->next, next = pos->next; pos != head; pos = next, next = pos->next) {
        fh = list_entry(pos, mpi_fh_handle_t, node);
        if (fd == fh->fd) {
            if (is_del) {
                list_del_init(&fh->node);
            }
            atomic_inc(&fh->ref_count);
            pthread_mutex_unlock(&g_init_mpi_fh_lock);
            return fh;
        }
    }
    pthread_mutex_unlock(&g_init_mpi_fh_lock);

    return NULL;
}

static void mpi_fh_put(mpi_fh_handle_t *fh)
{
    if (fh == NULL) {
        return;
    }

    if (!atomic_dec_and_test(&fh->ref_count)) {
        return;
    }

    pthread_mutex_lock(&fh->lock);
    mpi_fh_state_t status = fh->state;
    pthread_mutex_unlock(&fh->lock);

    if (status == MPI_FH_STATE_INUSE) {
        ASSERT(0);
        return;
    }

    if (status == MPI_FH_STATE_DELETE) {
        // 清除视图
        mpi_free(fh->view);
        mpi_free(fh);
    }
}

static void mpi_delete_fd_handle(int fd)
{
    int ret = init_mpi_fh_table();
    if (ret != 0) {
        return;
    }

    if (fd < 0) {
        return;
    }

    mpi_fh_handle_t *fh = find_mpi_fh_in_ht(fd, TRUE);
    if (fh == NULL) {
        return;
    }

    pthread_mutex_lock(&fh->lock);
    fh->state = MPI_FH_STATE_DELETE;
    pthread_mutex_unlock(&fh->lock);

    // find_fh_in_ht 会加引用计数，先减一次ref count
    mpi_fh_put(fh);

    // 删除fh
    mpi_fh_put(fh);
}

int mpi_fs_close(int fd)
{
    int ret = close(fd);
    if (ret == 0) {
        mpi_delete_fd_handle(fd);
    }

    return ret;
}

int mpi_fs_ftruncate(int fd, off_t length)
{
    return (int)ftruncate(fd, length);/*lint !e718 !e746*/
}

off_t mpi_fs_lseek(int fd, off_t offset, int whence)
{
    return lseek(fd, offset, whence);
}

static mpi_fs_view_t *AllocMPIFSView(uint32_t count)
{
    if (count == 0) {
        return NULL;
    }

    mpi_fs_view_t *view = mpi_zalloc(sizeof(mpi_fs_view_t) + count * sizeof(uint32_t) + count * sizeof(off_t));
    if (view == NULL) {
        return NULL;
    }

    view->count = count;
    view->blocklens = (u32 *)view->data;
    view->blockoffs = (off_t *)(view->data + count * sizeof(uint32_t));

    return view;
}

static mpi_fh_handle_t *mpi_fh_get(int fd)
{
    int ret = init_mpi_fh_table();
    if (ret != 0 || fd < 0) {
        return NULL;
    }

    return find_mpi_fh_in_ht(fd, FALSE);
}

int mpi_fs_set_fileview(int fd, off_t offset, u32 count, u32 *blocklens, off_t *blockoffs, off_t ub_off)
{
    int ret = 0;
    mpi_fh_handle_t *fh = mpi_fh_get(fd);
    if (fh == NULL) {
        errno = EINVAL;
        return -1;
    }

    mpi_fs_view_t *view = AllocMPIFSView(count);
    if (view == NULL) {
        mpi_fh_put(fh);
        errno = EINVAL;
        return -1;
    }

    pthread_mutex_lock(&fh->lock);
    // 释放老的view
    mpi_free(fh->view);

    fh->view = view;
    memcpy(fh->view->blocklens, blocklens, count * sizeof(uint32_t));
    memcpy(fh->view->blockoffs, blockoffs, count * sizeof(off_t));

    fh->view->offset = offset;
    fh->view->count = count;
    fh->view->ub_off = ub_off;
    pthread_mutex_unlock(&fh->lock);

    mpi_fh_put(fh);
    return 0;
}

static uint32_t GetBlockCntInMPIView(off_t readStart, uint32_t readLen, mpi_fs_view_t *mpiView)
{
    // 每个filetype的大小
    const uint32_t filetypeLen = mpiView->ub_off - mpiView->offset;
    const off_t displacement = mpiView->offset;

    // 当前在filetype中的偏移
    off_t posInFiletype = (readStart - displacement) % filetypeLen;
    // 正在读第几个块
    uint32_t cntBlockRead = 0;
    // 还需要读多少个字节
    uint32_t bytesLeft = readLen;

    uint32_t i;
    uint32_t bytesToRead;
    // 当前读取的块小于总的块数量
    while (bytesLeft > 0) {
        for (i = 0; i < mpiView->count; i++) {
            if (bytesLeft == 0) {
                break;
            }
            if (mpiView->blocklens[i] == 0) {
                // 空的blocklens
                continue;
            }
            if (posInFiletype <= mpiView->blockoffs[i]) {
                // 计划读一整块
                bytesToRead = mpiView->blocklens[i];
                // 检查是否够读
                bytesToRead = (bytesLeft > bytesToRead) ? bytesToRead : bytesLeft;
                // 更新剩余字节，只会减成0，不会减成负数
                bytesLeft -= bytesToRead;

                posInFiletype += bytesToRead;
                cntBlockRead++;
            } else if (posInFiletype < (mpiView->blockoffs[i] + mpiView->blocklens[i])) {
                // 计划读整块的部分
                bytesToRead = mpiView->blockoffs[i] + mpiView->blocklens[i] - posInFiletype;
                // 检查是否够读
                bytesToRead = (bytesLeft > bytesToRead) ? bytesToRead : bytesLeft;
                // 更新剩余字节，只会减成0，不会减成负数
                bytesLeft -= bytesToRead;

                posInFiletype += bytesToRead;
                cntBlockRead++;
            } else {
                // 读取的偏移超过当前视图块的位置，准备读下一个block
                continue;
            }
        }

        // 读完一个filetype，准备读下一个
        posInFiletype = 0;
    }

    return cntBlockRead;
}

static mpi_fs_view_read_t *AllocMPIViewRead(off_t offset, uint32_t readLen, uint32_t readRangeCount)
{
    mpi_fs_view_read_t *viewRead = NULL;
    uint32_t readRangeLen = readRangeCount * (sizeof(uint32_t) + sizeof(off_t));
    uint32_t viewReadSize = sizeof(mpi_fs_view_read_t) + readLen + readRangeLen;

    if (readRangeCount == 0) {
        return NULL;
    }

    if (readLen > MAX_VIEW_READ_SIZE || viewReadSize > MAX_VIEW_READ_SIZE) {
        return NULL;
    }

    viewRead = mpi_zalloc(viewReadSize);
    if (viewRead == NULL) {
        return NULL;
    }

    viewRead->offset = offset;
    viewRead->readLen = readLen;
    viewRead->readRangeLen = readRangeLen;
    viewRead->readRangeCount = readRangeCount;
    viewRead->blocklens = (uint32_t *)(viewRead->data + readLen);
    viewRead->blockoffs = (off_t *)(viewRead->data + readLen + (readRangeCount * sizeof(uint32_t)));

    return viewRead;
}

/* ****************************************************************************
 功能描述  : 根据MPI视图和读的范围生成批量读的范围
**************************************************************************** */
void MakeMPIViewReadRange(mpi_fs_view_read_t *viewRead, mpi_fs_view_t *mpiView)
{
    /*lint -e647*/
    uint32_t i;
    off_t readStart = viewRead->offset;
    uint32_t count = viewRead->readRangeCount;

    // 每个filetype的大小
    const uint32_t filetypeLen = mpiView->ub_off - mpiView->offset;
    const off_t displacement = mpiView->offset;

    // 当前在filetype中的偏移
    off_t posInFiletype = (readStart - displacement) % filetypeLen;
    // 第几个filetype
    uint32_t posFiletype = (readStart - displacement) / filetypeLen;
    // 正在读第几个块
    uint32_t cntBlockRead = 0;
    // 还需要读多少个字节
    uint32_t bytesLeft = viewRead->readLen;

    uint32_t bytesToRead;
    // 当前读取的块小于总的块数量
    while (cntBlockRead < count && bytesLeft > 0) {
        for (i = 0; i < mpiView->count; i++) {
            if (cntBlockRead >= count || bytesLeft == 0) {
                break;
            }
            if (mpiView->blocklens[i] == 0) {
                // 空的blocklens
                continue;
            }
            if (posInFiletype <= mpiView->blockoffs[i]) {
                // 计划读一整块
                bytesToRead = mpiView->blocklens[i];
                // 检查是否够读
                bytesToRead = (bytesLeft > bytesToRead) ? bytesToRead : bytesLeft;
                // 更新剩余字节，只会减成0，不会减成负数
                bytesLeft -= bytesToRead;

                viewRead->blocklens[cntBlockRead] = bytesToRead;
                viewRead->blockoffs[cntBlockRead] = displacement + posFiletype * filetypeLen + mpiView->blockoffs[i];

                posInFiletype += bytesToRead;
                cntBlockRead++;
            } else if (posInFiletype < (mpiView->blockoffs[i] + mpiView->blocklens[i])) {
                // 计划读整块的部分
                bytesToRead = mpiView->blockoffs[i] + mpiView->blocklens[i] - posInFiletype;
                // 检查是否够读
                bytesToRead = (bytesLeft > bytesToRead) ? bytesToRead : bytesLeft;
                // 更新剩余字节，只会减成0，不会减成负数
                bytesLeft -= bytesToRead;

                viewRead->blocklens[cntBlockRead] = bytesToRead;
                viewRead->blockoffs[cntBlockRead] = displacement + posFiletype * filetypeLen + posInFiletype;

                posInFiletype += bytesToRead;
                cntBlockRead++;
            } else {
                // 读取的偏移超过当前视图块的位置，准备读下一个block
                continue;
            }
        }

        // 读完一个filetype，准备读下一个
        posFiletype++;
        posInFiletype = 0;
    }
}

/* ****************************************************************************
 功能描述  : 拷贝buffer到iov
 返 回 值  :    未拷贝的字节数
**************************************************************************** */
static u32 CopyBuffToIov(const void *buff, u32 bufLen, struct iovec *iov, u32 iovLen)
{
    uint32_t pos = 0;
    uint32_t i;

    CHECK_NULL_POINTER(buff, EINVAL);
    CHECK_NULL_POINTER(iov, EINVAL);

    uint32_t left_len = (uint32_t)bufLen;
    uint32_t copy_len = 0;
    for (i = 0; i < iovLen; i++) {
        copy_len = MIN(left_len, iov[i].iov_len);
        left_len -= copy_len;
        memcpy(iov[i].iov_base, (((char *)buff) + pos), copy_len);

        iov[i].iov_len = copy_len;
        pos += copy_len;
        if (left_len == 0) {
            break;
        }
    }

    return left_len;
}

int mpi_fs_view_read(int fd, u32 iovcnt, struct iovec *iov, off_t offset)
{
    int ret = -1;
    u32 leftLen = 0;

    if (iov == NULL || iovcnt == 0) {
        errno = EINVAL;
        return -1;
    }

    mpi_fh_handle_t *fh = mpi_fh_get(fd);
    if (fh == NULL) {
        errno = EINVAL;
        return -1;
    }

    // 计算需要读取的字节长度
    uint32_t readLen = 0;
    uint32_t i;
    for (i = 0; i < iovcnt; i++) {
        readLen += iov[i].iov_len;
    }

    // 读取视图需加锁
    pthread_mutex_lock(&fh->lock);

    // 计算此次viewread 需要读取的块的个数
    uint32_t count;
    count = GetBlockCntInMPIView(offset, readLen, fh->view);
    if (count == 0) {
        pthread_mutex_unlock(&fh->lock);
        mpi_fh_put(fh);
        return -1;
    }

    // 根据算出来需要读取的块数量，分配内存
    // 需要一段连续内存发送给odcs
    // 内存格式为mpi_fs_view_read_t，末尾为read buffer + read range buffer
    mpi_fs_view_read_t *viewRead = AllocMPIViewRead(offset, readLen, count);
    if (viewRead == NULL) {
        pthread_mutex_unlock(&fh->lock);
        mpi_fh_put(fh);
        return -1;
    }

    MakeMPIViewReadRange(viewRead, fh->view);

    // 使用完视图后解锁
    pthread_mutex_unlock(&fh->lock);

    ret = ioctl(fd, ODCS_IOC_MPI_VIEW_READ, viewRead);
    if (ret == 0) {
        leftLen = CopyBuffToIov(viewRead->data, viewRead->readLen, iov, iovcnt);
        if (leftLen != 0) {
            ret = -1;
        }

        // 返回实际读取的长度
        ret = viewRead->readLen;
    }

    mpi_free(viewRead);

    mpi_fh_put(fh);
    return ret;
}

int mpi_fs_get_group_id(int fd, uint64_t *group_id)
{
    if (group_id == NULL) {
        errno = EINVAL;
        return -1;
    }
    
    mpi_fs_group_id_t group;
    group.fd = fd;
    int ret = ioctl(fd, ODCS_IOC_IOCTL_MPI_GROUP_ID, &group);
    if (ret) {
        *group_id = 0;
        return ret;
    }   

    *group_id = group.group_id;
    return ret;
}

int mpi_fs_set_group_id(int fd, uint64_t group_id)
{
    mpi_fs_group_id_t group;
    group.fd = fd;
    group.group_id = group_id;
    return ioctl(fd, ODCS_IOC_IOCTL_MPI_SET_GROUP_ID, &group);
}
