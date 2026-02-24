#ifndef _MPI_FS_INTERFACE_H_
#define _MPI_FS_INTERFACE_H_
#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <linux/types.h>
#include <linux/uuid.h>
#include <linux/ioctl.h>

typedef unsigned int u32;

/*
#define EPERM     1    Operation not permitted 
#define ENOEN     2    No such file or directory 
#define ESRCH     3    No such process 
#define EINTR     4    Interrupted system call 
#define EIO       5    I/O error 
#define ENXIO     6    No such device or address 
#define E2BIG     7    Argument list too long 
#define ENOEX     8    Exec format error 
#define EBADF     9    Bad file number 
#define ECHIL    10    No child processes 
#define EAGAI    11    Try again 
#define ENOME    12    Out of memory 
#define EACCE    13    Permission denied 
#define EFAUL    14    Bad address 
#define ENOTB    15    Block device required 
#define EBUSY    16    Device or resource busy 
#define EEXIS    17    File exists 
#define EXDEV    18    Cross-device link 
#define ENODE    19    No such device 
#define ENOTD    20    Not a directory 
#define EISDI    21    Is a directory 
#define EINVA    22    Invalid argument 
#define ENFIL    23    File table overflow 
#define EMFIL    24    Too many open files 
#define ENOTT    25    Not a typewriter 
#define ETXTB    26    Text file busy 
#define EFBIG    27    File too large 
#define ENOSP    28    No space left on device 
#define ESPIP    29    Illegal seek 
#define EROFS    30    Read-only file system 
#define EMLIN    31    Too many links 
#define EPIPE    32    Broken pipe 
#define EDOM     33    Math argument out of domain of func 
#define ERANG    34    Math result not representable
*/

#define FS_MAX_XATTR_NAME_LEN 256UL /* Max user-xattr's name len: 256(包括\0结束符) */
#define FS_MAX_NAME_LEN 1024UL      /* Max  name len: 256(包括\0结束符) */
#define FS_MAX_PATH_LENGTH 4096UL   /* Max  path len: 4096(包括\0结束符) */
#define FS_MAX_LONG_NAME_LEN 1024UL /* Max  long name len: 1024(包括\0结束符) */

/* ****************************************************************************
 函 数 名  : mpi_fs_open
 功能描述  : 打开一个文件
 输入参数  :
             param[in]  pathname  打开对象路径(包括/0)
             param[in]  flags     打开标志
             param[in]  mode      打开模式
 返回值处理：
             1、成功返回: 打开fd
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1.调用方保证pathname不为空，且以/0结束，由调用者分配、释放内存
**************************************************************************** */
int mpi_fs_open(const char *pathname, int flags, mode_t mode);

/* ****************************************************************************
 函 数 名  : mpi_fs_pread
 功能描述  : 读取文件内容
 输入参数  :
             param[in]  fd        读对象fd
             param[in]  buf       读取内容待填充buf
             param[in]  count     读取字节数
             param[in]  offset    读取的偏移地址
 返回值处理：
             1、成功返回: 大于等于零，代表读成功的字节数
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1.调用方保证buf不为空，由调用者分配、释放内存
**************************************************************************** */
int mpi_fs_pread(int fd, void *buf, size_t count, off_t offset);

/* ****************************************************************************
 函 数 名  : mpi_fs_pwrite
 功能描述  : 写文件内容
 输入参数  :
             param[in]  fd        写对象fd
             param[in]  buf       写内容buf
             param[in]  count     写字节数
             param[in]  offset    写偏移地址
 返回值处理：
             1、成功返回: 大于等于零，代表读成功的字节数
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1.调用方保证buf不为空，由调用者分配、释放内存
**************************************************************************** */
int mpi_fs_pwrite(int fd, const void *buf, size_t count, off_t offset);

/* ****************************************************************************
 函 数 名  : mpi_fs_stat
 功能描述  : 根据文件路径，查询文件属性状态
 输入参数  :
             param[in]  pathname        待查询属性文件路径(包括/0)
             param[in]  buf             查询属性结果buf
 返回值处理：
             1、成功返回: 0
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1、pathname不为空，且以/0结束，由调用者分配、释放内存
             2、buf不为空，由调用者分配、释放内存
**************************************************************************** */
int mpi_fs_stat(const char *pathname, struct stat *buf);

/* ****************************************************************************
 函 数 名  : mpi_fs_close
 功能描述  : 关闭文件
 输入参数  :
             param[in]  fd        关闭对象fd
 返回值处理：
             1、成功返回: 0
             2、失败返回: -1 is returned and errno is set to indicate the error
 使用约束  :
             1、调用方保证fd有效性。
**************************************************************************** */
int mpi_fs_close(int fd);
/* ****************************************************************************
 函 数 名  : mpi_fs_ftruncate
 功能描述  : 根据fd截断文件到指定长度
 输入参数  :
             param[in]  fd              待截断文件fd
             param[in]  length          截断长度
 返回值处理：
             1、成功返回: 0
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1、length 大于等于0
**************************************************************************** */
int mpi_fs_ftruncate(int fd, off_t length);
/* ****************************************************************************
 函 数 名  : mpi_fs_lseek
 功能描述  : reposition read/write file offset
 输入参数  :
             param[in]  fd              操作对象fd
             param[in]  offset          跳转偏移
             param[in]  whence          SEEK_SET、SEEK_CUR、SEEK_END
 返回值处理：
             1、成功返回: 大于零，代表offset location as measured in bytes
                from the beginning of the file
             2、失败返回: -1 and errno is set to indicate the error
**************************************************************************** */
off_t mpi_fs_lseek(int fd, off_t offset, int whence);

/* ****************************************************************************
 函 数 名  : mpi_fs_set_fileview
 功能描述  : 文件视图通过该接口透传给私有客户端，
            由私有客户端根据视图信息合理组织数据的cache预取与数据存储策略，系统垂直优化
 输入参数  :
            \param[in]  fd              操作对象fd
            \param[in]  offset          文件视图开始偏移(bytes)
            \param[in]  count           filetype中连续块（block）的数量
            \param[in]  blocklens       存储每个连续块长度（bytes）的数组，数组长度为count
            \param[in]  blockoffs       存储每个连续块开始位置偏移（bytes）的数组，数组长度为count
            \param[in]  ub_off          filetype结束位置偏移（bytes）
 返回值处理：
             1、成功返回: 0
             2、失败返回: -1 and errno is set to indicate the error
 使用约束  :
             1.调用前保证文件已被打开，文件close后视图随之清除
             2.调用方保证fd合法性, blocklens不为空, blockoffs不为空，由调用者分配、释放内存。
**************************************************************************** */
int mpi_fs_set_fileview(int fd, off_t offset, u32 count, u32 *blocklens, off_t *blockoffs, off_t ub_off);

/* ****************************************************************************
 函 数 名  : mpi_fs_view_read
 功能描述  : 设置视图读取文件内容
 输入参数  :
            \param[in]  fd          读对象fd
            \param[in]  iov         读取内容待填充io vector
            \param[in]  iovcnt      iov个数
            \param[in]  offset      读取的偏移地址(bytes)
 返回值处理：
             1.小于0      失败
             2.大于等于0   成功,读取成功的字节数
 使用约束  :
             1、如果未设置视图也能调用，功能与普通的read操作相同
             2、调用方保证各个入参不为空，并保证其有效性。
                iov不为空，iov_base不为空，由调用者分配、释放内存
**************************************************************************** */
int mpi_fs_view_read(int fd, u32 iovcnt, struct iovec *iov, off_t offset);

int mpi_fs_get_group_id(int fd, uint64_t *group_id);
int mpi_fs_set_group_id(int fd, uint64_t group_id);

#endif
