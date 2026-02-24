#include <string.h>
#include "ad_env.h"
#include "ad_oceanfs_llt.h"
#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "ad_oceanfs_tuning.h"
#include "ad_oceanfs_group_tuning.h"

int ad_open_GetMode(ADIO_File fd)
{
    int amode = 0;
    /* setup the file access mode */
    if (fd->access_mode & ADIO_CREATE) {
        amode = amode | O_CREAT;
    }
    if (fd->access_mode & ADIO_RDONLY) {
        amode = amode | O_RDONLY;
    }
    if (fd->access_mode & ADIO_WRONLY) {
        amode = amode | O_WRONLY;
    }
    if (fd->access_mode & ADIO_RDWR) {
        amode = amode | O_RDWR;
    }
    if (fd->access_mode & ADIO_EXCL) {
        amode = amode | O_EXCL;
    }
    if (fd->access_mode & ADIO_APPEND) {
        amode = amode | O_APPEND;
    }
    /* TO DO */
    return amode;
}

static uint64_t SyncGroupId(ADIO_File fd, int rank)
{
    int ret;
    uint64_t group_id = 0;
    if (get_group_lock_enable()) {
        if (rank == 0) {
            ret = mpi_fs_get_group_id(fd->fd_sys, &group_id);
            ROMIO_LOG((ret) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
                "mpi_fs_get_group_id name:%s,group:%llu,ret:%d", fd->filename,
                (unsigned long long)group_id, ret);
            if (ret < 0) {
                group_id = 0;
            }

            MPI_Bcast(&group_id, 1, MPI_UNSIGNED_LONG_LONG, 0, fd->comm);
        } else {
            MPI_Bcast(&group_id, 1, MPI_UNSIGNED_LONG_LONG, 0, fd->comm);
            ret = mpi_fs_set_group_id(fd->fd_sys, group_id);
            ROMIO_LOG((ret) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
                "mpi_fs_set_group_id name:%s,group:%llu,ret:%d", fd->filename,
                (unsigned long long)group_id, ret);
        }
    }
    return group_id;
}

static int SetupFilePerm(ADIO_File fd)
{
    static const int umask_param = 022;
    static const int mask_param = 0666;
    mode_t old_mask;
    int perm;
    if (fd->perm == ADIO_PERM_NULL) {
        old_mask = umask(umask_param);
        umask(old_mask);
        perm = old_mask ^ mask_param;
    } else {
        perm = fd->perm;
    }
    return perm;
}

void ad_open_OpenCheck(ADIO_File fd, int *error_code)
{
    static char myname[] = "ADIOI_OCEANFS_OPEN";
    if ((fd->fd_sys != -1) && ((uint32_t)fd->access_mode & ADIO_APPEND)) {
        int ret = mpi_fs_lseek(fd->fd_sys, 0, SEEK_END);
        ROMIO_LOG((ret) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
            "mpi_fs_lseek name:%s,ret:%d", fd->filename, ret);
        if (ret == -1) {
            *error_code = ADIOI_Err_create_code(myname, fd->filename, errno);
            ADIOI_OCEANFS_fs *nas_fs = (ADIOI_OCEANFS_fs *)fd->fs_ptr;
            ADIOI_Free(nas_fs->context);
            ADIOI_Free(nas_fs);

            fd->fs_ptr = NULL;
            return;
        }
        fd->fp_ind = ret;
        fd->fp_sys_posn = ret;
    }

    *error_code = MPI_SUCCESS;
}

static void AllocFS(ADIO_File fd, int *error_code)
{
    static char myname[] = "ADIOI_OCEANFS_OPEN";
    ADIOI_OCEANFS_fs *nas_fs = (ADIOI_OCEANFS_fs *)ADIOI_Malloc(sizeof(ADIOI_OCEANFS_fs));
    if (nas_fs == NULL) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_UNKNOWN,
            "Error allocating memory", 0);
        return;
    }
    nas_fs->nas_filename = NULL;

    nas_fs->context = (MPI_CONTEXT_T *)ADIOI_Malloc(sizeof(MPI_CONTEXT_T));
    if (nas_fs->context == NULL) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_UNKNOWN,
            "Error allocating memory", 0);
        ADIOI_Free(nas_fs);
        return;
    }
    memset(nas_fs->context, 0, sizeof(MPI_CONTEXT_T));

    fd->fs_ptr = nas_fs;
    *error_code = MPI_SUCCESS;
}
#ifdef MPI_LLT_ENABLE
static void RunLlt()
{
    int total = 0;
    int success = 0;
    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT All Start");
    llt_pub(&total, &success);
    llt_viewio_1(&total, &success);
    llt_viewio_2(&total, &success);
    llt_fcntl(&total, &success);
    llt_group_report(&total, &success);
    llt_aggrs(&total, &success);
    llt_aggrs_1(&total, &success);
    llt_aggrs_2(&total, &success);
    llt_aggrs_3(&total, &success);
    llt_aggrs_4(&total, &success);
    llt_resize(&total, &success);
    llt_fs(&total, &success);
    llt_file(&total, &success);
    llt_wrcoll(&total, &success);
    llt_view(&total, &success);
    llt_open(&total, &success);

    ROMIO_LOG(AD_LOG_LEVEL_NON, "LLT All End");
}
#endif

void ADIOI_OCEANFS_Open(ADIO_File fd, int *error_code)
{
    ROMIO_LOG(AD_LOG_LEVEL_ALL, "interface");
    static char myname[] = "ADIOI_OCEANFS_OPEN";
    int perm, amode, ret, rank;
    uint64_t group_id;

    /* validate input args */
    if (!fd) {
        *error_code = MPI_ERR_FILE;
        return;
    }

    /* set internal variables for tuning environment variables */
    ad_oceanfs_get_env_vars();

#ifdef MPI_LLT_ENABLE
    if (strcmp(fd->filename, "/llt/llt") == 0) {
        RunLlt();
        *error_code = MPI_SUCCESS;
        return;
    }
#endif

    /* setup file permissions */
    perm = SetupFilePerm(fd);

    amode = ad_open_GetMode(fd);
    /* init OCEANFS */
    fd->fs_ptr = NULL;
    MPI_Comm_rank(fd->comm, &rank);
    ADIOI_OCEANFS_Init(rank, error_code);
    if (*error_code != MPI_SUCCESS) {
        return;
    }

    AllocFS(fd, error_code);
    if (*error_code != MPI_SUCCESS) {
        return;
    }

    /* all processes open the file */
    ret = mpi_fs_open(fd->filename, amode, perm);
    ROMIO_LOG(AD_LOG_LEVEL_ALL, "mpi_fs_open name:%s,mode:%d,ret:%d", fd->filename, amode, ret);
    if (ret < 0) {
        *error_code = ADIOI_Err_create_code(myname, fd->filename, errno);
        return;
    }

    fd->fd_sys = ret;
    fd->fd_direct = -1;

    group_id = SyncGroupId(fd, rank);
    ((ADIOI_OCEANFS_fs *)(fd->fs_ptr))->context->group_id = group_id;
    ad_oceanfs_group_report(fd, group_id);

    ad_open_OpenCheck(fd, error_code);
    return;
}
