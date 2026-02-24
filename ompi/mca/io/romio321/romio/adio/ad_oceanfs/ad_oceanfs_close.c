#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "ad_oceanfs_group_tuning.h"
#include "mpi_fs_intf.h"

void ADIOI_OCEANFS_Close(ADIO_File fd, int *error_code)
{
    ROMIO_LOG(AD_LOG_LEVEL_ALL, "interface");
    static char myname[] = "ADIOI_OCEANFS_CLOSE";
    int ret;
    int tmp_error_code;
    ADIOI_OCEANFS_fs *nas_fs = NULL;

    ret = mpi_fs_close(fd->fd_sys);
    ROMIO_LOG((ret) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
        "mpi_fs_close name:%s,ret:%d", fd->filename, ret);
    if (ret != 0) {
        tmp_error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_UNKNOWN,
            "Error in mpi_fs_close", 0);
    } else {
        tmp_error_code = MPI_SUCCESS;
    }

    if (error_code) {
        *error_code = tmp_error_code;
    }

    nas_fs = (ADIOI_OCEANFS_fs *)fd->fs_ptr;
    if (nas_fs) {
        if (nas_fs->nas_filename) {
            ADIOI_Free(nas_fs->nas_filename);
            nas_fs->nas_filename = NULL;
        }
        if (nas_fs->context) {
            ROMIO_LOG((ret) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
                "close name:%s,group:%llu", fd->filename,
                (unsigned long long)(nas_fs->context->group_id));
            ad_oceanfs_group_report(fd, nas_fs->context->group_id);
            ADIOI_Free(nas_fs->context);
            nas_fs->context = NULL;
        }
        ADIOI_Free(nas_fs);
        fd->fs_ptr = NULL;
    }

    /* reset fds */
    fd->fd_direct = -1;
    fd->fd_sys = -1;
}
