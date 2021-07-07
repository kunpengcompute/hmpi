#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "ad_oceanfs_group_tuning.h"
#include "mpi_fs_intf.h"

void ADIOI_OCEANFS_Close(ADIO_File fd, int *error_code)
{
    static char myname[] = "ADIOI_OCEANFS_CLOSE";
    int ret;
    int tmp_error_code;
    ADIOI_OCEANFS_fs *ocean_fs = NULL;

    ret = mpi_fs_close(fd->fd_sys);
    if (ret != 0) {
        tmp_error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_UNKNOWN,
            "Error in mpi_fs_close", 0);
    } else {
        tmp_error_code = MPI_SUCCESS;
    }

    if (error_code) {
        *error_code = tmp_error_code;
    }

    ocean_fs = (ADIOI_OCEANFS_fs *)fd->fs_ptr;
    if (ocean_fs) {
        if (ocean_fs->oceanfs_filename) {
            ADIOI_Free(ocean_fs->oceanfs_filename);
            ocean_fs->oceanfs_filename = NULL;
        }
        if (ocean_fs->context) {
            ad_oceanfs_group_report(fd, ocean_fs->context->group_id);
            ADIOI_Free(ocean_fs->context);
            ocean_fs->context = NULL;
        }
        ADIOI_Free(ocean_fs);
        fd->fs_ptr = NULL;
    }

    /* reset fds */
    fd->fd_direct = -1;
    fd->fd_sys = -1;
}
