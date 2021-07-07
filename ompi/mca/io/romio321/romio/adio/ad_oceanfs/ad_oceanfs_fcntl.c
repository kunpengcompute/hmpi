#include "ad_oceanfs.h"
#include "adio_extern.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"

void ADIOI_OCEANFS_Fcntl(ADIO_File fd, int flag, ADIO_Fcntl_t *fcntl_struct, int *error_code)
{
    int ret;
    static char myname[] = "ADIOI_OCEANFS_FCNTL";

    switch (flag) {
        case ADIO_FCNTL_GET_FSIZE: {
            struct stat stbuf;

            stbuf.st_size = 0;
            ret = mpi_fs_stat(fd->filename, &stbuf);
            if (ret) {
                *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_FILE,
                    "Error in mpi_fs_stat", 0);
                return;
            }

            fcntl_struct->fsize = stbuf.st_size;
            *error_code = MPI_SUCCESS;
            break;
        }
        case ADIO_FCNTL_SET_DISKSPACE:
            MPIO_CHECK_NOT_SEQUENTIAL_MODE(fd, myname, *error_code);
            if (fd->split_coll_count || fd->async_count) {
                *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_IO,
                    "**io", "**io %s", strerror(errno));
                return;
            }
            ADIOI_GEN_Prealloc(fd, fcntl_struct->diskspace, error_code);
            break;

        case ADIO_FCNTL_SET_ATOMICITY:
            fd->atomicity = (fcntl_struct->atomicity == 0) ? 0 : 1;
            *error_code = MPI_SUCCESS;
            break;
        default:
            *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_ARG,
                "**flag", "**flag %d", flag);
            break;
    }
fn_exit:
    return;
}
