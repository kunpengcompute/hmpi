#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"

void ADIOI_OCEANFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code)
{
    int ret, rank;
    static char myname[] = "ADIOI_OCEANFS_RESIZE";

    if (!error_code) {
        return;
    }
    if (!fd) {
        *error_code = MPI_ERR_FILE;
        return;
    }

    if (fd->split_coll_count || fd->async_count) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_IO, "**io",
            "**io %s", strerror(errno));
        return;
    }
    MPI_Comm_rank(fd->comm, &rank);

    /* rank 0 process performs ftruncate(), then bcast torest process */

    if (rank == fd->hints->ranklist[0]) {
        ret = mpi_fs_ftruncate(fd->fd_sys, size);
    }
    /* bcast return value */
    MPI_Bcast(&ret, 1, MPI_INT, fd->hints->ranklist[0], fd->comm);

    if (ret != 0) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_FILE,
            "Error in mpi_fs_ftruncate", 0);
    } else {
        *error_code = MPI_SUCCESS;
    }

    return;
}
