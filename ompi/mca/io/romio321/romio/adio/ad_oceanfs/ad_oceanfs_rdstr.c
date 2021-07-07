#include <string.h>
#include "adio.h"
#include "adio_extern.h"
#include "ad_oceanfs.h"
#include "ad_oceanfs_pub.h"

static int ADIOI_OCEANFS_ReadStridedView(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code)
{
    return ADIOI_OCEANFS_StridedViewIO(fd, buf, count, datatype, file_ptr_type, offset, status, OCEANFS_READ_STRIDED,
        error_code);
}

void ADIOI_OCEANFS_ReadStrided(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code)
{
    double t = ad_oceanfs_timing_get_time();
    if (fd->hints->fs_hints.oceanfs.view_io == ADIOI_HINT_ENABLE) {
        ADIOI_OCEANFS_ReadStridedView(fd, buf, count, datatype, file_ptr_type, offset, status, error_code);
    } else {
        ADIOI_GEN_ReadStrided(fd, buf, count, datatype, file_ptr_type, offset, status, error_code);
    }
    ad_oceanfs_timing_report(fd, OCEANFSMPIO_CIO_R_STRIDED, t);
    return;
}
