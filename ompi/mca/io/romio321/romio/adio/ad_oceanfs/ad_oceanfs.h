#ifndef AD_OCEANFS_H_INCLUDED
#define AD_OCEANFS_H_INCLUDED

#include "adio.h"
#include "ad_oceanfs_tuning.h"

#define OCEANFS_READ 0
#define OCEANFS_WRITE 1
#define OCEANFS_READ_STRIDED 2
#define OCEANFS_READ_COLL 3
#define OCEANFS_WRITE_STRIDED 4
#define OCEANFS_WRITE_COLL 5

#define ADIOI_OCEANFS_PREFIX "oceanfs:"
#define ADIOI_OCEANFS_PREFIX_LEN (sizeof(ADIOI_OCEANFS_PREFIX) - 1)

void ADIOI_OCEANFS_Open(ADIO_File fd, int *error_code);

void ADIOI_OCEANFS_Close(ADIO_File fd, int *error_code);

void ADIOI_OCEANFS_ReadContig(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code);

void ADIOI_OCEANFS_WriteContig(ADIO_File fd, const void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code);

void ADIOI_OCEANFS_ReadStrided(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code);

void ADIOI_OCEANFS_ReadStridedColl(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code);

void ADIOI_OCEANFS_WriteStridedColl(ADIO_File fd, const void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code);

void ADIOI_OCEANFS_Fcntl(ADIO_File fd, int flag, ADIO_Fcntl_t *fcntl_struct, int *error_code);

void ADIOI_OCEANFS_Resize(ADIO_File fd, ADIO_Offset size, int *error_code);

void ADIOI_OCEANFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code);

int ADIOI_OCEANFS_set_view(ADIO_File fd, int *error_code);

int ADIOI_OCEANFS_StridedViewIO(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int io_flag, int *error_code);
int ADIOI_OCEANFS_Set_lock(FDTYPE fd, int cmd, int type, ADIO_Offset offset, int whence, ADIO_Offset len);

#endif /* AD_OCEANFS_H_INCLUDED */
