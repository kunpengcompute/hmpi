#include <stdint.h>
#include "adio.h"
#include "adio_extern.h"
#include "ad_oceanfs.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"

static int g_io_size_max = 1048576; // 1024 * 1024

static int OCEANFS_IO_Pread(ADIO_File fd, char *p_buf, ADIO_Offset wr_len, off_t offset)
{
    double t = ad_oceanfs_timing_get_time();
    int ret = mpi_fs_pread(fd->fd_sys, p_buf, wr_len, offset);
    ad_oceanfs_timing_report(fd, OCEANFSMPIO_CIO_R_OCEANFS, t);
    return ret;
}

static int OCEANFS_IO_Pwrite(ADIO_File fd, char *p_buf, ADIO_Offset wr_len, off_t offset)
{
    double t = ad_oceanfs_timing_get_time();
    int ret = mpi_fs_pwrite(fd->fd_sys, p_buf, wr_len, offset);
    ad_oceanfs_timing_report(fd, OCEANFSMPIO_CIO_W_OCEANFS, t);
    return ret;
}

static int OCEANFS_IO(int io_flag, ADIO_File fd, char *p_buf, ADIO_Offset wr_len, off_t offset)
{
    int ret = -1;
    switch (io_flag) {
        case OCEANFS_READ: {
            ret = OCEANFS_IO_Pread(fd, p_buf, wr_len, offset);
            break;
        }
        case OCEANFS_WRITE: {
            ret = OCEANFS_IO_Pwrite(fd, p_buf, wr_len, offset);
            break;
        }
        default: {
            break;
        }
    }
    return ret;
}

static void OCEANFS_IOContig(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int io_flag, int *error_code)
{
    int ret = 0;
    MPI_Count datatype_size;
    ADIO_Offset mem_len, wr_len;
    uint64_t file_offset = offset;
    static char myname[] = "ADIOI_OCEANFS_IOCONTIG";
    char *p_buf = NULL;
    ADIO_Offset bytes_xfered = 0;

    MPI_Type_size_x(datatype, &datatype_size);
    mem_len = (ADIO_Offset)datatype_size * (ADIO_Offset)count;

    if (file_ptr_type == ADIO_INDIVIDUAL) {
        file_offset = fd->fp_ind;
    }

    p_buf = (char *)buf;
    while (bytes_xfered < mem_len) {
        wr_len = ADIOI_MIN(g_io_size_max, mem_len - bytes_xfered);
        ret = OCEANFS_IO(io_flag, fd, p_buf, wr_len, file_offset + bytes_xfered);
        if (!ret) {
            break;
        }
        if (ret == -1) {
            *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_IO, "**io",
                "**io %s", strerror(errno));
            fd->fp_sys_posn = -1;
            return;
        }
        bytes_xfered += ret;
        p_buf += ret;
    }
    /* Let the application decide how to fail */
    if (ret < 0) {
        *error_code = MPI_SUCCESS;
        return;
    }

    if (file_ptr_type == ADIO_INDIVIDUAL) {
        fd->fp_ind += bytes_xfered;
    }
    fd->fp_sys_posn = file_offset + bytes_xfered;

#ifdef HAVE_STATUS_SET_BYTES
    MPIR_Status_set_bytes(status, datatype, bytes_xfered);
#endif

    *error_code = MPI_SUCCESS;
}

void ADIOI_OCEANFS_ReadContig(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code)
{
    double t = ad_oceanfs_timing_get_time();
    OCEANFS_IOContig(fd, buf, count, datatype, file_ptr_type, offset, status, OCEANFS_READ, error_code);
    ad_oceanfs_timing_report(fd, OCEANFSMPIO_CIO_R_CONTIG, t);
}

void ADIOI_OCEANFS_WriteContig(ADIO_File fd, const void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int *error_code)
{
    double t = ad_oceanfs_timing_get_time();
    OCEANFS_IOContig(fd, (void *)buf, count, datatype, file_ptr_type, offset, status, OCEANFS_WRITE, error_code);
    ad_oceanfs_timing_report(fd, OCEANFSMPIO_CIO_W_CONTIG, t);
}
