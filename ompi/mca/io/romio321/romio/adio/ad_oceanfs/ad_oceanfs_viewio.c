#include <unistd.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include "adio.h"
#include "adio_extern.h"
#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"

static int g_max_size = 1048576; // 1024 * 1024

void ad_viewio_SetIovec(struct iovec *iov, void  *iov_base, size_t iov_len)
{
    iov->iov_base = iov_base;
    iov->iov_len = iov_len;
}

void ad_viewio_ClacFlatBuf(struct iovec *iov, void *buf, ADIO_Offset size_in_buftype, ADIOI_Flatlist_node *flat_buf, int* pi, int* pk, ADIO_Offset j_bufextent)
{
    ADIO_Offset sum = 0;
    int i = *pi;
    int k = *pk;
    for (; i < flat_buf->count; i++) {
        if (flat_buf->blocklens[i]) {
            sum += flat_buf->blocklens[i];
            iov[k].iov_base = buf + j_bufextent + flat_buf->indices[i];
            if (sum > size_in_buftype) {
                iov[k].iov_len = size_in_buftype - sum + flat_buf->blocklens[i];
                break;
            }
            iov[k].iov_len = flat_buf->blocklens[i];
            k++;
        }
        i++;
    }
    *pi = i;
    *pk = k;
}

int ADIOI_OCEANFS_fill_iovec(ADIO_File fd, struct iovec *iov, ADIO_Offset *offset_inbuftypes, void *buf,
    MPI_Datatype buftype, ADIOI_Flatlist_node *flat_buf, int *buftype_idx, int *block_idx, int len, int xfered_len)
{
    ADIO_Offset part_len, j_bufextent;
    ADIO_Offset n_buftypes, size_in_buftype;
    MPI_Aint buftype_extent;
    MPI_Count buftype_size;
    int buftype_is_contig;

    int i = *block_idx;
    int j = *buftype_idx;
    int k = 0;
    MPI_Type_size_x(buftype, &buftype_size);
    MPI_Type_extent(buftype, &buftype_extent);
    ADIOI_Datatype_iscontig(buftype, &buftype_is_contig);

    if (buftype_is_contig) {
        ad_viewio_SetIovec(iov, buf + *offset_inbuftypes, len);
        *offset_inbuftypes += len;
        return 1;
    }
    part_len = flat_buf->blocklens[i] + flat_buf->indices[i] + j * buftype_extent - *offset_inbuftypes;
    if (part_len) {
        ad_viewio_SetIovec(iov, buf + *offset_inbuftypes, (len < part_len) ? len : part_len);
        *offset_inbuftypes += iov[0].iov_len;
        part_len -= iov[0].iov_len;
        if (part_len)
            return 1;
        i++;
        k = 1;
    }

    if (buftype_size == 0) {
        return 0;
    }
    n_buftypes = (xfered_len + len) / buftype_size;
    size_in_buftype = (xfered_len + len) % buftype_size;

    for (; j < n_buftypes; j++) {
        j_bufextent = j * buftype_extent;
        while (i < flat_buf->count) {
            if (flat_buf->blocklens[i]) {
                ad_viewio_SetIovec(iov + k, buf + j_bufextent + flat_buf->indices[i], flat_buf->blocklens[i]);
                k++;
            }
            i++;
        }
        i = 0;
    }
    j_bufextent = j * buftype_extent;

    ad_viewio_ClacFlatBuf(iov, buf, size_in_buftype, flat_buf, &i, &k, j_bufextent);

    *block_idx = i;
    *buftype_idx = j;
    *offset_inbuftypes = iov[k].iov_base + iov[k].iov_len - buf;
    return k;
}

ADIO_Offset ad_viewio_update_offset_in_file(ADIO_File fd, ADIO_Offset update_bytes, ADIO_Offset curr_offset)
{
    ADIO_Offset abs_off;
    ADIO_Offset off;
    ADIOI_Flatlist_node *flat_file = NULL;
    int i;
    ADIO_Offset n_filetypes;
    ADIO_Offset abs_off_in_filetype = 0;
    ADIO_Offset size_in_filetype, sum;
    MPI_Count filetype_size;
    int filetype_is_contig;
    MPI_Aint filetype_extent;

    ADIOI_Datatype_iscontig(fd->filetype, &filetype_is_contig);

    abs_off = curr_offset + update_bytes;
    if (filetype_is_contig) {
        off = abs_off;
    } else {
        flat_file = ADIOI_Flatten_and_find(fd->filetype);

        MPI_Type_extent(fd->filetype, &filetype_extent);
        MPI_Type_size_x(fd->filetype, &filetype_size);
        if (!filetype_size) {
            return 0;
        }

        n_filetypes = abs_off / filetype_size;
        size_in_filetype = abs_off - n_filetypes * filetype_size;

        sum = 0;
        for (i = 0; i < flat_file->count; i++) {
            sum += flat_file->blocklens[i];
            if (sum > size_in_filetype) {
                abs_off_in_filetype = flat_file->indices[i] + size_in_filetype - (sum - flat_file->blocklens[i]);
                break;
            }
        }

        /* abs. offset in bytes in the file */
        off = n_filetypes * filetype_extent + abs_off_in_filetype;
    }

    return off;
}


int ADIOI_OCEANFS_StridedViewIO(ADIO_File fd, void *buf, int count, MPI_Datatype datatype, int file_ptr_type,
    ADIO_Offset offset, ADIO_Status *status, int io_flag, int *error_code)
{
    ADIOI_Flatlist_node *flat_buf = NULL;
    ADIO_Offset off, file_offset, begin_off, start_offset;
    MPI_Count wr_len, sentlen, cur_sendlen;
    MPI_Count filetype_size, etype_size, buftype_size;
    int buftype_is_contig, filetype_is_contig;
    int ret, iovcnt, n_buftype;
    struct iovec *iov = NULL;
    ADIO_Offset offset_inbuftypes;
    int block_idx, buftype_idx;

    *error_code = MPI_SUCCESS; /* changed below if error */
    MPI_Aint buftype_extent;
    static char myname[] = "ADIOI_OCEANFS_StridedViewIO";

    ADIOI_Datatype_iscontig(datatype, &buftype_is_contig);
    ADIOI_Datatype_iscontig(fd->filetype, &filetype_is_contig);
    MPI_Type_size_x(fd->filetype, &filetype_size);
    if (!filetype_size) {
#ifdef HAVE_STATUS_SET_BYTES
        MPIR_Status_set_bytes(status, datatype, 0);
#endif
        *error_code = MPI_SUCCESS;
        return -1;
    }

    MPI_Type_size_x(datatype, &buftype_size);
    MPI_Type_extent(datatype, &buftype_extent);
    etype_size = fd->etype_size;
    ADIOI_Assert((buftype_size * count) == ((ADIO_Offset)(MPI_Count)buftype_size * (ADIO_Offset)count));
    wr_len = buftype_size * count;
    start_offset = offset * etype_size;
    file_offset = 0;
    sentlen = 0;
    off = 0;
    block_idx = 0;
    offset_inbuftypes = 0;
    buftype_idx = 0;

    if (buftype_is_contig) {
        iov = (struct iovec *)ADIOI_Malloc(sizeof(struct iovec));
    } else {
        flat_buf = ADIOI_Flatten_and_find(datatype);
        n_buftype = ((wr_len < g_max_size) ? wr_len : g_max_size) / buftype_size + 1;
        iov = (struct iovec *)ADIOI_Malloc(sizeof(struct iovec) * flat_buf->count * n_buftype);
        offset_inbuftypes = flat_buf->indices[0];
    }

    if (filetype_is_contig) {
        off = ((file_ptr_type == ADIO_INDIVIDUAL) ? fd->fp_ind : (fd->disp + (ADIO_Offset)etype_size * offset));
        file_offset = off - fd->disp;
    } else {
        file_offset = 0;
        file_offset = ad_viewio_update_offset_in_file(fd, 0, start_offset);
    }

    ret = 0;
    begin_off = file_offset;

    // begin calculate the iovec (poll)
    if (io_flag == OCEANFS_READ_STRIDED) {
        if ((fd->atomicity) && ADIO_Feature(fd, ADIO_LOCKS)) {
            ADIOI_WRITE_LOCK(fd, begin_off + fd->disp, SEEK_SET, wr_len + 1);
        }
    } else if (io_flag == OCEANFS_WRITE_STRIDED) {
        if ((fd->atomicity)) {
            ADIOI_WRITE_LOCK(fd, begin_off + fd->disp, SEEK_SET, wr_len + 1);
        }
    }
    while (sentlen < wr_len) {
        // OCEANFS_CALCULATE_IOVEC
        cur_sendlen = ((g_max_size) < (wr_len - sentlen)) ? g_max_size : (wr_len - sentlen);
        iovcnt = ADIOI_OCEANFS_fill_iovec(fd, iov, &offset_inbuftypes, buf, datatype,
            flat_buf, &buftype_idx, &block_idx, cur_sendlen, sentlen);
        switch (io_flag) {
            case OCEANFS_READ_COLL:
            case OCEANFS_READ_STRIDED:
                ret = mpi_fs_view_read(fd->fd_sys, iovcnt, iov, fd->disp + file_offset);
                break;
            default:
                *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_IO,
                    "Unknown flag", 0);
                goto exit;
        }

        if (ret < 0) {
            *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE, myname, __LINE__, MPI_ERR_IO, "**io",
                "**io %s", strerror(errno));
            fd->fp_sys_posn = -1;
            return -1;
        }
        file_offset = ad_viewio_update_offset_in_file(fd, ret, sentlen + start_offset);
        if (ret == 0) {
            break;
        }
        sentlen += ret;
    }
    if (io_flag == OCEANFS_READ_STRIDED) {
        if ((fd->atomicity) && ADIO_Feature(fd, ADIO_LOCKS)) {
            ADIOI_UNLOCK(fd, begin_off + fd->disp, SEEK_SET, wr_len + 1);
        }
    } else if (io_flag == OCEANFS_WRITE_STRIDED) {
        if ((fd->atomicity)) {
            ADIOI_UNLOCK(fd, begin_off + fd->disp, SEEK_SET, wr_len + 1);
        }
    }
#ifdef HAVE_STATUS_SET_BYTES
    /* what if we only read half a datatype? */
    /* bytes_xfered could be larger than int */
    if (ret != -1) {
        MPIR_Status_set_bytes(status, datatype, sentlen);
    }
#endif
    if (file_ptr_type == ADIO_INDIVIDUAL) {
        fd->fp_ind = fd->disp + file_offset;
    }
    fd->fp_sys_posn = fd->disp + file_offset;

    *error_code = MPI_SUCCESS;
exit:
    /* group lock, flush data */
    MPI_Barrier(fd->comm);
    ADIO_Flush(fd, error_code);

    OCEANFS_Delete_flattened(datatype);
    ADIOI_Free(iov);
    return sentlen;
}
