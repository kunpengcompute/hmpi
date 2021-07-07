#include <stdlib.h>
#include "adio.h"
#include "adio_extern.h"
#include "ad_oceanfs.h"
#include "ad_oceanfs_pub.h"
#include "mpi_fs_intf.h"

static int ADIOI_OCEANFS_datatype_cost(ADIO_File fd, MPI_Datatype etype, MPI_Datatype filetype)
{
    int etype_is_contig;

    ADIOI_Datatype_iscontig(etype, &etype_is_contig);
    return etype_is_contig ? 1 : 0;
}

void ad_view_Init(u32* block_count, u32** blocklens, off_t** blockoffs, off_t* ub_off, int filetype_is_contig, MPI_Count filetype_size,
    ADIO_File fd)
{
    ADIOI_Flatlist_node *flat_file = NULL;
    if (filetype_is_contig == 1) {
        *block_count = 1;
        *blocklens = (u32 *)ADIOI_Malloc(sizeof(u32));
        *blockoffs = (off_t *)ADIOI_Malloc(sizeof(off_t));
        (*blockoffs)[0] = 0;
        (*blocklens)[0] = (u32)filetype_size;
        *ub_off = (off_t)filetype_size;
    } else {
        flat_file = OCEANFS_Flatten_and_find(fd->filetype);
        if (flat_file == NULL) {
            ADIOI_Info_set(fd->info, "view_io", "false");
            fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_DISABLE;
            return;
        }
        *block_count = (u32)flat_file->count;

        *blocklens = (u32 *)ADIOI_Malloc(sizeof(u32) * (*block_count));
        *blockoffs = (off_t *)ADIOI_Malloc(sizeof(off_t) * (*block_count));
        int i;
        for (i = 0; i < *block_count; i++) {
            (*blockoffs)[i] = (off_t)flat_file->indices[i];
            (*blocklens)[i] = (u32)flat_file->blocklens[i];
        }
        *ub_off = (*blockoffs)[*block_count - 1] + (off_t)((*blocklens)[*block_count - 1]);
    }
}

int ADIOI_OCEANFS_set_view(ADIO_File fd, int *error_code)
{
    int ret, filetype_is_contig;
    u32 block_count;
    u32 *blocklens = NULL;
    off_t *blockoffs = NULL;
    off_t ub_off;
    MPI_Count filetype_size;

    MPI_Type_size_x(fd->filetype, &filetype_size);

    if (ADIOI_OCEANFS_datatype_cost(fd, fd->etype, fd->filetype) == 0) {
        ADIOI_Info_set(fd->info, "view_io", "false");
        fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_DISABLE;
        *error_code = MPI_SUCCESS;
        return 0;
    }
    ADIOI_Datatype_iscontig(fd->filetype, &filetype_is_contig);
    if (fd->disp < 0) {
        ADIOI_Info_set(fd->info, "view_io", "false");
        fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_DISABLE;
        return 0;
    }

    ad_view_Init(&block_count, &blocklens, &blockoffs, &ub_off, filetype_is_contig, filetype_size, fd);

    ret = mpi_fs_set_fileview(fd->fd_sys, fd->disp, block_count, blocklens, blockoffs, ub_off);
    if (ret < 0) {
        ADIOI_Info_set(fd->info, "view_io", "false");
        fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_DISABLE;
    }

    FreeAdioiTwo(blocklens, blockoffs);
    return ((ret) < 0) ? 0 : 1;
}
