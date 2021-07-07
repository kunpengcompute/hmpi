#include "ad_oceanfs.h"
#include "adio_extern.h"
#include "ad_oceanfs_pub.h"
#include "hint_fns.h"

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

void ADIOI_OCEANFS_SetInfo(ADIO_File fd, MPI_Info users_info, int *error_code)
{
    char *value = NULL;
    int flag;
    static char myname[] = "ADIOI_OCEANFS_SETINFO";

    value = (char *)ADIOI_Malloc((MPI_MAX_INFO_VAL + 1) * sizeof(char));
    if (fd->info == MPI_INFO_NULL) {
        /* This must be part of the open call. can set striping parameters
            if necessary. */
        MPI_Info_create(&(fd->info));

        ADIOI_Info_set(fd->info, "direct_read", "false");
        ADIOI_Info_set(fd->info, "direct_write", "false");
        fd->direct_read = fd->direct_write = 0;
        /* TODO  initialize oceanfs hints */
        ADIOI_Info_set(fd->info, "view_io", "false");
        fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_DISABLE;

        if (users_info != MPI_INFO_NULL) {
            /* TODO striping information */

            /* view io */
            ADIOI_Info_get(users_info, "view_io", MPI_MAX_INFO_VAL, value, &flag);
            if (flag && (!strcmp(value, "true") || !strcmp(value, "TRUE"))) {
                ADIOI_Info_set(fd->info, "view_io", "true");
                fd->hints->fs_hints.oceanfs.view_io = ADIOI_HINT_ENABLE;
            }
        }
    }

    /* get other hint */
    if (users_info != MPI_INFO_NULL) {
        /* TO DO */
    }

    /* set internal variables for tuning environment variables */
    if (!fd->hints->initialized) {
        ad_oceanfs_get_env_vars();
    }

    /* set the values for collective I/O and data sieving parameters */
    ADIOI_GEN_SetInfo(fd, users_info, error_code);

    /* generic hints might step on striping_unit */
    if (users_info != MPI_INFO_NULL) {
        ADIOI_Info_check_and_install_int(fd, users_info, "striping_unit", NULL, myname, error_code);
    }

    ADIOI_Free(value);

    *error_code = MPI_SUCCESS;
}
