#include <unistd.h>
#include <sys/types.h>
#include "ad_oceanfs.h"
#include "ad_oceanfs_common.h"

/* keyval hack to both tell us if we've already initialized im and also
 * close it down when mpi exits */
static int g_Initialized = MPI_KEYVAL_INVALID;

void ADIOI_OCEANFS_Init(int rank, int *error_code)
{
    /* do nothing if we've already fired up the OCEANFS interface */
    if (g_Initialized != MPI_KEYVAL_INVALID) {
        *error_code = MPI_SUCCESS;
        return;
    }

    *error_code = MPI_SUCCESS;

    /* just like romio does, we make a dummy attribute so we
     * get cleaned up */
}

int ADIOI_OCEANFS_Set_lock(FDTYPE fd, int cmd, int type, ADIO_Offset offset, int whence, ADIO_Offset len)
{
    ROMIO_LOG(AD_LOG_LEVEL_ALL, "interface");
    int err, sav_errno;
    int err_count = 0;
    struct flock lock;
    static const int ten_thousand = 10000;

    if (len == 0) {
        return MPI_SUCCESS;
    }

#ifdef NEEDS_INT_CAST_WITH_FLOCK
    lock.l_type = type;
    lock.l_start = (int)offset;
    lock.l_whence = whence;
    lock.l_len = (int)len;
#else
    lock.l_type = type;
    lock.l_whence = whence;
    lock.l_start = offset;
    lock.l_len = len;
#endif

    sav_errno = errno; /* save previous errno in case we recover from retryable errors */
    errno = 0;
    do {
        err = fcntl(fd, cmd, &lock);
        ROMIO_LOG((err) < 0 ? AD_LOG_LEVEL_ERR : AD_LOG_LEVEL_ALL,
            "fcntl fd:%d,ret:%d", fd, err);
    } while (err && ((errno == EINTR) || ((errno == EINPROGRESS) && (++err_count < ten_thousand))));

    if (err && (errno != EBADF)) {
        /* FIXME: This should use the error message system, especially for MPICH */
        ROMIO_LOG(AD_LOG_LEVEL_ERR,
            "cmd:%d,type:%d,errno:%d,offset:%llu,len:%llu",
            cmd, type, errno, (unsigned long long)offset, (unsigned long long)len);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (!err) {            /* report fcntl failure errno's (EBADF), otherwise */
        errno = sav_errno; /* restore previous errno in case we recovered from retryable errors */
    }

    return (err == 0) ? MPI_SUCCESS : MPI_ERR_UNKNOWN;
}
