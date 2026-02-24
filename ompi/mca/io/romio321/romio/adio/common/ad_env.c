#include "ad_env.h"
#include "mpi.h"

/* page mpiio_vars MPIIO Configuration section env_sec Environment Variables
 * - NASMPIO_COMM - Define how data is exchanged on collective
 *   reads and writes.  Possible values:
 *   - 0 - Use MPI_Alltoallv.
 *   - 1 - Use MPI_Isend/MPI_Irecv.
 *   - Default is 1.
 *
 * - NASMPIO_TIMING - collect timing breakdown for MPI I/O collective calls.
 *   Possible values:
 *   - 0 - Do not collect/report timing.
 *   - 1 - Collect/report timing.
 *   - Default is 0.
 *
 * - NASMPIO_TUNEGATHER - Tune how starting and ending offsets are communicated
 *   for aggregator collective i/o.  Possible values:
 *   - 0 - Use two MPI_Allgather's to collect starting and ending offsets.
 *   - 1 - Use MPI_Allreduce(MPI_MAX) to collect starting and ending offsets.
 *   - Default is 0(as GEN).
 *
 * - NASMPIO_TUNEBLOCKING - Tune how aggregate file domains are
 *   calculated (block size).  Possible values:
 *   - 0 - Evenly calculate file domains across aggregators.  Also use
 *   MPI_Isend/MPI_Irecv to exchange domain information.
 *   - 1 - Align file domains with the underlying file system's block size.  Also use
 *   MPI_Alltoallv to exchange domain information.
 *   - Default is 0(as GEN).
 *
 * - NASMPIO_PTHREADIO - Enables a very simple form of asyncronous io where a
 *   pthread is spawned to do the posix writes while the main thread does the
 *   data aggregation - useful for large files where multiple rounds are
 *   required (more that the cb_buffer_size of data per aggregator).   User
 *   must ensure there is hw resource available for the thread to run.  I
 *   am sure there is a better way to do this involving comm threads - this is
 *   just a start.  NOTE: For some reason the stats collected when this is
 *   enabled misses some of the data so the data sizes are off a bit - this is
 *   a statistical issue only, the data is still accurately written out
 *
 * - NASMPIO_P2PCONTIG -  Does simple point-to-point communication between the
 *   aggregator and the procs that feed it.  Performance could be enhanced by a
 *   one-sided put algorithm.  Current implementation allows only 1 round of
 *   data.  Useful/allowed only when:
 * 1.) The datatype is contiguous.
 * 2.) The offsets are increasing in rank-order.
 * 3.) There are no gaps between the offsets.
 * 4.) No single rank has a data size which spans multiple file domains.
 *
 * - NASMPIO_WRITE_AGGMETHOD/NASMPIO_READ_AGGMETHOD -  Replaces the two-phase
 *   collective IO aggregation
 *   with a one-sided algorithm, significantly reducing communication and
 *   memory overhead.  Fully
 *   supports all datasets and datatypes, the only caveat is that any holes in the data
 *   when writing to a pre-existing file are ignored -- there is no read-modify-write
 *   support to maintain the correctness of regions of pre-existing data so every byte
 *   must be explicitly written to maintain correctness.  Users must beware of middle-ware
 *   libraries like PNETCDF which may count on read-modify-write functionality for certain
 *   features (like fill values).  Possible values:
 *   - 0 - Normal two-phase collective IO is used.
 *   - 1 - A separate one-sided MPI_Put or MPI_Get is used for each contigous chunk of data
 *         for a compute to write to or read from the collective buffer on the aggregator.
 *   - 2 - An MPI derived datatype is created using all the contigous chunks and just one
 *         call to MPI_Put or MPI_Get is done with the derived datatype.  On Blue Gene /Q
 *         optimal performance for this is achieved when paired with PAMID_TYPED_ONESIDED=1.
 *   - Default is 0(as GEN).
 *
 * - NASMPIO_ONESIDED_NO_RMW - For one-sided aggregation (NASMPIO_WRITE_AGGMETHOD = 1 or 2)
 *   disable the detection of holes in the data when writing to a pre-existing
 *   file requiring a read-modify-write, thereby avoiding the communication
 *   overhead for this detection.
 *   - 0 (hole detection enabled) or 1 (hole detection disabled)
 *   - Default is 0
 *
 * - NASMPIO_ONESIDED_INFORM_RMW - For one-sided aggregation
 *   (NASMPIO_AGGMETHOD = 1 or 2) generate an informational message informing
 *   the user whether holes exist in the data when writing to a pre-existing
 *   file requiring a read-modify-write, thereby educating the user to set
 *   NASMPIO_ONESIDED_NO_RMW=1 on a future run to avoid the communication
 *   overhead for this detection.
 *   - 0 (disabled) or 1 (enabled)
 *   - Default is 0
 *
 * - NASMPIO_DEVNULLIO - do everything *except* write to / read from the file
 *   system. When experimenting with different two-phase I/O strategies, it's
 *   helpful to remove the highly variable file system from the experiment.
 *   - 0 (disabled) or 1 (enabled)
 *   - Default is 0
 *
 */
static int g_init = 0;
static int g_nasmpio_comm = 1;
static int g_nasmpio_timing = 0;
static int g_nasmpio_tunegather = 0;
static int g_nasmpio_tuneblocking = 0;
static int g_nasmpio_pthreadio = 0;
static int g_nasmpio_p2pcontig = 0;
static int g_nasmpio_write_aggmethod = 0;
static int g_nasmpio_read_aggmethod = 0;
static int g_nasmpio_onesided_no_rmw = 0;
static int g_nasmpio_onesided_always_rmw = 0;
static int g_nasmpio_onesided_inform_rmw = 0;
static int g_group_lock_enable = 0;
static int g_log_level = 1;
static int g_max_size = 1048576; // 1024 * 1024
static int g_io_size_max = 16777216; // 16 * 1024 * 1024
static int g_nasmpio_oceanfs = 0;

static int safe_atoi(char *str, int def)
{
    return (NULL == str) ? def : atoi(str);
}

void ad_oceanfs_get_env_vars()
{
    if (g_init > 0) {
        return;
    }

    g_init = 1;

    g_nasmpio_comm = safe_atoi(getenv("NASMPIO_COMM"), 1);
    g_nasmpio_timing = safe_atoi(getenv("NASMPIO_TIMING"), 0);
    g_nasmpio_tunegather = safe_atoi(getenv("NASMPIO_TUNEGATHER"), 0);
    g_nasmpio_tuneblocking = safe_atoi(getenv("NASMPIO_TUNEBLOCKING"), 0);
    g_nasmpio_pthreadio = safe_atoi(getenv("NASMPIO_PTHREADIO"), 0);
    g_nasmpio_p2pcontig = safe_atoi(getenv("NASMPIO_P2PCONTIG"), 0);
    g_nasmpio_write_aggmethod = safe_atoi(getenv("NASMPIO_WRITE_AGGMETHOD"), 0);
    g_nasmpio_read_aggmethod = safe_atoi(getenv("NASMPIO_READ_AGGMETHOD"), 0);
    g_nasmpio_onesided_no_rmw = safe_atoi(getenv("NASMPIO_ONESIDED_NO_RMW"), 0);
    g_nasmpio_onesided_always_rmw = safe_atoi(getenv("NASMPIO_ONESIDED_ALWAYS_RMW"), 0);
    g_nasmpio_onesided_inform_rmw = safe_atoi(getenv("NASMPIO_ONESIDED_INFORM_RMW"), 0);
    g_max_size = safe_atoi(getenv("NASMPIO_MAX_SIZE"), 1048576);
    g_io_size_max = safe_atoi(getenv("NASMPIO_MAX_IO_SIZE"), 16777216);
    g_nasmpio_oceanfs = safe_atoi(getenv("NASMPIO_OCEANFS"), 0);

    if (g_nasmpio_onesided_always_rmw) {
        g_nasmpio_onesided_no_rmw = 1;
    }

    g_group_lock_enable = safe_atoi(getenv("NASMPIO_GROUP_LOCK"), 1);
    g_log_level = safe_atoi(getenv("NASMPIO_LOG_LEVEL"), 1);
}

int get_nasmpio_timing()
{
    return g_nasmpio_timing;
}
void set_nasmpio_timing(int val)
{
    g_nasmpio_timing = val;
}
int get_nasmpio_comm()
{
    return g_nasmpio_comm;
}
int get_nasmpio_tunegather()
{
    return g_nasmpio_tunegather;
}
int get_nasmpio_tuneblocking()
{
    return g_nasmpio_tuneblocking;
}
int get_nasmpio_pthreadio()
{
    return g_nasmpio_pthreadio;
}
int get_nasmpio_p2pcontig()
{
    return g_nasmpio_p2pcontig;
}
int get_nasmpio_write_aggmethod()
{
    return g_nasmpio_write_aggmethod;
}
int get_nasmpio_read_aggmethod()
{
    return g_nasmpio_read_aggmethod;
}
int get_nasmpio_onesided_no_rmw()
{
    return g_nasmpio_onesided_no_rmw;
}
void set_nasmpio_onesided_no_rmw(int val)
{
    g_nasmpio_onesided_no_rmw = val;
}
int get_nasmpio_onesided_always_rmw()
{
    return g_nasmpio_onesided_always_rmw;
}
void set_nasmpio_onesided_always_rmw(int val)
{
    g_nasmpio_onesided_always_rmw = val;
}
int get_nasmpio_onesided_inform_rmw()
{
    return g_nasmpio_onesided_inform_rmw;
}
int get_group_lock_enable()
{
    return g_group_lock_enable;
}
int get_log_level()
{
    return g_log_level;
}
int get_oceanfs_max_size()
{
    return g_max_size;
}
int get_oceanfs_io_size_max()
{
    return g_io_size_max;
}
int get_nasmpio_oceanfs()
{
    if(g_init == 0) {
        ad_oceanfs_get_env_vars();
    }
    return g_nasmpio_oceanfs;
}