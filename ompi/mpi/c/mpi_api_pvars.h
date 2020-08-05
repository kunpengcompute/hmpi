/**
* Copyright (C) Huawei Technologies Co., Ltd. 2020.  ALL RIGHTS RESERVED.
*/

#if !defined(_MPI_API_PVARS_H_)
#define _MPI_API_PVARS_H_

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/runtime/params.h"
#include "opal/mca/timer/timer.h"
#include "opal/mca/base/mca_base_pvar.h"
#include "opal/mca/hwloc/hwloc-internal.h"

/* Enables MPI API PVARs */
#define MPI_API_PVARS_ENABLE

/* A structure for the OMPI API PVARs */
typedef struct ompi_api_pvars_s {
   /* Counts the number of consecutive MPI_Allreduce() calls with the same sendbuf and count */
   volatile opal_atomic_size_t allreduce_same_sendbuf_cnt;

   /* Counts the number of consecutive MPI_Allreduce() calls with the same recvbuf and count */
   volatile opal_atomic_size_t allreduce_same_recvbuf_cnt;

   /* Counts the number of consecutive MPI_Reduce() calls with the same sendbuf and count */
   volatile opal_atomic_size_t reduce_same_sendbuf_cnt;

   /* Counts the number of consecutive MPI_Reduce() calls with the same recvbuf and count */
   volatile opal_atomic_size_t reduce_same_recvbuf_cnt;

   /* Counts the number of local ranks in P2P communication API function calls */
   volatile opal_atomic_size_t p2p_to_local_rank_cnt;

   /* Counts the number of remote ranks in P2P communication API function calls */
   volatile opal_atomic_size_t p2p_to_remote_rank_cnt;

   /* Last buffer used by MPI_Bcast() - For preparing histogram */
   volatile opal_atomic_size_t bcast_last_buffer_ptr_used;

   /* Last send size used by MPI_Gatherv(), variadic size - For preparing histogram */
   volatile opal_atomic_size_t gatherv_last_send_size_used;

} ompi_api_pvars_t;

/* Instantiation of the OMPI API PVARs object */
extern ompi_api_pvars_t ompi_api_pvars;

#if defined(MPI_API_PVARS_ENABLE)
/*(void)opal_atomic_fetch_add_size_t(&ompi_api_pvars.allreduce_same_params_cnt, 1);*/
/* MPI_Allreduce: Count same params */
#define MPI_API_PVARS_ALL_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)\
	static const void *prev_sendbuf = NULL;\
	static void *prev_recvbuf = NULL;\
	static int prev_count = 0;\
	if (count == prev_count) {\
		if (sendbuf == prev_sendbuf)\
		   ompi_api_pvars.allreduce_same_sendbuf_cnt++;\
	    if (recvbuf == prev_recvbuf)\
	       ompi_api_pvars.allreduce_same_recvbuf_cnt++;\
	}\
	prev_sendbuf = sendbuf;\
	prev_recvbuf = recvbuf;\
	prev_count = count;


/* MPI_Reduce: Count same params */
#define MPI_API_PVARS_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)\
	static const void *prev_sendbuf = NULL;\
	static void *prev_recvbuf = NULL;\
	static int prev_count = 0;\
	if (count == prev_count) {\
		if (sendbuf == prev_sendbuf)\
		   ompi_api_pvars.reduce_same_sendbuf_cnt++;\
		if (recvbuf == prev_recvbuf)\
		   ompi_api_pvars.reduce_same_recvbuf_cnt++;\
	}\
	prev_sendbuf = sendbuf;\
	prev_recvbuf = recvbuf;\
	prev_count = count;

/*
   Count P2P local/remote
   P2P: MPI_Send(), MPI_Isend(), MPI_Recv(), MPI_Irecv()
*/
#define MPI_API_PVARS_P2P_LOCAL_REMOTE_COUNT(rank_idx, comm) {\
		struct ompi_proc_t *rank_iter =\
            (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, rank_idx);\
        if (OPAL_PROC_ON_LOCAL_SOCKET(rank_iter->super.proc_flags)) {\
		   ompi_api_pvars.p2p_to_local_rank_cnt++; /* UCG_GROUP_MEMBER_DISTANCE_SOCKET */ \
		} else if (OPAL_PROC_ON_LOCAL_HOST(rank_iter->super.proc_flags)) {\
		   ompi_api_pvars.p2p_to_remote_rank_cnt++; /* UCG_GROUP_MEMBER_DISTANCE_HOST */ \
		} else /* UCG_GROUP_MEMBER_DISTANCE_NET */ \
		    ompi_api_pvars.p2p_to_remote_rank_cnt++;\
}

/*
   MPI_Bcast() buffers used for trace (for histogram)
*/
#define MPI_API_PVARS_BCAST_LAST_BUFFER_USED(buffer) \
		ompi_api_pvars.bcast_last_buffer_ptr_used = (uintptr_t)buffer;

/*
   MPI_Gatherv() - Used sizes for trace
*/
#define MPI_API_PVARS_GATHERV_LAST_SEND_SIZE_USED(size) \
		ompi_api_pvars.gatherv_last_send_size_used = size;


#else

#define MPI_API_PVARS_ALL_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)

#define MPI_API_PVARS_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)

MPI_API_PVARS_P2P_LOCAL_REMOTE_COUNT(source, comm)

#define MPI_API_PVARS_BCAST_LAST_BUFFER_USED(buffer)

#define MPI_API_PVARS_GATHERV_LAST_SEND_SIZE_USED(size)

#endif /* MPI_API_PVARS_ENABLE */

/* Initialize collecting MPI API PVARs */
int mpi_api_pvars_init(void);

#endif /* _MPI_API_PVARS_H_ */
