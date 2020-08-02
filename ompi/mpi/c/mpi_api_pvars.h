/**
* Copyright (C) Huawei Technologies Co., Ltd. 2020.  ALL RIGHTS RESERVED.
*/

#if !defined(_MPI_API_PVARS_H_)
#define _MPI_API_PVARS_H_

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/runtime/params.h"
#include "opal/mca/timer/timer.h"
#include "opal/mca/base/mca_base_pvar.h"

/* Enables MPI API PVARs */
#define MPI_API_PVARS_ENABLE

/* Enables MPI API PVARs debug prints */
//#define MPI_T_API_PVARS_DEBUG_ENABLE

#if defined(MPI_T_API_PVARS_DEBUG_ENABLE)
#define MPI_T_API_PVARS_DEBUG_PRINT printf
#else
#define MPI_T_API_PVARS_DEBUG_PRINT
#endif

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

} ompi_api_pvars_t;

/* Instantiation of the OMPI API PVARs object */
extern ompi_api_pvars_t ompi_api_pvars;

typedef opal_atomic_size_t ompi_spc_value_t;

#if defined(MPI_API_PVARS_ENABLE)
/*(void)opal_atomic_fetch_add_size_t(&ompi_api_pvars.allreduce_same_params_cnt, 1);*/
/* MPI_Allreduce: Count same params */
#define MPI_API_PVARS_ALL_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)\
	static const void *prev_sendbuf = NULL;\
	static void *prev_recvbuf = NULL;\
	static int prev_count = 0;\
	MPI_T_API_PVARS_DEBUG_PRINT("ALLREDUCE: %p %p %d, %lu, %lu\n", sendbuf, recvbuf, count, ompi_api_pvars.allreduce_same_sendbuf_cnt, ompi_api_pvars.allreduce_same_recvbuf_cnt);\
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
	MPI_T_API_PVARS_DEBUG_PRINT("REDUCE: %p %p %d, %lu, %lu\n", sendbuf, recvbuf, count, ompi_api_pvars.reduce_same_sendbuf_cnt, ompi_api_pvars.reduce_same_recvbuf_cnt);\
	if (count == prev_count) {\
		if (sendbuf == prev_sendbuf)\
		   ompi_api_pvars.reduce_same_sendbuf_cnt++;\
		if (recvbuf == prev_recvbuf)\
		   ompi_api_pvars.reduce_same_recvbuf_cnt++;\
	}\
	prev_sendbuf = sendbuf;\
	prev_recvbuf = recvbuf;\
	prev_count = count;

#else
#define MPI_API_PVARS_ALL_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)

#define MPI_API_PVARS_REDUCE_SAME_PARAMS_COUNT(sendbuf, recvbuf, count)
#endif /* MPI_API_PVARS_ENABLE */

/* Initialize collecting MPI API PVARs */
int mpi_api_pvars_init(void);

#endif /* _MPI_API_PVARS_H_ */
