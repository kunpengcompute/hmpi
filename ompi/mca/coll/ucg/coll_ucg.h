/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2022 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#ifndef MCA_COLL_UCG_H
#define MCA_COLL_UCG_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_list.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/op/op.h"

#include <ucg/api/ucg.h>
#include <limits.h>

BEGIN_C_DECLS

#ifndef container_of
    #define container_of(ptr, type, member) ((type *) (((char *) (ptr)) - offsetof(type, member)))
#endif

#if OMPI_MAJOR_VERSION < 5
    #define PMIX_NODEID             OPAL_PMIX_NODEID
    #define PMIX_UINT32             OPAL_UINT32
    #define PMIX_LOCALITY_STRING    OPAL_PMIX_LOCALITY_STRING
    #define PMIX_STRING             OPAL_STRING
#endif

typedef struct {
    /** Base coll component */
    mca_coll_base_component_t super;
    bool initialized;

    /** MCA parameter */
    int priority;               /* Priority of this component */
    int verbose;                /* Verbose level of this component */
    int max_rcache_size;        /* Max size of request cache */
    char *disable_coll;         /* JUST FOR TEST, may remove later */
    char *topology;             /* Topology file path */
    int npolls;                 /* test progress npolls */

    ucg_context_h ucg_context;

    char **blacklist; /**  disabled collective operations */
} mca_coll_ucg_component_t;
OMPI_MODULE_DECLSPEC extern mca_coll_ucg_component_t mca_coll_ucg_component;

typedef struct {
    mca_coll_base_module_t  super;
    ompi_communicator_t    *comm;
    ucg_group_h             group;

    /* blocking fallback */
    mca_coll_base_module_allreduce_fn_t previous_allreduce;
    mca_coll_base_module_t *previous_allreduce_module;

    mca_coll_base_module_bcast_fn_t previous_bcast;
    mca_coll_base_module_t *previous_bcast_module;

    mca_coll_base_module_barrier_fn_t previous_barrier;
    mca_coll_base_module_t *previous_barrier_module;

    mca_coll_base_module_alltoallv_fn_t previous_alltoallv;
    mca_coll_base_module_t *previous_alltoallv_module;

    mca_coll_base_module_scatterv_fn_t previous_scatterv;
    mca_coll_base_module_t *previous_scatterv_module;

    mca_coll_base_module_gather_fn_t previous_gather;
    mca_coll_base_module_t *previous_gather_module;

    mca_coll_base_module_gatherv_fn_t previous_gatherv;
    mca_coll_base_module_t *previous_gatherv_module;

    mca_coll_base_module_allgatherv_fn_t previous_allgatherv;
    mca_coll_base_module_t *previous_allgatherv_module;

    mca_coll_base_module_reduce_scatter_fn_t previous_reduce_scatter;
    mca_coll_base_module_t *previous_reduce_scatter_module;

    mca_coll_base_module_reduce_scatter_block_fn_t previous_reduce_scatter_block;
    mca_coll_base_module_t *previous_reduce_scatter_block_module;

    /* non-blocking fallback */
    mca_coll_base_module_iallreduce_fn_t previous_iallreduce;
    mca_coll_base_module_t *previous_iallreduce_module;

    mca_coll_base_module_ibcast_fn_t previous_ibcast;
    mca_coll_base_module_t *previous_ibcast_module;

    mca_coll_base_module_ibarrier_fn_t previous_ibarrier;
    mca_coll_base_module_t *previous_ibarrier_module;

    mca_coll_base_module_ialltoallv_fn_t previous_ialltoallv;
    mca_coll_base_module_t *previous_ialltoallv_module;

    mca_coll_base_module_iscatterv_fn_t previous_iscatterv;
    mca_coll_base_module_t *previous_iscatterv_module;

    mca_coll_base_module_igather_fn_t previous_igather;
    mca_coll_base_module_t *previous_igather_module;

    mca_coll_base_module_igatherv_fn_t previous_igatherv;
    mca_coll_base_module_t *previous_igatherv_module;

    mca_coll_base_module_iallgatherv_fn_t previous_iallgatherv;
    mca_coll_base_module_t *previous_iallgatherv_module;

    mca_coll_base_module_ireduce_scatter_fn_t previous_ireduce_scatter;
    mca_coll_base_module_t *previous_ireduce_scatter_module;

    mca_coll_base_module_ireduce_scatter_block_fn_t previous_ireduce_scatter_block;
    mca_coll_base_module_t *previous_ireduce_scatter_block_module;

    /* persistent fallback */
    mca_coll_base_module_allreduce_init_fn_t previous_allreduce_init;
    mca_coll_base_module_t *previous_allreduce_init_module;

    mca_coll_base_module_bcast_init_fn_t previous_bcast_init;
    mca_coll_base_module_t *previous_bcast_init_module;

    mca_coll_base_module_barrier_init_fn_t previous_barrier_init;
    mca_coll_base_module_t *previous_barrier_init_module;

    mca_coll_base_module_alltoallv_init_fn_t previous_alltoallv_init;
    mca_coll_base_module_t *previous_alltoallv_init_module;

    mca_coll_base_module_scatterv_init_fn_t previous_scatterv_init;
    mca_coll_base_module_t *previous_scatterv_init_module;

    mca_coll_base_module_gather_init_fn_t previous_gather_init;
    mca_coll_base_module_t *previous_gather_init_module;

    mca_coll_base_module_gatherv_init_fn_t previous_gatherv_init;
    mca_coll_base_module_t *previous_gatherv_init_module;

    mca_coll_base_module_allgatherv_init_fn_t previous_allgatherv_init;
    mca_coll_base_module_t *previous_allgatherv_init_module;

    mca_coll_base_module_reduce_scatter_init_fn_t previous_reduce_scatter_init;
    mca_coll_base_module_t *previous_reduce_scatter_init_module;

    mca_coll_base_module_reduce_scatter_block_init_fn_t previous_reduce_scatter_block_init;
    mca_coll_base_module_t *previous_reduce_scatter_block_init_module;
} mca_coll_ucg_module_t;
OBJ_CLASS_DECLARATION(mca_coll_ucg_module_t);

int mca_coll_ucg_init_query(bool enable_progress_threads, bool enable_mpi_threads);
mca_coll_base_module_t *mca_coll_ucg_comm_query(ompi_communicator_t *comm, int *priority);

int mca_coll_ucg_init_once(void);
void mca_coll_ucg_cleanup_once(void);

/* allreduce */
int mca_coll_ucg_allreduce(const void *sbuf, void *rbuf, int count,
                           ompi_datatype_t *dtype, ompi_op_t *op,
                           ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_allreduce_cache(const void *sbuf, void *rbuf, int count,
                                 ompi_datatype_t *dtype, ompi_op_t *op,
                                 ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_iallreduce(const void *sbuf, void *rbuf, int count,
                            ompi_datatype_t *datatype, ompi_op_t *op,
                            ompi_communicator_t *comm, ompi_request_t **request,
                            mca_coll_base_module_t *module);

int mca_coll_ucg_iallreduce_cache(const void *sbuf, void *rbuf, int count,
                                  ompi_datatype_t *datatype, ompi_op_t *op,
                                  ompi_communicator_t *comm, ompi_request_t **request,
                                  mca_coll_base_module_t *module);

int mca_coll_ucg_allreduce_init(const void *sbuf, void *rbuf, int count, ompi_datatype_t *datatype,
                                ompi_op_t *op, ompi_communicator_t *comm, ompi_info_t *info,
                                ompi_request_t **request, mca_coll_base_module_t *module);

/* bcast */
int mca_coll_ucg_bcast(void *buff, int count, ompi_datatype_t *datatype,
                       int root, ompi_communicator_t *comm,
                       mca_coll_base_module_t *module);

int mca_coll_ucg_bcast_cache(void *buff, int count, ompi_datatype_t *datatype,
                             int root, ompi_communicator_t *comm,
                             mca_coll_base_module_t *module);

int mca_coll_ucg_ibcast(void *buffer, int count, MPI_Datatype datatype, int root,
                        ompi_communicator_t *comm, ompi_request_t **request,
                        mca_coll_base_module_t *module);

int mca_coll_ucg_ibcast_cache(void *buffer, int count, MPI_Datatype datatype, int root,
                            ompi_communicator_t *comm, ompi_request_t **request,
                              mca_coll_base_module_t *module);

int mca_coll_ucg_bcast_init(void *buffer, int count, MPI_Datatype datatype, int root,
                            ompi_communicator_t *comm, ompi_info_t *info,
                            ompi_request_t **request, mca_coll_base_module_t *module);

/* alltoallv */
int mca_coll_ucg_alltoallv(const void *sbuf, const int *scounts, const int *sdispls,
                           ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                           const int *rdispls, ompi_datatype_t *rdtype,
                           ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_alltoallv_cache(const void *sbuf, const int *scounts, const int *sdispls,
                                 ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                 const int *rdispls, ompi_datatype_t *rdtype,
                                 ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_ialltoallv(const void *sbuf, const int *scounts, const int *sdispls,
                           ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                           const int *rdispls, ompi_datatype_t *rdtype,
                           ompi_communicator_t *comm, ompi_request_t **request,
                           mca_coll_base_module_t *module);

int mca_coll_ucg_ialltoallv_cache(const void *sbuf, const int *scounts, const int *sdispls,
                                  ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                  const int *rdispls, ompi_datatype_t *rdtype,
                                  ompi_communicator_t *comm, ompi_request_t **request,
                                  mca_coll_base_module_t *module);

int mca_coll_ucg_alltoallv_init(const void *sbuf, const int *scounts, const int *sdispls,
                                ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                const int *rdispls, ompi_datatype_t *rdtype,
                                ompi_communicator_t *comm, ompi_info_t *info,
                                ompi_request_t **request, mca_coll_base_module_t *module);

/* barrier */
int mca_coll_ucg_barrier(ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_barrier_cache(ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucg_ibarrier(ompi_communicator_t *comm, ompi_request_t **request,
                          mca_coll_base_module_t *module);

int mca_coll_ucg_ibarrier_cache(ompi_communicator_t *comm, ompi_request_t **request,
                                mca_coll_base_module_t *module);

int mca_coll_ucg_barrier_init(ompi_communicator_t *comm, ompi_info_t *info,
                              ompi_request_t **request, mca_coll_base_module_t *module);

/* scatterv */
int mca_coll_ucg_scatterv(const void *sbuf, const int *scounts, const int *disps,
                          ompi_datatype_t *sdtype, void *rbuf, int rcount,
                          ompi_datatype_t *rdtype, int root,
                          ompi_communicator_t *comm,
                          mca_coll_base_module_t *module);

int mca_coll_ucg_scatterv_cache(const void *sbuf, const int *scounts, const int *disps,
                                ompi_datatype_t *sdtype, void *rbuf, int rcount,
                                ompi_datatype_t *rdtype, int root,
                                ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);

int mca_coll_ucg_iscatterv(const void *sbuf, const int *scounts, const int *disps,
                           ompi_datatype_t *sdtype, void *rbuf, int rcount,
                           ompi_datatype_t *rdtype, int root,
                           ompi_communicator_t *comm, ompi_request_t **request,
                           mca_coll_base_module_t *module);

int mca_coll_ucg_iscatterv_cache(const void *sbuf, const int *scounts, const int *disps,
                                 ompi_datatype_t *sdtype, void *rbuf, int rcount,
                                 ompi_datatype_t *rdtype, int root,
                                 ompi_communicator_t *comm, ompi_request_t **request,
                                 mca_coll_base_module_t *module);

int mca_coll_ucg_scatterv_init(const void *sbuf, const int *scounts, const int *disps,
                               ompi_datatype_t *sdtype, void *rbuf, int rcount,
                               ompi_datatype_t *rdtype, int root,
                               ompi_communicator_t *comm, ompi_info_t *info,
                               ompi_request_t **request, mca_coll_base_module_t *module);
/* gather */
int mca_coll_ucg_gather(const void *sbuf, int sendcount,
                         ompi_datatype_t *sdtype, void *rbuf, int recvcount,
                         ompi_datatype_t *rdtype, int root,
                         ompi_communicator_t *comm,
                         mca_coll_base_module_t *module);

int mca_coll_ucg_gather_cache(const void *sbuf, int sendcount,
                              ompi_datatype_t *sdtype, void *rbuf, int recvcount,
                              ompi_datatype_t *rdtype, int root,
                              ompi_communicator_t *comm,
                              mca_coll_base_module_t *module);

int mca_coll_ucg_igather(const void *sbuf, int sendcount,
                         ompi_datatype_t *sdtype, void *rbuf, int recvcount,
                         ompi_datatype_t *rdtype, int root,
                         ompi_communicator_t *comm, ompi_request_t **request,
                         mca_coll_base_module_t *module);

int mca_coll_ucg_igather_cache(const void *sbuf, int sendcount,
                               ompi_datatype_t *sdtype, void *rbuf, int recvcount,
                               ompi_datatype_t *rdtype, int root,
                               ompi_communicator_t *comm, ompi_request_t **request,
                               mca_coll_base_module_t *module);

int mca_coll_ucg_gather_init(const void *sbuf, int sendcount,
                              ompi_datatype_t *sdtype, void *rbuf, int recvcount,
                              ompi_datatype_t *rdtype, int root,
                              ompi_communicator_t *comm, ompi_info_t *info,
                              ompi_request_t **request, mca_coll_base_module_t *module);

/* gatherv */
int mca_coll_ucg_gatherv(const void *sbuf, int sendcount,
                         ompi_datatype_t *sdtype, void *rbuf, const int *recvcounts,
                         const int *disps,
                         ompi_datatype_t *rdtype, int root,
                         ompi_communicator_t *comm,
                         mca_coll_base_module_t *module);

int mca_coll_ucg_gatherv_cache(const void *sbuf, int sendcount,
                               ompi_datatype_t *sdtype, void *rbuf, const int *recvcounts,
                               const int *disps,
                               ompi_datatype_t *rdtype, int root,
                               ompi_communicator_t *comm,
                               mca_coll_base_module_t *module);

int mca_coll_ucg_igatherv(const void *sbuf, int sendcount,
                          ompi_datatype_t *sdtype, void *rbuf, const int *recvcounts,
                          const int *disps,
                          ompi_datatype_t *rdtype, int root,
                          ompi_communicator_t *comm, ompi_request_t **request,
                          mca_coll_base_module_t *module);

int mca_coll_ucg_igatherv_cache(const void *sbuf, int sendcount,
                                ompi_datatype_t *sdtype, void *rbuf, const int *recvcounts,
                                const int *disps,
                                ompi_datatype_t *rdtype, int root,
                                ompi_communicator_t *comm, ompi_request_t **request,
                                mca_coll_base_module_t *module);

int mca_coll_ucg_gatherv_init(const void *sbuf, int sendcount,
                              ompi_datatype_t *sdtype, void *rbuf, const int *recvcounts,
                              const int *disps,
                              ompi_datatype_t *rdtype, int root,
                              ompi_communicator_t *comm, ompi_info_t *info,
                              ompi_request_t **request, mca_coll_base_module_t *module);

/* allgatherv */
int mca_coll_ucg_allgatherv(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                            void *rbuf, const int *rcounts, const int *disps,
                            ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                            mca_coll_base_module_t *module);

int mca_coll_ucg_allgatherv_cache(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                  void *rbuf, const int *rcounts, const int *disps,
                                  ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);

int mca_coll_ucg_iallgatherv(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                             void *rbuf, const int *rcounts, const int *disps,
                             ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                             ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_iallgatherv_cache(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                   void *rbuf, const int *rcounts, const int *disps,
                                   ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                   ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_allgatherv_init(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                 void *rbuf, const int *rcounts, const int *disps,
                                 ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                 ompi_info_t *info, ompi_request_t **request,
                                 mca_coll_base_module_t *module);

/* reduce_scatter */
int mca_coll_ucg_reduce_scatter(const void *sbuf, void *rbuf, const int *rcounts, 
                                ompi_datatype_t *dtype, ompi_op_t *op,
                                ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);

int mca_coll_ucg_reduce_scatter_cache(const void *sbuf, void *rbuf, const int *rcounts, 
                                      ompi_datatype_t *dtype, ompi_op_t *op,
                                      ompi_communicator_t *comm,
                                      mca_coll_base_module_t *module);

int mca_coll_ucg_ireduce_scatter(const void *sbuf, void *rbuf, const int *rcounts, 
                                 ompi_datatype_t *dtype, ompi_op_t *op,
                                 ompi_communicator_t *comm,
                                 ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_ireduce_scatter_cache(const void *sbuf, void *rbuf, const int *rcounts, 
                                       ompi_datatype_t *dtype, ompi_op_t *op,
                                       ompi_communicator_t *comm,
                                       ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_reduce_scatter_init(const void *sbuf, void *rbuf, const int *rcounts, 
                                     ompi_datatype_t *dtype, ompi_op_t *op,
                                     ompi_communicator_t *comm,
                                     ompi_info_t *info, ompi_request_t **request,
                                     mca_coll_base_module_t *module);

/* reduce_scatter_block */
int mca_coll_ucg_reduce_scatter_block(const void *sbuf, void *rbuf, int rcount, 
                                ompi_datatype_t *dtype, ompi_op_t *op,
                                ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);

int mca_coll_ucg_reduce_scatter_block_cache(const void *sbuf, void *rbuf, int rcount, 
                                      ompi_datatype_t *dtype, ompi_op_t *op,
                                      ompi_communicator_t *comm,
                                      mca_coll_base_module_t *module);

int mca_coll_ucg_ireduce_scatter_block(const void *sbuf, void *rbuf, int rcount, 
                                 ompi_datatype_t *dtype, ompi_op_t *op,
                                 ompi_communicator_t *comm,
                                 ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_ireduce_scatter_block_cache(const void *sbuf, void *rbuf, int rcount, 
                                       ompi_datatype_t *dtype, ompi_op_t *op,
                                       ompi_communicator_t *comm,
                                       ompi_request_t **request, mca_coll_base_module_t *module);

int mca_coll_ucg_reduce_scatter_block_init(const void *sbuf, void *rbuf, int rcount, 
                                     ompi_datatype_t *dtype, ompi_op_t *op,
                                     ompi_communicator_t *comm,
                                     ompi_info_t *info, ompi_request_t **request,
                                     mca_coll_base_module_t *module);

END_C_DECLS
#endif //MCA_COLL_UCG_H