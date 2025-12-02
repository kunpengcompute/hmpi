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
#ifndef MCA_COLL_UCG_REQUEST_H
#define MCA_COLL_UCG_REQUEST_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_list.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/op/op.h"
#include "ompi/mca/coll/base/coll_base_util.h"

#include <ucg/api/ucg.h>

/*
 * Blocking execution pattern:
 *  1. Initialize coll request
 *  2. Execute
 *  3. Add to cache
 * If any failure, goto fallback.
 */
#define MCA_COLL_UCG_REQUEST_PATTERN(_cache_key, _coll_request_init, ...) \
    do {\
        mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get(); \
        rc = mca_coll_ucg_request_common_init(coll_req, false, false); \
        if (rc != OMPI_SUCCESS) { \
            goto fallback; \
        } \
        \
        int rc = _coll_request_init(coll_req, ##__VA_ARGS__); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_rpool_put(coll_req); \
            goto fallback; \
        } \
        \
        rc = mca_coll_ucg_request_execute(coll_req); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_request_cleanup(coll_req); \
            mca_coll_ucg_rpool_put(coll_req); \
            goto fallback; \
        } \
        \
        rc = mca_coll_ucg_rcache_add(coll_req, _cache_key); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_request_cleanup(coll_req); \
            mca_coll_ucg_rpool_put(coll_req); \
        }\
    } while(0)

/*
 * Non-blockingexecution pattern:
 *  1. Initialize coll request
 *  2. Non-blocking execute
 *  3. Mark coll request cacheable
 *  4. Assign ompi request
 * If any failure, goto fallback.
 */
#define MCA_COLL_UCG_REQUEST_PATTERN_NB(_ompi_req, _cache_key, _coll_request_init, ...) \
    do {\
        mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get(); \
        rc = mca_coll_ucg_request_common_init(coll_req, true, false); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_rpool_put(coll_req); \
            goto fallback; \
        } \
        \
        rc = _coll_request_init(coll_req, ##__VA_ARGS__); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_rpool_put(coll_req); \
            goto fallback; \
        } \
        \
        rc = mca_coll_ucg_request_execute_nb(coll_req); \
        if (rc != OMPI_SUCCESS) { \
            mca_coll_ucg_request_cleanup(coll_req); \
            mca_coll_ucg_rpool_put(coll_req); \
            goto fallback; \
        } \
        /* mark cacheable, so when request is completed, it will be added to cache.*/ \
        mca_coll_ucg_rcache_mark_cacheable(coll_req, &args); \
        *_ompi_req = &coll_req->super.super; \
    } while(0)


typedef enum {
    MCA_COLL_UCG_TYPE_BCAST,
    MCA_COLL_UCG_TYPE_IBCAST,
    MCA_COLL_UCG_TYPE_BARRIER,
    MCA_COLL_UCG_TYPE_IBARRIER,
    MCA_COLL_UCG_TYPE_ALLREDUCE,
    MCA_COLL_UCG_TYPE_IALLREDUCE,
    MCA_COLL_UCG_TYPE_ALLTOALLV,
    MCA_COLL_UCG_TYPE_IALLTOALLV,
    MCA_COLL_UCG_TYPE_SCATTERV,
    MCA_COLL_UCG_TYPE_ISCATTERV,
    MCA_COLL_UCG_TYPE_GATHER,
    MCA_COLL_UCG_TYPE_IGATHER, 
    MCA_COLL_UCG_TYPE_GATHERV,
    MCA_COLL_UCG_TYPE_IGATHERV,
    MCA_COLL_UCG_TYPE_ALLGATHERV,
    MCA_COLL_UCG_TYPE_IALLGATHERV,
    MCA_COLL_UCG_TYPE_REDUCE_SCATTER,
    MCA_COLL_UCG_TYPE_IREDUCE_SCATTER,
    MCA_COLL_UCG_TYPE_REDUCE_SCATTER_BLOCK,
    MCA_COLL_UCG_TYPE_IREDUCE_SCATTER_BLOCK,
    MCA_COLL_UCG_TYPE_LAST,
} mca_coll_ucg_type_t;

typedef struct {
    opal_list_t requests;
    int max_size;

    /* access statistics */
    uint64_t total;
    uint64_t hit;
} mca_coll_ucg_rcache_t;

typedef struct {
    opal_free_list_t flist;
} mca_coll_ucg_rpool_t;

typedef struct {
    opal_free_list_item_t super;
    int buf[];
} mca_coll_ucg_subargs_t;
OBJ_CLASS_DECLARATION(mca_coll_ucg_subargs_t);

typedef struct {
    opal_free_list_t flist;
} mca_coll_ucg_subargs_pool_t;

typedef struct mca_coll_bcast_args {
    void *buffer;
    int count;
    ompi_datatype_t *datatype;
    int root;
} mca_coll_bcast_args_t;

typedef struct mca_coll_allreduce_args {
    const void *sbuf;
    void *rbuf;
    int count;
    ompi_datatype_t *datatype;
    ompi_op_t *op;
} mca_coll_allreduce_args_t;

typedef struct mca_coll_alltoallv_args {
    const void *sbuf;
    const int *scounts;
    const int *sdispls;
    ompi_datatype_t *sdtype;
    void *rbuf;
    const int *rcounts;
    const int *rdispls;
    ompi_datatype_t *rdtype;
} mca_coll_alltoallv_args_t;

typedef struct mca_coll_scatterv_args {
    const void *sbuf;
    const int *scounts;
    const int *disps;
    ompi_datatype_t *sdtype;
    void *rbuf;
    int rcount;
    ompi_datatype_t *rdtype;
    int root;
} mca_coll_scatterv_args_t;

typedef struct mca_coll_gather_args {
    const void *sbuf;
    int scount;
    ompi_datatype_t *sdtype;
    void *rbuf;
    int rcount;
    ompi_datatype_t *rdtype;
    int root;
} mca_coll_gather_args_t;

typedef struct mca_coll_gatherv_args {
    const void *sbuf;
    int scount;
    ompi_datatype_t *sdtype;
    void *rbuf;
    const int *rcounts;
    const int *disps;
    ompi_datatype_t *rdtype;
    int root;
} mca_coll_gatherv_args_t;

typedef struct mca_coll_allgatherv_args {
    const void *sbuf;
    int scount;
    ompi_datatype_t *sdtype;
    void *rbuf;
    const int *rcounts;
    const int *disps;
    ompi_datatype_t *rdtype;
} mca_coll_allgatherv_args_t;

typedef struct mca_coll_reduce_scatter_args {
    const void *sbuf;
    ompi_datatype_t *dtype;
    void *rbuf;
    const int *rcounts;
    ompi_op_t *op;
} mca_coll_reduce_scatter_args_t;

typedef struct mca_coll_reduce_scatter_block_args {
    const void *sbuf;
    ompi_datatype_t *dtype;
    void *rbuf;
    int rcount;
    ompi_op_t *op;
} mca_coll_reduce_scatter_block_args_t;

typedef struct mca_coll_ucg_args {
    mca_coll_ucg_type_t coll_type;
    ompi_communicator_t *comm;
    union {
        mca_coll_bcast_args_t bcast;
        mca_coll_allreduce_args_t allreduce;
        mca_coll_alltoallv_args_t alltoallv;
        mca_coll_scatterv_args_t scatterv;
        mca_coll_gather_args_t gather;
        mca_coll_gatherv_args_t gatherv;
        mca_coll_allgatherv_args_t allgatherv;
        mca_coll_reduce_scatter_args_t reduce_scatter;
        mca_coll_reduce_scatter_block_args_t reduce_scatter_block;
    };
    /* Stores pointers in the rcache, combine with deep copy content */
    const int32_t *scounts;
    const int32_t *sdispls;
    const int32_t *rcounts;
    const int32_t *rdispls;
} mca_coll_ucg_args_t;

typedef struct mca_coll_ucg_req {
    ompi_coll_base_nbc_request_t super;
    ucg_request_h ucg_req;
    ucg_request_info_t info;
    /* only cached request need to fill the following fields */
    opal_list_item_t list;
    mca_coll_ucg_args_t args;
    bool cacheable;
} mca_coll_ucg_req_t;
OBJ_CLASS_DECLARATION(mca_coll_ucg_req_t);


extern mca_coll_ucg_rpool_t mca_coll_ucg_rpool;
/* Initialize coll request pool */
int mca_coll_ucg_rpool_init(void);
/* cleanup the coll request pool */
void mca_coll_ucg_rpool_cleanup(void);
/* get an empty coll request */
static inline mca_coll_ucg_req_t* mca_coll_ucg_rpool_get(void)
{
    return (mca_coll_ucg_req_t*)opal_free_list_wait(&mca_coll_ucg_rpool.flist);
}

/* give back the coll request */
static inline void mca_coll_ucg_rpool_put(mca_coll_ucg_req_t *coll_req)
{
    opal_free_list_return(&mca_coll_ucg_rpool.flist, (opal_free_list_item_t*)coll_req);
    return;
}

extern mca_coll_ucg_subargs_pool_t mca_coll_ucg_subargs_pool;
/* Initialize coll subargs pool */
int mca_coll_ucg_subargs_pool_init(uint32_t size);
/* cleanup the coll subargs pool */
void mca_coll_ucg_subargs_pool_cleanup(void);
/* get an empty coll subargs */
static inline mca_coll_ucg_subargs_t* mca_coll_ucg_subargs_pool_get(void)
{
    return (mca_coll_ucg_subargs_t*)opal_free_list_wait(&mca_coll_ucg_subargs_pool.flist);
}
/* give back the coll subargs */
static inline void mca_coll_ucg_subargs_pool_put(mca_coll_ucg_subargs_t *subargs)
{
    opal_free_list_return(&mca_coll_ucg_subargs_pool.flist, (opal_free_list_item_t*)subargs);
    return;
}

/* Initialize request cache */
int mca_coll_ucg_rcache_init(int size);

/* Init ucg progress npolls */
void mca_coll_ucg_npolls_init(int n);

/* Cleanup request cache */
void mca_coll_ucg_rcache_cleanup(void);

/* Used in non-blocking and persistent requests, so that coll request will be
   cached when it's completed. */
void mca_coll_ucg_rcache_mark_cacheable(mca_coll_ucg_req_t *coll_req, mca_coll_ucg_args_t *key);

/* add a new coll request to cache, the coll request should be allocated from rpool. */
int mca_coll_ucg_rcache_add(mca_coll_ucg_req_t *coll_req, mca_coll_ucg_args_t *key);

/* find the matched coll request in cache and return it */
mca_coll_ucg_req_t* mca_coll_ucg_rcache_get(mca_coll_ucg_args_t *key);

/* put the coll request that returned by get() routine into cache */
void mca_coll_ucg_rcache_put(mca_coll_ucg_req_t *coll_req);

/* delete the coll request from request cache and return it to rpool */
void mca_coll_ucg_rcache_del(mca_coll_ucg_req_t *coll_req);

/* Delete requests of the specified comm. */
void mca_coll_ucg_rcache_del_by_comm(ompi_communicator_t *comm);


/* Initialize the common part of the request */
int mca_coll_ucg_request_common_init(mca_coll_ucg_req_t *coll_req,
                                     bool nb,
                                     bool persistent);

/* cleanup coll request */
void mca_coll_ucg_request_cleanup(mca_coll_ucg_req_t *coll_req);

/* execute request in blocking mode */
int mca_coll_ucg_request_execute(mca_coll_ucg_req_t *coll_req);

/* execute request in non-blocking mode */
int mca_coll_ucg_request_execute_nb(mca_coll_ucg_req_t *coll_req);

/* Try to find the request in the cache and execute */
int mca_coll_ucg_request_execute_cache(mca_coll_ucg_args_t *key);

/* Try to find the request in the cache and execute */
int mca_coll_ucg_request_execute_cache_nb(mca_coll_ucg_args_t *key,
                                          mca_coll_ucg_req_t **coll_req);

#endif //MCA_COLL_UCG_REQUEST_H