/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2024 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "coll_ucg_request.h"
#include "coll_ucg_debug.h"
#include "coll_ucg.h"


/* todo: move to op.h ? */
#define OMPI_OP_RETAIN(_op) \
    if (!ompi_op_is_intrinsic(_op)) { \
        OBJ_RETAIN(_op); \
    }

#define OMPI_OP_RELEASE(_op) \
    if (!ompi_op_is_intrinsic(_op)) { \
        OBJ_RELEASE(_op); \
    }

mca_coll_ucg_rpool_t mca_coll_ucg_rpool = {0};
static mca_coll_ucg_rcache_t mca_coll_ucg_rcache;
static int npolls = 10;
const int MAX_NPOLLS = 100;

mca_coll_ucg_subargs_pool_t mca_coll_ucg_subargs_pool = {0};

static void ucg_coll_ucg_rcache_ref(mca_coll_ucg_req_t *coll_req)
{
    mca_coll_ucg_args_t *args = &coll_req->args;
    switch (args->coll_type) {
        case MCA_COLL_UCG_TYPE_BCAST:
        case MCA_COLL_UCG_TYPE_IBCAST:
            OMPI_DATATYPE_RETAIN(args->bcast.datatype);
            break;
        case MCA_COLL_UCG_TYPE_ALLREDUCE:
        case MCA_COLL_UCG_TYPE_IALLREDUCE:
            OMPI_DATATYPE_RETAIN(args->allreduce.datatype);
            OMPI_OP_RETAIN(args->allreduce.op);
            break;
        case MCA_COLL_UCG_TYPE_ALLTOALLV:
        case MCA_COLL_UCG_TYPE_IALLTOALLV:
            OMPI_DATATYPE_RETAIN(args->alltoallv.sdtype);
            OMPI_DATATYPE_RETAIN(args->alltoallv.rdtype);
        case MCA_COLL_UCG_TYPE_BARRIER:
        case MCA_COLL_UCG_TYPE_IBARRIER:
            break;
        case MCA_COLL_UCG_TYPE_SCATTER:
        case MCA_COLL_UCG_TYPE_ISCATTER:
            OMPI_DATATYPE_RETAIN(args->scatter.sdtype);
            OMPI_DATATYPE_RETAIN(args->scatter.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_SCATTERV:
        case MCA_COLL_UCG_TYPE_ISCATTERV:
            OMPI_DATATYPE_RETAIN(args->scatterv.sdtype);
            OMPI_DATATYPE_RETAIN(args->scatterv.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_GATHERV:
        case MCA_COLL_UCG_TYPE_IGATHERV:
            OMPI_DATATYPE_RETAIN(args->gatherv.sdtype);
            OMPI_DATATYPE_RETAIN(args->gatherv.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_ALLGATHERV:
        case MCA_COLL_UCG_TYPE_IALLGATHERV:
            OMPI_DATATYPE_RETAIN(args->allgatherv.sdtype);
            OMPI_DATATYPE_RETAIN(args->allgatherv.rdtype);
            break;
        default:
            UCG_FATAL("Unsupported collective type(%d).", args->coll_type);
            break;
    }
    return;
}

static void ucg_coll_ucg_rcache_deref(mca_coll_ucg_req_t *coll_req)
{
    mca_coll_ucg_args_t *args = &coll_req->args;
    switch (args->coll_type) {
        case MCA_COLL_UCG_TYPE_BCAST:
        case MCA_COLL_UCG_TYPE_IBCAST:
            OMPI_DATATYPE_RELEASE(args->bcast.datatype);
            break;
        case MCA_COLL_UCG_TYPE_ALLREDUCE:
        case MCA_COLL_UCG_TYPE_IALLREDUCE:
            OMPI_DATATYPE_RELEASE(args->allreduce.datatype);
            OMPI_OP_RELEASE(args->allreduce.op);
            break;
        case MCA_COLL_UCG_TYPE_ALLTOALLV:
        case MCA_COLL_UCG_TYPE_IALLTOALLV:
            OMPI_DATATYPE_RELEASE(args->alltoallv.sdtype);
            OMPI_DATATYPE_RELEASE(args->alltoallv.rdtype);
        case MCA_COLL_UCG_TYPE_BARRIER:
        case MCA_COLL_UCG_TYPE_IBARRIER:
            break;
        case MCA_COLL_UCG_TYPE_SCATTER:
        case MCA_COLL_UCG_TYPE_ISCATTER:
            OMPI_DATATYPE_RELEASE(args->scatter.sdtype);
            OMPI_DATATYPE_RELEASE(args->scatter.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_SCATTERV:
        case MCA_COLL_UCG_TYPE_ISCATTERV:
            OMPI_DATATYPE_RELEASE(args->scatterv.sdtype);
            OMPI_DATATYPE_RELEASE(args->scatterv.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_GATHERV:
        case MCA_COLL_UCG_TYPE_IGATHERV:
            OMPI_DATATYPE_RELEASE(args->gatherv.sdtype);
            OMPI_DATATYPE_RELEASE(args->gatherv.rdtype);
            break;
        case MCA_COLL_UCG_TYPE_ALLGATHERV:
        case MCA_COLL_UCG_TYPE_IALLGATHERV:
            OMPI_DATATYPE_RELEASE(args->allgatherv.sdtype);
            OMPI_DATATYPE_RELEASE(args->allgatherv.rdtype);
            break;
        default:
            UCG_FATAL("Unsupported collective type(%d).", args->coll_type);
            break;
    }
    return;
}

static inline void mca_coll_ucg_rcache_full_adjust(void)
{
    // LRU, remove the last item
    opal_list_t *requests = &mca_coll_ucg_rcache.requests;
    if ((int)opal_list_get_size(requests) == mca_coll_ucg_rcache.max_size) {
        opal_list_item_t *item = opal_list_remove_last(requests);
        mca_coll_ucg_req_t *coll_req = container_of(item, mca_coll_ucg_req_t, list);
        mca_coll_ucg_rcache_del(coll_req);
    }
    return;
}

static int mca_coll_ucg_request_start(size_t count, ompi_request_t **requests)
{
    for (size_t i = 0; i < count; ++i) {
        mca_coll_ucg_req_t *coll_req = (mca_coll_ucg_req_t*)requests[i];
        if (coll_req == NULL) {
            continue;
        }

        int rc = mca_coll_ucg_request_execute_nb(coll_req);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }
    return OMPI_SUCCESS;
}

static void mca_coll_ucg_request_complete(void *arg, ucg_status_t status)
{
    mca_coll_ucg_req_t *coll_req = (mca_coll_ucg_req_t*)arg;
    ompi_request_t *ompi_req = &coll_req->super.super;
    if (status == UCG_OK) {
        ompi_req->req_status.MPI_ERROR = MPI_SUCCESS;
    } else {
        ompi_req->req_status.MPI_ERROR = MPI_ERR_INTERN;
    }
    ompi_request_complete(ompi_req, true);
    return;
}

static int mca_coll_ucg_request_free(ompi_request_t **ompi_req)
{
    mca_coll_ucg_req_t *coll_req = (mca_coll_ucg_req_t*)(*ompi_req);
    if (!REQUEST_COMPLETE(*ompi_req)) {
        return MPI_ERR_REQUEST;
    }

    if (coll_req->cacheable) {
        if ((*ompi_req)->req_status.MPI_ERROR == MPI_SUCCESS) {
            mca_coll_ucg_rcache_put(coll_req);
        } else {
            mca_coll_ucg_rcache_del(coll_req);
        }
    } else {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
    }
    *ompi_req = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

static int mca_coll_ucg_request_cancel(ompi_request_t* request, int flag)
{
    return MPI_ERR_REQUEST;
}

OBJ_CLASS_INSTANCE(mca_coll_ucg_req_t,
                   ompi_coll_base_nbc_request_t,
                   NULL,
                   NULL);

OBJ_CLASS_INSTANCE(mca_coll_ucg_subargs_t,
                   opal_free_list_item_t,
                   NULL,
                   NULL);

int mca_coll_ucg_rpool_init(void)
{
    OBJ_CONSTRUCT(&mca_coll_ucg_rpool.flist, opal_free_list_t);
    int rc = opal_free_list_init(&mca_coll_ucg_rpool.flist, sizeof(mca_coll_ucg_req_t),
                                 opal_cache_line_size, OBJ_CLASS(mca_coll_ucg_req_t),
                                 0, 0,
                                 0, INT_MAX, 128,
                                 NULL, 0, NULL, NULL, NULL);
    return rc == OPAL_SUCCESS ? OMPI_SUCCESS : OMPI_ERROR;
}

void mca_coll_ucg_rpool_cleanup(void)
{
    OBJ_DESTRUCT(&mca_coll_ucg_rpool.flist);
    return;
}

int mca_coll_ucg_subargs_pool_init(uint32_t size)
{
    OBJ_CONSTRUCT(&mca_coll_ucg_subargs_pool.flist, opal_free_list_t);
    int rc = opal_free_list_init(&mca_coll_ucg_subargs_pool.flist,
                                 sizeof(mca_coll_ucg_subargs_t) + 4 * size * sizeof(int),
                                 opal_cache_line_size, OBJ_CLASS(mca_coll_ucg_subargs_t),
                                 0, 0,
                                 0, INT_MAX, 128,
                                 NULL, 0, NULL, NULL, NULL);
    return rc == OPAL_SUCCESS ? OMPI_SUCCESS : OMPI_ERROR;
}

void mca_coll_ucg_subargs_pool_cleanup(void)
{
    OBJ_DESTRUCT(&mca_coll_ucg_subargs_pool.flist);
    return;
}

void mca_coll_ucg_npolls_init(int n)
{
    if (n < 1) {
        n = 1;
    } else if (n > MAX_NPOLLS) {
        n = MAX_NPOLLS;
    }
    npolls = n;
    return;
}

int mca_coll_ucg_rcache_init(int size)
{
    if (size <= 0) {
        return OMPI_ERROR;
    }
    mca_coll_ucg_rcache.max_size = size;
    mca_coll_ucg_rcache.total = 0;
    mca_coll_ucg_rcache.hit = 0;
    OBJ_CONSTRUCT(&mca_coll_ucg_rcache.requests, opal_list_t);
    return OMPI_SUCCESS;
}

void mca_coll_ucg_rcache_cleanup(void)
{
    UCG_INFO_IF(mca_coll_ucg_rcache.total > 0, "rcache hit rate: %.2f%% (%lu/%lu)",
                100.0 * mca_coll_ucg_rcache.hit / mca_coll_ucg_rcache.total ,
                mca_coll_ucg_rcache.hit, mca_coll_ucg_rcache.total);
    opal_list_t *requests = &mca_coll_ucg_rcache.requests;
    if (!opal_list_is_empty(requests)) {
        UCG_WARN("%zu requests are not deleted from the cache.", opal_list_get_size(requests));
    }
    OBJ_DESTRUCT(&mca_coll_ucg_rcache.requests);
    return;
}

static void mca_coll_ucg_rcache_coll_req_args_init(mca_coll_ucg_args_t *dst,
                                                   const mca_coll_ucg_args_t *src)
{
    *dst = *src;
    int *scounts, *sdispls, *rcounts, *rdispls, *disps;
    uint32_t i, size = (uint32_t)ompi_comm_size(src->comm);
    mca_coll_ucg_subargs_t *args = NULL;

    switch (src->coll_type) {
        case MCA_COLL_UCG_TYPE_ALLTOALLV:
        case MCA_COLL_UCG_TYPE_IALLTOALLV:
            if (src->alltoallv.scounts == NULL ||
                src->alltoallv.sdispls == NULL ||
                src->alltoallv.rcounts == NULL ||
                src->alltoallv.rdispls == NULL) {
                return;
            }
            args = mca_coll_ucg_subargs_pool_get();
            scounts = args->buf;
            sdispls = scounts + size;
            rcounts = sdispls + size;
            rdispls = rcounts + size;
            for (i = 0; i < size; ++i) {
                scounts[i] = src->alltoallv.scounts[i];
                sdispls[i] = src->alltoallv.sdispls[i];
                rcounts[i] = src->alltoallv.rcounts[i];
                rdispls[i] = src->alltoallv.rdispls[i];
            }
            dst->alltoallv.scounts = scounts;
            dst->alltoallv.sdispls = sdispls;
            dst->alltoallv.rcounts = rcounts;
            dst->alltoallv.rdispls = rdispls;
            dst->scounts = src->alltoallv.scounts;
            dst->sdispls = src->alltoallv.sdispls;
            dst->rcounts = src->alltoallv.rcounts;
            dst->rdispls = src->alltoallv.rdispls;
            break;
        case MCA_COLL_UCG_TYPE_SCATTERV:
        case MCA_COLL_UCG_TYPE_ISCATTERV:
            if (src->scatterv.scounts == NULL ||
                src->scatterv.disps == NULL ||
                ompi_comm_rank(src->comm) != src->scatterv.root) {
                return;
            }
            args = mca_coll_ucg_subargs_pool_get();
            scounts = args->buf;
            disps = scounts + size;
            for (i = 0; i < size; ++i) {
                scounts[i] = src->scatterv.scounts[i];
                disps[i] = src->scatterv.disps[i];
            }
            dst->scatterv.scounts = scounts;
            dst->scatterv.disps = disps;
            dst->scounts = src->scatterv.scounts;
            dst->sdispls = src->scatterv.disps;
            break;
        case MCA_COLL_UCG_TYPE_GATHERV:
        case MCA_COLL_UCG_TYPE_IGATHERV:
             if (src->gatherv.rcounts == NULL ||
                 src->gatherv.disps == NULL ||
                 ompi_comm_rank(src->comm) != src->gatherv.root) {
                return;
            }
            args = mca_coll_ucg_subargs_pool_get();
            rcounts = args->buf;
            disps = rcounts + size;
            for (i = 0; i < size; ++i) {
                rcounts[i] = src->gatherv.rcounts[i];
                disps[i] = src->gatherv.disps[i];
            }
            dst->gatherv.rcounts = rcounts;
            dst->gatherv.disps = disps;
            dst->rcounts = src->gatherv.rcounts;
            dst->rdispls = src->gatherv.disps;
            break;
        case MCA_COLL_UCG_TYPE_ALLGATHERV:
        case MCA_COLL_UCG_TYPE_IALLGATHERV:
             if (src->allgatherv.rcounts == NULL ||
                 src->allgatherv.disps == NULL) {
                return;
            }
            args = mca_coll_ucg_subargs_pool_get();
            rcounts = args->buf;
            disps = rcounts + size;
            for (i = 0; i < size; ++i) {
                rcounts[i] = src->allgatherv.rcounts[i];
                disps[i] = src->allgatherv.disps[i];
            }
            dst->allgatherv.rcounts = rcounts;
            dst->allgatherv.disps = disps;
            dst->rcounts = src->allgatherv.rcounts;
            dst->rdispls = src->allgatherv.disps;
            break;
        default:
            break;
    }
    return;
}

static void mca_coll_ucg_rcache_coll_req_args_uninit(mca_coll_ucg_args_t *args)
{
    void *buf = NULL;
    switch (args->coll_type) {
        case MCA_COLL_UCG_TYPE_ALLTOALLV:
        case MCA_COLL_UCG_TYPE_IALLTOALLV:
            buf = (void *)args->alltoallv.scounts;
            break;
        case MCA_COLL_UCG_TYPE_SCATTERV:
        case MCA_COLL_UCG_TYPE_ISCATTERV:
            if (ompi_comm_rank(args->comm) == args->scatterv.root) {
                buf = (void *)args->scatterv.scounts;
            }
            break;
        case MCA_COLL_UCG_TYPE_GATHERV:
        case MCA_COLL_UCG_TYPE_IGATHERV:
            if (ompi_comm_rank(args->comm) == args->gatherv.root) {
                buf = (void *)args->gatherv.rcounts;
            }
            break;
        case MCA_COLL_UCG_TYPE_ALLGATHERV:
        case MCA_COLL_UCG_TYPE_IALLGATHERV:
            buf = (void *)args->allgatherv.rcounts;
            break;
        default:
            break;
    }
    if (buf != NULL) {
        mca_coll_ucg_subargs_t *data = container_of(buf, mca_coll_ucg_subargs_t, buf);
        mca_coll_ucg_subargs_pool_put(data);
    }
    return;
}

void mca_coll_ucg_rcache_mark_cacheable(mca_coll_ucg_req_t *coll_req,
                                        mca_coll_ucg_args_t *key)
{
    OBJ_CONSTRUCT(&coll_req->list, opal_list_item_t);
    mca_coll_ucg_rcache_coll_req_args_init(&coll_req->args, key);   // deep copy
    ucg_coll_ucg_rcache_ref(coll_req);
    coll_req->cacheable = true;
    return;
}

int mca_coll_ucg_rcache_add(mca_coll_ucg_req_t *coll_req, mca_coll_ucg_args_t *key)
{
    opal_list_t *requests = &mca_coll_ucg_rcache.requests;

    mca_coll_ucg_rcache_mark_cacheable(coll_req, key);

    mca_coll_ucg_rcache_full_adjust();
    opal_list_prepend(requests, &coll_req->list);
    return OMPI_SUCCESS;
}

static bool mca_coll_ucg_rcache_compare(int size, const int *array1, const int *array2, const int32_t *src)
{
    if (array1 == NULL || array2 == NULL) {
        return true;
    }
    if (array1 != src) {
        return false;
    }
    for (int i = 0; i < size; ++i) {
        if (array1[i] != array2[i]) {
            return false;
        }
    }
    return true;
}

static bool mca_coll_ucg_rcache_is_same(const mca_coll_ucg_args_t *key1,
                                        const mca_coll_ucg_args_t *key2)
{
    if (key1->coll_type != key2->coll_type) {
        return false;
    }

    if (key1->comm != key2->comm) {
        return false;
    }

    uint32_t comm_size = (uint32_t)ompi_comm_size(key1->comm);
    bool is_same = false;
    switch (key1->coll_type) {
        case MCA_COLL_UCG_TYPE_BCAST:
        case MCA_COLL_UCG_TYPE_IBCAST: {
            const mca_coll_bcast_args_t *args1 = &key1->bcast;
            const mca_coll_bcast_args_t *args2 = &key2->bcast;
            is_same = args1->buffer == args2->buffer &&
                      args1->count == args2->count &&
                      args1->datatype == args2->datatype &&
                      args1->root == args2->root;
            break;
        }
        case MCA_COLL_UCG_TYPE_BARRIER:
        case MCA_COLL_UCG_TYPE_IBARRIER: {
            is_same = true;
            break;
        }
        case MCA_COLL_UCG_TYPE_ALLREDUCE:
        case MCA_COLL_UCG_TYPE_IALLREDUCE: {
            const mca_coll_allreduce_args_t *args1 = &key1->allreduce;
            const mca_coll_allreduce_args_t *args2 = &key2->allreduce;
            is_same = args1->sbuf == args2->sbuf &&
                      args1->rbuf == args2->rbuf &&
                      args1->count == args2->count &&
                      args1->datatype == args2->datatype &&
                      args1->op == args2->op;
            break;
        }
        case MCA_COLL_UCG_TYPE_ALLTOALLV:
        case MCA_COLL_UCG_TYPE_IALLTOALLV: {
            const mca_coll_alltoallv_args_t *args1 = &key1->alltoallv;
            const mca_coll_alltoallv_args_t *args2 = &key2->alltoallv;
            is_same = args1->sbuf == args2->sbuf &&
                      args1->sdtype == args2->sdtype &&
                      args1->rbuf == args2->rbuf &&
                      args1->rdtype == args2->rdtype;
            is_same = is_same &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->scounts, args2->scounts, key2->scounts) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->sdispls, args2->sdispls, key2->sdispls) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->rcounts, args2->rcounts, key2->rcounts) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->rdispls, args2->rdispls, key2->rdispls);
            break;
        }
        case MCA_COLL_UCG_TYPE_SCATTER:
        case MCA_COLL_UCG_TYPE_ISCATTER: {
            const mca_coll_scatter_args_t *args1 = &key1->scatter;
            const mca_coll_scatter_args_t *args2 = &key2->scatter;
            is_same = args1->rbuf == args2->rbuf &&
                      args1->rcount == args2->rcount &&
                      args1->rdtype == args2->rdtype &&
                      args1->root == args2->root;
            if (ompi_comm_rank(key1->comm) != args1->root) { // Non-root processes don't compare send parms
                break;
            }
            is_same = is_same &&
                      args1->sbuf == args2->sbuf &&
                      args1->sdtype == args2->sdtype;
            break;
        }
        case MCA_COLL_UCG_TYPE_SCATTERV:
        case MCA_COLL_UCG_TYPE_ISCATTERV: {
            const mca_coll_scatterv_args_t *args1 = &key1->scatterv;
            const mca_coll_scatterv_args_t *args2 = &key2->scatterv;
            is_same = args1->rbuf == args2->rbuf &&
                      args1->rcount == args2->rcount &&
                      args1->rdtype == args2->rdtype &&
                      args1->root == args2->root;
            if (ompi_comm_rank(key1->comm) != args1->root) { // Non-root processes don't compare send parms
                break;
            }
            is_same = is_same &&
                      args1->sbuf == args2->sbuf &&
                      args1->sdtype == args2->sdtype &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->scounts, args2->scounts, key2->scounts) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->disps, args2->disps, key2->sdispls);
            break;
        }
        case MCA_COLL_UCG_TYPE_GATHERV:
        case MCA_COLL_UCG_TYPE_IGATHERV: {
            const mca_coll_gatherv_args_t *args1 = &key1->gatherv;
            const mca_coll_gatherv_args_t *args2 = &key2->gatherv;
            is_same = args1->sbuf == args2->sbuf &&
                      args1->scount == args2->scount &&
                      args1->sdtype == args2->sdtype &&
                      args1->root == args2->root;
            if (ompi_comm_rank(key1->comm) != args1->root) { // Non-root processes don't compare recv parms
                break;
            }
            is_same = is_same &&
                      args1->rbuf == args2->rbuf &&
                      args1->rdtype == args2->rdtype &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->rcounts, args2->rcounts, key2->rcounts) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->disps, args2->disps, key2->rdispls);
            break;
        }
        case MCA_COLL_UCG_TYPE_ALLGATHERV:
        case MCA_COLL_UCG_TYPE_IALLGATHERV: {
            const mca_coll_allgatherv_args_t *args1 = &key1->allgatherv;
            const mca_coll_allgatherv_args_t *args2 = &key2->allgatherv;
            is_same = args1->sbuf == args2->sbuf &&
                      args1->scount == args2->scount &&
                      args1->sdtype == args2->sdtype &&
                      args1->rbuf == args2->rbuf &&
                      args1->rdtype == args2->rdtype;
            is_same = is_same &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->rcounts, args2->rcounts, key2->rcounts) &&
                      mca_coll_ucg_rcache_compare(comm_size, args1->disps, args2->disps, key2->rdispls);
            break;
        }
        default:
            UCG_FATAL("Unsupported collective type(%d).", key1->coll_type);
            break;
    }

    return is_same;
}

mca_coll_ucg_req_t* mca_coll_ucg_rcache_get(mca_coll_ucg_args_t *key)
{
    opal_list_t *requests = &mca_coll_ucg_rcache.requests;
    mca_coll_ucg_req_t *coll_req = NULL;
    opal_list_item_t *item = NULL;

    ++mca_coll_ucg_rcache.total;
    OPAL_LIST_FOREACH(item, requests, opal_list_item_t) {
        coll_req = container_of(item, mca_coll_ucg_req_t, list);
        if (mca_coll_ucg_rcache_is_same(key, &coll_req->args)) {
            opal_list_remove_item(requests, item);
            ++mca_coll_ucg_rcache.hit;
            return coll_req;
        }
    }
    return NULL;
}

void mca_coll_ucg_rcache_put(mca_coll_ucg_req_t *coll_req)
{
    if (!coll_req->cacheable) {
        return;
    }
    mca_coll_ucg_rcache_full_adjust();
    opal_list_prepend(&mca_coll_ucg_rcache.requests, &coll_req->list);
    return;
}

void mca_coll_ucg_rcache_del(mca_coll_ucg_req_t *coll_req)
{
    if (!coll_req->cacheable) {
        return;
    }

    coll_req->cacheable = false;
    ucg_coll_ucg_rcache_deref(coll_req);
    mca_coll_ucg_rcache_coll_req_args_uninit(&coll_req->args);
    OBJ_DESTRUCT(&coll_req->list);

    mca_coll_ucg_request_cleanup(coll_req);
    // Convention: All requests in the cache are from the rpool.
    mca_coll_ucg_rpool_put(coll_req);
    return;
}

void mca_coll_ucg_rcache_del_by_comm(ompi_communicator_t *comm)
{
    opal_list_t *requests = &mca_coll_ucg_rcache.requests;
    opal_list_item_t *item;
    opal_list_item_t *next;
    OPAL_LIST_FOREACH_SAFE(item, next, requests, opal_list_item_t) {
        mca_coll_ucg_req_t *coll_req = container_of(item, mca_coll_ucg_req_t, list);
        if (comm == coll_req->args.comm) {
            opal_list_remove_item(requests, item);
            mca_coll_ucg_rcache_del(coll_req);
        }
    }
    return;
}

int mca_coll_ucg_request_common_init(mca_coll_ucg_req_t *coll_req,
                                     bool nb,
                                     bool persistent)
{
    ompi_request_t *ompi_req = &coll_req->super.super;
    OMPI_REQUEST_INIT(ompi_req, persistent);

    ucg_request_info_t *info = &coll_req->info;
    info->field_mask = 0;
    if (nb || persistent) {
        // For those case, the request is not done in the current call stack.
        info->field_mask |= UCG_REQUEST_INFO_FIELD_CB;
        info->complete_cb.cb = mca_coll_ucg_request_complete;
        info->complete_cb.arg = coll_req;

        ompi_req->req_free = mca_coll_ucg_request_free;
        ompi_req->req_cancel = mca_coll_ucg_request_cancel;
    }

    if (persistent) {
        ompi_req->req_type = OMPI_REQUEST_COLL;
        ompi_req->req_start = mca_coll_ucg_request_start;
    }
    coll_req->ucg_req = NULL;
    coll_req->cacheable = false;
    return OMPI_SUCCESS;
}

void mca_coll_ucg_request_cleanup(mca_coll_ucg_req_t *coll_req)
{
    // clean up resource initialized by ${coll_type}_init
    if (coll_req->ucg_req != NULL) {
        ucg_status_t status = ucg_request_cleanup(coll_req->ucg_req);
        if (status != UCG_OK) {
            UCG_ERROR("Failed to cleanup ucg request, %s", ucg_status_string(status));
        }
    }
    // clean up resource initialized by common_init
    OMPI_REQUEST_FINI(&coll_req->super.super);
    return;
}

int mca_coll_ucg_request_execute(mca_coll_ucg_req_t *coll_req)
{
    ucg_request_h ucg_req = coll_req->ucg_req;

    ucg_status_t status;
    status = ucg_request_start(ucg_req);
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to start ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }

    int count = 0;
    while (UCG_INPROGRESS == (status = ucg_request_test(ucg_req))) {
        // TODO: test wether opal_progress() can be removed
        if (++count % npolls == 0) {
            opal_progress();
        }
    }
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to progress ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}

int mca_coll_ucg_request_execute_nb(mca_coll_ucg_req_t *coll_req)
{
    /* ompi_req may be completed in ucg_request_start(), set the state first. */
    ompi_request_t *ompi_req = &coll_req->super.super;
    ompi_req->req_complete = REQUEST_PENDING;
    ompi_req->req_state = OMPI_REQUEST_ACTIVE;

    ucg_status_t status = ucg_request_start(coll_req->ucg_req);
    if (status != UCG_OK) {
        mca_coll_ucg_request_complete(coll_req, status);
        UCG_DEBUG("Failed to start ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

int mca_coll_ucg_request_execute_cache(mca_coll_ucg_args_t *key)
{
    mca_coll_ucg_req_t *coll_req = NULL;
    coll_req = mca_coll_ucg_rcache_get(key);
    if (coll_req == NULL) {
        return OMPI_ERR_NOT_FOUND;
    }
    int rc = mca_coll_ucg_request_execute(coll_req);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rcache_del(coll_req);
        return rc;
    }
    mca_coll_ucg_rcache_put(coll_req);
    return OMPI_SUCCESS;
}

int mca_coll_ucg_request_execute_cache_nb(mca_coll_ucg_args_t *key,
                                          mca_coll_ucg_req_t **coll_req)
{
    mca_coll_ucg_req_t *tmp_coll_req;
    tmp_coll_req = mca_coll_ucg_rcache_get(key);
    if (tmp_coll_req == NULL) {
        return OMPI_ERR_NOT_FOUND;
    }
    int rc = mca_coll_ucg_request_execute_nb(tmp_coll_req);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rcache_del(tmp_coll_req);
        return rc;
    }
    *coll_req = tmp_coll_req;
    // mca_coll_ucg_request_free() will put the coll_req into cache again.
    return OMPI_SUCCESS;
}
