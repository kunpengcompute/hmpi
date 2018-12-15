/*
 * Copyright (c) 2020 Huawei Technologies Co., Ltd.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#if HAVE_UCG
#include <ucg/api/ucg.h>
#endif

#include "common_ucx.h"

#include "ompi/communicator/communicator.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/op/op.h"

ompi_common_ucx_module_t ompi_common_ucx = {0};

static void mca_common_ucx_request_init_common(ompi_request_t* ompi_req,
                                               bool req_persistent,
                                               ompi_request_state_t state,
                                               ompi_request_free_fn_t req_free,
                                               ompi_request_cancel_fn_t req_cancel)
{
    OMPI_REQUEST_INIT(ompi_req, req_persistent);
    ompi_req->req_state            = state;
    ompi_req->req_start            = mca_common_ucx_start;
    ompi_req->req_free             = req_free;
    ompi_req->req_cancel           = req_cancel;
    /* This field is used to attach persistant request to a temporary req.
     * Receive (ucp_tag_recv_nb) may call completion callback
     * before the field is set. If the field is not NULL then mca_common_ucx_preq_completion()
     * will try to complete bogus persistant request.
     */
    ompi_req->req_complete_cb_data = NULL;
}

static int mca_common_ucx_request_cancel(ompi_request_t *req, int flag)
{
    ucp_request_cancel(opal_common_ucx.ucp_worker, req);
    return OMPI_SUCCESS;
}

static void mca_common_ucx_request_init(void *request)
{
    ompi_request_t* ompi_req = request;
    OBJ_CONSTRUCT(ompi_req, ompi_request_t);
    mca_common_ucx_request_init_common(ompi_req, false, OMPI_REQUEST_ACTIVE,
                                       mca_common_ucx_request_free,
                                       mca_common_ucx_request_cancel);
}

static void mca_common_ucx_request_cleanup(void *request)
{
    ompi_request_t* ompi_req = request;
    ompi_req->req_state = OMPI_REQUEST_INVALID;
    OMPI_REQUEST_FINI(ompi_req);
    OBJ_DESTRUCT(ompi_req);
}

int mca_common_ucx_start(size_t count, ompi_request_t** requests)
{
    mca_common_ucx_persistent_request_t *preq;
    ompi_request_t *tmp_req;
    size_t i;

    for (i = 0; i < count; ++i) {
        preq = (mca_common_ucx_persistent_request_t *)requests[i];

        if ((preq == NULL) ||
            ((OMPI_REQUEST_PML  != preq->ompi.req_type) &&
             (OMPI_REQUEST_COLL != preq->ompi.req_type))) {
            /* Skip irrelevant requests */
            continue;
        }

        MCA_COMMON_UCX_ASSERT(preq->ompi.req_state != OMPI_REQUEST_INVALID);
        preq->ompi.req_state = OMPI_REQUEST_ACTIVE;
        mca_common_ucx_request_reset(&preq->ompi);

        tmp_req = preq->start_cb(preq);

        if (tmp_req == NULL) {
            MCA_COMMON_UCX_VERBOSE(8, "completed immediately, completing persistent request %p",
                                   (void*)preq);
            preq->ompi.req_status.MPI_ERROR = MPI_SUCCESS;
            ompi_request_complete(&preq->ompi, true);
        } else if (!UCS_PTR_IS_ERR(tmp_req)) {
            if (REQUEST_COMPLETE(tmp_req)) {
                /* tmp_req is already completed */
                MCA_COMMON_UCX_VERBOSE(8, "completing persistent request %p", (void*)preq);
                mca_common_ucx_persistent_request_complete(preq, tmp_req);
            } else {
                /* tmp_req would be completed by callback and trigger completion
                 * of preq */
                MCA_COMMON_UCX_VERBOSE(8, "temporary request %p will complete persistent request %p",
                                (void*)tmp_req, (void*)preq);
                tmp_req->req_complete_cb_data = preq;
                preq->tmp_req                 = tmp_req;
            }
        } else {
            MCA_COMMON_UCX_ERROR("request failed: %s",
                                 ucs_status_string(UCS_PTR_STATUS(tmp_req)));
            return OMPI_ERROR;
        }
    }

    return OMPI_SUCCESS;
}

static int mca_common_ucx_persistent_request_free(ompi_request_t **rptr)
{
    mca_common_ucx_persistent_request_t* preq = (mca_common_ucx_persistent_request_t*)*rptr;
    ompi_request_t *tmp_req = preq->tmp_req;

    preq->ompi.req_state = OMPI_REQUEST_INVALID;
    if (tmp_req != NULL) {
        mca_common_ucx_persistent_request_detach(preq, tmp_req);
        ucp_request_free(tmp_req);
    }

    COMMON_UCX_FREELIST_RETURN(&ompi_common_ucx.requests, &preq->ompi.super);
    *rptr = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}

static int mca_common_ucx_persistent_request_cancel(ompi_request_t *req, int flag)
{
    mca_common_ucx_persistent_request_t* preq = (mca_common_ucx_persistent_request_t*)req;

    if (preq->tmp_req != NULL) {
        ucp_request_cancel(opal_common_ucx.ucp_worker, preq->tmp_req);
    }
    return OMPI_SUCCESS;
}

static void mca_common_ucx_persisternt_request_construct(mca_common_ucx_persistent_request_t* req)
{
    mca_common_ucx_request_init_common(&req->ompi, true, OMPI_REQUEST_INACTIVE,
                                       mca_common_ucx_persistent_request_free,
                                       mca_common_ucx_persistent_request_cancel);
    req->tmp_req = NULL;
}

static void mca_common_ucx_persisternt_request_destruct(mca_common_ucx_persistent_request_t* req)
{
    req->ompi.req_state = OMPI_REQUEST_INVALID;
    OMPI_REQUEST_FINI(&req->ompi);
}

OBJ_CLASS_INSTANCE(mca_common_ucx_persistent_request_t,
                   ompi_request_t,
                   mca_common_ucx_persisternt_request_construct,
                   mca_common_ucx_persisternt_request_destruct);

static int mca_common_completed_request_free(struct ompi_request_t** rptr)
{
    *rptr = MPI_REQUEST_NULL;
    return OMPI_SUCCESS;
}

static int mca_common_completed_request_cancel(struct ompi_request_t* ompi_req, int flag)
{
    return OMPI_SUCCESS;
}

static void mca_common_ucx_completed_request_init(ompi_request_t *ompi_req)
{
    mca_common_ucx_request_init_common(ompi_req, false, OMPI_REQUEST_ACTIVE,
                                       mca_common_completed_request_free,
                                       mca_common_completed_request_cancel);
    ompi_req->req_mpi_object.comm = &ompi_mpi_comm_world.comm;
    ompi_request_complete(ompi_req, false);
}

static int mca_common_ucx_datatype_convert(ompi_datatype_t *mpi_dt,
                                           ucp_datatype_t *ucp_dt)
{
    *ucp_dt = mca_common_ucx_get_datatype(mpi_dt);
    return 0;
}

static int mca_common_ucx_is_dtype_int(ompi_datatype_t *dtype)
{
    /* TODO: what about signed/unsigned? */
    return (dtype->super.flags & OMPI_DATATYPE_FLAG_DATA_INT);
}

static int mca_common_ucx_is_dtype_fp(ompi_datatype_t *dtype)
{
    return (dtype->super.flags & OMPI_DATATYPE_FLAG_DATA_FLOAT);
}

static int mca_common_ucx_is_op_sum(ompi_op_t *op)
{
    return (op == &ompi_mpi_op_sum.op);
}

static int mca_common_ucx_is_op_loc(ompi_op_t *op)
{
    return ((op == &ompi_mpi_op_minloc.op) ||
            (op == &ompi_mpi_op_maxloc.op));
}

static int mca_common_ucx_is_commutative(ompi_op_t *op)
{
    return ompi_op_is_commute(op);
}

static int mca_common_ucx_resolve_address(void *cb_group_obj,
                                          ucg_group_member_index_t rank,
                                          ucp_address_t **addr,
                                          size_t *addr_len)
{
    /* Sanity checks */
    ompi_communicator_t* comm = (ompi_communicator_t*)cb_group_obj;
    if (rank == (ucg_group_member_index_t)comm->c_my_rank) {
        MCA_COMMON_UCX_ERROR("mca_common_ucx_resolve_address(rank=%lu)"
                             "shouldn't be called on its own rank (loopback)", rank);
        return 1;
    }

    /* Check the cache for a previously established connection to that rank */
    ompi_proc_t *proc_peer = ompi_comm_peer_lookup(comm, rank);
    *addr = proc_peer->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_UCX];
    *addr_len = 0; /* UCX doesn't need the length to unpack the address */
    if (*addr) {
       return 0;
    }

    /* Obtain the UCP address of the remote */
    int ret = opal_common_ucx_recv_worker_address(&proc_peer->super.proc_name,
                                                  addr, addr_len);
    if (ret < 0) {
        MCA_COMMON_UCX_ERROR("mca_common_ucx_recv_worker_address(proc=%d rank=%lu) failed",
                             proc_peer->super.proc_name.vpid, rank);
        return 1;
    }


    MCA_COMMON_UCX_VERBOSE(2, "Got proc %d address, size %ld",
                           proc_peer->super.proc_name.vpid, *addr_len);

    /* Cache the connection for future invocations with this rank */
    proc_peer->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_UCX] = *addr;
    return 0;
}

static void mca_common_ucx_release_address(ucp_address_t *addr)
{
    /* no need to free - the address is stored in proc_peer->proc_endpoints */
}

static int mca_common_ucx_neighbors_count(ompi_communicator_t *comm,
                                          int *indegree, int *outdegree)
{
    if (OMPI_COMM_IS_CART(comm)) {
        /* cartesian */
        /* outdegree is always 2*ndims because we need to iterate
         * over empty buffers for MPI_PROC_NULL */
        *outdegree = *indegree = 2 * comm->c_topo->mtc.cart->ndims;
    } else if (OMPI_COMM_IS_GRAPH(comm)) {
        /* graph */
        int nneighbors, rank = ompi_comm_rank(comm);
        mca_topo_base_graph_neighbors_count(comm, rank, &nneighbors);
        *outdegree = *indegree = nneighbors;
    } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
        /* graph */
        *indegree = comm->c_topo->mtc.dist_graph->indegree;
        *outdegree = comm->c_topo->mtc.dist_graph->outdegree;
    } else {
        return 1;
    }

    return 0;
}

static int mca_common_ucx_neighbors_query(ompi_communicator_t *comm,
                                          int *sources, int *destinations)
{
    int res, indeg, outdeg;

    res = mca_common_ucx_neighbors_count(comm, &indeg, &outdeg);
    if (!res) {
        return res;
    }

    if (OMPI_COMM_IS_CART(comm)) {
        /* cartesian */
        int rpeer, speer;

        /* silence clang static analyzer warning */
        assert(indeg == outdeg);

        for (int dim = 0, i = 0; dim < comm->c_topo->mtc.cart->ndims; ++dim) {
            mca_topo_base_cart_shift(comm, dim, 1, &rpeer, &speer);
            sources[i] = destinations[i] = rpeer; i++;
            sources[i] = destinations[i] = speer; i++;
        }
    } else if (OMPI_COMM_IS_GRAPH(comm)) {
        /* graph */
        mca_topo_base_graph_neighbors(comm, ompi_comm_rank(comm), indeg, sources);
        memcpy(destinations, sources, indeg * sizeof (int));
    } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
        /* dist graph */
        mca_topo_base_dist_graph_neighbors(comm, indeg, sources,
                MPI_UNWEIGHTED, outdeg, destinations, MPI_UNWEIGHTED);
    }

    return 0;
}

static void mca_common_ucx_collective_completion(ompi_request_t *request,
                                                 ucs_status_t status)
{
    mca_common_ucx_set_status(&request->req_status, status);
    ompi_request_complete(request, true);
}

int mca_common_ucx_open(const char *prefix, size_t *request_size)
{
    ucp_params_t ucp_params;

    /* Initialize UCX context */
    ucp_params.field_mask        = UCP_PARAM_FIELD_FEATURES |
                                   UCP_PARAM_FIELD_REQUEST_SIZE |
                                   UCP_PARAM_FIELD_REQUEST_INIT |
                                   UCP_PARAM_FIELD_REQUEST_CLEANUP |
                                   UCP_PARAM_FIELD_TAG_SENDER_MASK |
                                   UCP_PARAM_FIELD_MT_WORKERS_SHARED |
                                   UCP_PARAM_FIELD_ESTIMATED_NUM_EPS;
    ucp_params.features          = UCP_FEATURE_TAG |
                                   /* The features below are for SPML-UCX */
                                   UCP_FEATURE_RMA   |
                                   UCP_FEATURE_AMO32 |
                                   UCP_FEATURE_AMO64;
    ucp_params.request_size      = sizeof(ompi_request_t);
    ucp_params.request_init      = mca_common_ucx_request_init;
    ucp_params.request_cleanup   = mca_common_ucx_request_cleanup;
    ucp_params.tag_sender_mask   = MCA_COMMON_UCX_SPECIFIC_SOURCE_MASK;
    ucp_params.mt_workers_shared = 0; /* we do not need mt support for context
                                         since it will be protected by worker */
    ucp_params.estimated_num_eps = ompi_process_info.myprocid.rank;

#ifdef HAVE_DECL_UCP_PARAM_FIELD_ESTIMATED_NUM_PPN
    ucp_params.estimated_num_ppn = opal_process_info.num_local_peers + 1;
    ucp_params.field_mask       |= UCP_PARAM_FIELD_ESTIMATED_NUM_PPN;
#endif

#ifdef HAVE_UCG
    ucg_params_t ucg_params;

    ucp_params.field_mask          |= UCP_PARAM_FIELD_GROUP_PEER_INFO;
    ucp_params.features            |= UCP_FEATURE_GROUPS;
    ucp_params.peer_info.num_local  = ompi_process_info.num_local_peers + 1;
    ucp_params.peer_info.local_idx  = ompi_process_info.my_local_rank;
    ucp_params.peer_info.num_global = ompi_process_info.num_procs;
    ucp_params.peer_info.global_idx = ompi_process_info.myprocid.rank;

    /* Initialize UCG context parameters */
    ucg_params.super                    = &ucp_params;
    ucg_params.field_mask               = UCG_PARAM_FIELD_JOB_UID       |
                                          UCG_PARAM_FIELD_ADDRESS_CB    |
                                          UCG_PARAM_FIELD_NEIGHBORS_CB  |
                                          UCG_PARAM_FIELD_DATATYPE_CB   |
                                          UCG_PARAM_FIELD_REDUCE_OP_CB  |
                                          UCG_PARAM_FIELD_COMPLETION_CB |
                                          UCG_PARAM_FIELD_MPI_IN_PLACE  |
                                          UCG_PARAM_FIELD_HANDLE_FAULT;
    ucg_params.job_uid                  = ompi_process_info.my_name.jobid;
    ucg_params.address.lookup_f         = (typeof(ucg_params.address.lookup_f))
                                          mca_common_ucx_resolve_address;
    ucg_params.address.release_f        = (typeof(ucg_params.address.release_f))
                                          mca_common_ucx_release_address;
    ucg_params.neighbors.vertex_count_f = (typeof(ucg_params.neighbors.vertex_count_f))
                                          mca_common_ucx_neighbors_count;
    ucg_params.neighbors.vertex_query_f = (typeof(ucg_params.neighbors.vertex_query_f))
                                          mca_common_ucx_neighbors_query;
    ucg_params.datatype.convert         = (typeof(ucg_params.datatype.convert))
                                          mca_common_ucx_datatype_convert;
    ucg_params.datatype.is_integer_f    = (typeof(ucg_params.datatype.is_integer_f))
                                          mca_common_ucx_is_dtype_int;
    ucg_params.datatype.is_floating_point_f = (typeof(ucg_params.datatype.is_floating_point_f))
                                              mca_common_ucx_is_dtype_fp;
    ucg_params.reduce_op.reduce_cb_f    = (typeof(ucg_params.reduce_op.reduce_cb_f))
                                          ompi_op_reduce;
    ucg_params.reduce_op.is_sum_f       = (typeof(ucg_params.reduce_op.is_sum_f))
                                          mca_common_ucx_is_op_sum;
    ucg_params.reduce_op.is_loc_expected_f = (typeof(ucg_params.reduce_op.is_loc_expected_f))
                                             mca_common_ucx_is_op_loc;
    ucg_params.reduce_op.is_commutative_f = (typeof(ucg_params.reduce_op.is_commutative_f))
                                            mca_common_ucx_is_commutative;
    ucg_params.completion.coll_comp_cb_f = (typeof(ucg_params.completion.coll_comp_cb_f))
                                           mca_common_ucx_collective_completion;
    ucg_params.mpi_in_place             = (void*)MPI_IN_PLACE;
    ucg_params.fault.mode               = UCG_FAULT_IS_FATAL;

    return opal_common_ucx_open(prefix, &ucg_params,
#else
    return opal_common_ucx_open(prefix, &ucp_params,
#endif
                                request_size);
}

int mca_common_ucx_close(void)
{
    return opal_common_ucx_close();
}

void mca_common_ucx_enable(void)
{
    if (ompi_common_ucx.is_initialized) {
        return;
    }
    ompi_common_ucx.is_initialized = 1;

    OBJ_CONSTRUCT(&ompi_common_ucx.requests, mca_common_ucx_freelist_t);

    COMMON_UCX_FREELIST_INIT(&ompi_common_ucx.requests,
                             mca_common_ucx_persistent_request_t,
                             MCA_COMMON_UCX_PERSISTENT_REQUEST_SLACK,
                             128, -1, 128);

    OBJ_CONSTRUCT(&ompi_common_ucx.completed_request, ompi_request_t);
    mca_common_ucx_completed_request_init(&ompi_common_ucx.completed_request);

    OBJ_CONSTRUCT(&ompi_common_ucx.datatype_ctx, mca_common_ucx_datatype_ctx_t);

}

static void mca_common_ucx_common_finalize(void)
{
    if (!ompi_common_ucx.is_initialized) {
        return;
    }
    ompi_common_ucx.is_initialized = 0;

    OBJ_DESTRUCT(&ompi_common_ucx.datatype_ctx);

    ompi_common_ucx.completed_request.req_state = OMPI_REQUEST_INVALID;
    OMPI_REQUEST_FINI(&ompi_common_ucx.completed_request);
    OBJ_DESTRUCT(&ompi_common_ucx.completed_request);

    OBJ_DESTRUCT(&ompi_common_ucx.requests);
}

int mca_common_ucx_init(const mca_base_component_t *version)
{
    return opal_common_ucx_init(ompi_mpi_thread_multiple, version);
}

int mca_common_ucx_cleanup(void)
{
    mca_common_ucx_common_finalize();

    return opal_common_ucx_cleanup();
}
