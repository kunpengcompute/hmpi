/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2024 Huawei Technologies Co., Ltd.
 *                                All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "coll_ucg.h"
#include "coll_ucg_request.h"
#include "coll_ucg_debug.h"
#include "coll_ucg_dt.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/mca/coll/base/coll_base_topo.h"

static int mca_coll_ucg_comm_neighbors_count(ompi_communicator_t *comm, int *in_deg, int *out_deg)
{
    int ret = OMPI_SUCCESS;
    if (OMPI_COMM_IS_CART(comm)) {
        const int ndims = comm->c_topo->mtc.cart->ndims;
        *in_deg = 2 * ndims;
        *out_deg = 2 * ndims;
    } else if (OMPI_COMM_IS_GRAPH(comm)) {
        int rank = ompi_comm_rank(comm);
        int count = 0;
        ret = mca_topo_base_graph_neighbors_count(comm, rank, &count);
        *in_deg = count;
        *out_deg = count;
    } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
        /* graph */
        *in_deg = comm->c_topo->mtc.dist_graph->indegree;
        *out_deg = comm->c_topo->mtc.dist_graph->outdegree;
    } else {
        UCG_DEBUG("unsupported communicator topology type");
        return OMPI_ERR_BAD_PARAM;
    }
    return ret;
}

static int mca_coll_ucg_comm_neighbors(ompi_communicator_t *comm, int **sources, int *src_cnt, int **dests,
                                       int *dst_cnt)
{
    int in_deg = 0;
    int out_deg = 0;
    int ret;
    *sources = NULL;
    *dests = NULL;
    *src_cnt = 0;
    *dst_cnt = 0;
    ret = mca_coll_ucg_comm_neighbors_count(comm, &in_deg, &out_deg);
    if (ret != OMPI_SUCCESS) {
        goto cleanup;
    }
    *src_cnt = in_deg;
    *dst_cnt = out_deg;
    if (in_deg > 0) {
        *sources = (int *)malloc((size_t)in_deg * sizeof(**sources));
        if (*sources == NULL) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup;
        }
    }
    if (out_deg > 0) {
        *dests = (int *)malloc((size_t)out_deg * sizeof(**dests));
        if (*dests == NULL) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup;
        }
    }
    if (OMPI_COMM_IS_CART(comm)) {
        if (in_deg != out_deg) {
            UCG_DEBUG("cartesian communicator with asymmetric degrees");
            ret = OMPI_ERR_BAD_PARAM;
            goto cleanup;
        }
        const int ndims = comm->c_topo->mtc.cart->ndims;
        int idx = 0;
        for (int dim = 0; dim < ndims; ++dim) {
            int right_peer = MPI_PROC_NULL;
            int left_peer = MPI_PROC_NULL;
            ret = mca_topo_base_cart_shift(comm, dim, 1, &right_peer, &left_peer);
            if (ret != OMPI_SUCCESS)
                goto cleanup;
            (*sources)[idx] = right_peer;
            (*dests)[idx] = right_peer;
            ++idx;
            (*sources)[idx] = left_peer;
            (*dests)[idx] = left_peer;
            ++idx;
        }
    } else if (OMPI_COMM_IS_GRAPH(comm)) {
        ret = mca_topo_base_graph_neighbors(comm, ompi_comm_rank(comm), in_deg, *sources);
        if (ret != OMPI_SUCCESS)
            goto cleanup;
        (void)memcpy(*dests, *sources, (size_t)in_deg * sizeof(**sources));
    } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
        ret =
            mca_topo_base_dist_graph_neighbors(comm, in_deg, *sources, MPI_UNWEIGHTED, out_deg, *dests, MPI_UNWEIGHTED);
        if (ret != OMPI_SUCCESS)
            goto cleanup;
    } else {
        UCG_DEBUG("unexpected topology after count check");
        ret = OMPI_ERR_BAD_PARAM;
        goto cleanup;
    }
cleanup:
    if (ret != OMPI_SUCCESS) {
        free(*sources);
        free(*dests);
        *sources = NULL;
        *dests = NULL;
        *src_cnt = 0;
        *dst_cnt = 0;
    }
    return ret;
}

int mca_coll_ucg_request_neighbor_alltoallw_init(mca_coll_ucg_req_t *coll_req, ompi_communicator_t *comm,
                                                 const void *sbuf, const int *scounts, const MPI_Aint *sdispls,
                                                 ompi_datatype_t *const *sdtypes, void *rbuf, const int *rcounts,
                                                 const MPI_Aint *rdispls, ompi_datatype_t *const *rdtypes,
                                                 mca_coll_ucg_module_t *module, ucg_request_type_t nb)
{
    int rc, indegree, outdegree, *srcs, *dsts;
    rc = mca_coll_ucg_comm_neighbors(comm, &srcs, &indegree, &dsts, &outdegree);
    free(srcs);
    free(dsts);
    if (rc != OMPI_SUCCESS) {
        UCG_DEBUG("fail in topology resolve");
        return rc;
    }
    ucg_dt_h ucg_send_dts[outdegree];
    ucg_dt_h ucg_recv_dts[indegree];
    for (int i = 0; i < indegree; i++) {
        rc = mca_coll_ucg_type_adapt(rdtypes[i], &ucg_recv_dts[i], NULL, NULL);
        if (rc != OMPI_SUCCESS) {
            UCG_DEBUG("Failed to adapt type");
            return rc;
        }
    }
    for (int i = 0; i < outdegree; i++) {
        rc = mca_coll_ucg_type_adapt(sdtypes[i], &ucg_send_dts[i], NULL, NULL);
        if (rc != OMPI_SUCCESS) {
            UCG_DEBUG("Failed to adapt type");
            return rc;
        }
    }
    ucg_request_h ucg_req;
    ucg_status_t status = ucg_request_neighbor_alltoallw_init(sbuf, scounts, sdispls, ucg_send_dts, rbuf, rcounts,
                                                              rdispls, ucg_recv_dts, module->group, &coll_req->info, nb,
                                                              &ucg_req);
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to initialize ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }
    coll_req->ucg_req = ucg_req;
    return OMPI_SUCCESS;
}

int mca_coll_ucg_neighbor_alltoallw_init(const void *sbuf, const int *scounts, const MPI_Aint *sdispls,
                                         ompi_datatype_t *const *sdtype, void *rbuf, const int *rcounts,
                                         const MPI_Aint *rdispls, ompi_datatype_t *const *rdtype,
                                         ompi_communicator_t *comm, ompi_info_t *info, ompi_request_t **request,
                                         mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg neighbor alltoallw init");
    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t *)module;
    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, false, true);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }
    rc = mca_coll_ucg_request_neighbor_alltoallw_init(coll_req, comm, sbuf, scounts, sdispls, sdtype, rbuf, rcounts,
                                                      rdispls, rdtype, ucg_module, UCG_REQUEST_BLOCKING);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }
    *request = &coll_req->super.super;
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback neighbor_alltoallw init");
    return ucg_module->previous_neighbor_alltoallw_init(sbuf, scounts, sdispls, sdtype, rbuf, rcounts, rdispls, rdtype,
                                                        comm, info, request,
                                                        ucg_module->previous_neighbor_alltoallw_init_module);
}
