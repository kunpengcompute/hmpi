/*
 * Copyright (c) 2011 Mellanox Technologies. All rights reserved.
 * Copyright (c) 2014 Research Organization for Information Science and
 *                    Technology (RIST). All rights reserved.
 * Copyright (c) 2019 Huawei Technologies Co., Ltd. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/op/op.h"

#include "coll_ucx.h"

static int mca_coll_ucg_create(mca_coll_ucx_module_t *module,
                               struct ompi_communicator_t *comm)
{
#if OMPI_GROUP_SPARSE
    COLL_UCX_ERROR("Sparse process groups are not supported");
    return UCS_ERR_UNSUPPORTED;
#endif

    /* Fill in group initialization parameters */
    ucg_group_params_t args;
    args.field_mask               = UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |
                                    UCG_GROUP_PARAM_FIELD_MEMBER_INDEX |
                                    UCG_GROUP_PARAM_FIELD_CB_CONTEXT   |
                                    UCG_GROUP_PARAM_FIELD_DISTANCES    |
                                    UCG_GROUP_PARAM_FIELD_NAME;
    args.member_count             = ompi_comm_size(comm);
    args.member_index             = ompi_comm_rank(comm);
    args.cb_context               = comm;
    args.name                     = comm->c_name;
    args.distance_type            = UCG_GROUP_DISTANCE_TYPE_ARRAY;
    args.distance_array           = alloca(args.member_count *
                                           sizeof(*args.distance_array));
    if (args.distance_array == NULL) {
        COLL_UCX_ERROR("Failed to allocate memory for %u local ranks", args.member_count);
        return OMPI_ERROR;
    }

    /* Generate (temporary) rank-distance array */
    ucg_group_member_index_t rank_idx;
    for (rank_idx = 0; rank_idx < args.member_count; rank_idx++) {
        struct ompi_proc_t *rank_iter =
                (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, rank_idx);
        if (rank_idx == args.member_index) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_NONE;
        } else if (OPAL_PROC_ON_LOCAL_HWTHREAD(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_HWTHREAD;
        } else if (OPAL_PROC_ON_LOCAL_CORE(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_CORE;
        } else if (OPAL_PROC_ON_LOCAL_L1CACHE(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_L1CACHE;
        } else if (OPAL_PROC_ON_LOCAL_L2CACHE(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_L2CACHE;
        } else if (OPAL_PROC_ON_LOCAL_L3CACHE(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
        } else if (OPAL_PROC_ON_LOCAL_SOCKET(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
        } else if (OPAL_PROC_ON_LOCAL_NUMA(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_NUMA;
        } else if (OPAL_PROC_ON_LOCAL_BOARD(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_BOARD;
        } else if (OPAL_PROC_ON_LOCAL_HOST(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_HOST;
        } else if (OPAL_PROC_ON_LOCAL_CU(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_CU;
        } else if (OPAL_PROC_ON_LOCAL_CLUSTER(rank_iter->super.proc_flags)) {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_CLUSTER;
        } else {
            args.distance_array[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_UNKNOWN;
        }
    }

    /* TODO: use additional info, such as OMPI_COMM_IS_CART(comm) */
    /* TODO: add support for comm->c_remote_comm  */
    /* TODO: add support for sparse group storage */

    ucs_status_t error = ucg_group_create(opal_common_ucx.ucp_worker,
                                          &args, &module->ucg_group);

    /* Examine comm_new return value */
    if (error != UCS_OK) {
        COLL_UCX_ERROR("ucg_group_create failed: %s", ucs_status_string(error));
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

/*
 * Initialize module on the communicator
 */
static int mca_coll_ucx_module_enable(mca_coll_base_module_t *module,
                                      struct ompi_communicator_t *comm)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*) module;
    int rc;

    /* Initialize some structures, e.g. datatype context, if haven't already */
    mca_common_ucx_enable();

    /* prepare the placeholder for the array of request* */
    module->base_data = OBJ_NEW(mca_coll_base_comm_t);
    if (NULL == module->base_data) {
        return OMPI_ERROR;
    }

    rc = mca_coll_ucg_create(ucx_module, comm);
    if (rc != OMPI_SUCCESS)
        return rc;

    COLL_UCX_VERBOSE(1, "UCX Collectives Module initialized");

    return OMPI_SUCCESS;
}

static void mca_coll_ucx_module_construct(mca_coll_ucx_module_t *module)
{
    memset(&module->super.super + 1, 0,
           sizeof(*module) - sizeof(module->super.super));

    module->super.coll_module_enable  = mca_coll_ucx_module_enable;
    module->super.coll_allreduce      = mca_coll_ucx_component.stable_reduce ?
                                        mca_coll_ucx_allreduce_stable :
                                        mca_coll_ucx_allreduce;
/*    module->super.coll_iallreduce     = mca_coll_ucx_iallreduce; */
/*    module->super.coll_allreduce_init = mca_coll_ucx_allreduce_init; */
    module->super.coll_barrier        = mca_coll_ucx_barrier;
    module->super.coll_bcast          = mca_coll_ucx_bcast;
    module->super.coll_reduce         = mca_coll_ucx_component.stable_reduce ?
                                        mca_coll_ucx_reduce_stable :
                                        mca_coll_ucx_reduce;
    module->super.coll_scatter        = mca_coll_ucx_scatter;
    module->super.coll_scatterv       = mca_coll_ucx_scatterv;
    module->super.coll_gather         = mca_coll_ucx_gather;
    module->super.coll_gatherv        = mca_coll_ucx_gatherv;
/*    module->super.coll_allgather      = mca_coll_ucx_allgather; */
    module->super.coll_alltoall       = mca_coll_ucx_alltoall;
}

static void mca_coll_ucx_module_destruct(mca_coll_ucx_module_t *module) {
    ucg_group_destroy(module->ucg_group);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucx_module_construct,
                   mca_coll_ucx_module_destruct);
