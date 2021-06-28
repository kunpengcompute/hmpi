/*
 * Copyright (c) 2011      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2020      Huawei Technologies Co., Ltd. All rights reserved.
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
#include "coll_ucx_request.h"
#include "coll_ucx_datatype.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

static int mca_coll_ucg_obtain_addr_from_hostname(const char *hostname, struct in_addr *ip_addr)
{
    struct addrinfo hints;
    struct addrinfo *res = NULL, *cur = NULL;
    struct sockaddr_in *addr = NULL;
    int ret;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET; 
    hints.ai_flags = AI_PASSIVE; 
    hints.ai_protocol = 0;
    hints.ai_socktype = SOCK_DGRAM;
    ret = getaddrinfo(hostname, NULL, &hints, &res);
    if (ret < 0) {
        COLL_UCX_ERROR("%s", gai_strerror(ret));
        return OMPI_ERROR;
    }
    
    for (cur = res; cur != NULL; cur = cur->ai_next) {
        addr = (struct sockaddr_in *)cur->ai_addr;
    }

    *ip_addr = addr->sin_addr;
    freeaddrinfo(res);
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_obtain_node_index(unsigned member_count, struct ompi_communicator_t *comm, uint16_t *node_index)
{
    ucg_group_member_index_t rank_idx, rank2_idx;
    uint16_t same_node_flag;
    uint16_t node_idx = 0;
    uint16_t init_node_idx = (uint16_t) - 1;
    int  status, status2;
    struct in_addr ip_address, ip_address2;

    /* initialize: -1: unnumbering flag */
    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        node_index[rank_idx] = init_node_idx;
    }
    
    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        if (node_index[rank_idx] == init_node_idx) {
            struct ompi_proc_t *rank_iter = 
                    (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, rank_idx);
            /* super.proc_hostname give IP address or real hostname */
            /* transform  hostname to IP address for uniform format */
            status = mca_coll_ucg_obtain_addr_from_hostname(rank_iter->super.proc_hostname, &ip_address);
            for (rank2_idx = rank_idx; rank2_idx < member_count; rank2_idx++) {
                struct ompi_proc_t *rank2_iter = 
                    (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, rank2_idx);
                
                status2 = mca_coll_ucg_obtain_addr_from_hostname(rank2_iter->super.proc_hostname, &ip_address2);
                if (status != OMPI_SUCCESS || status2 != OMPI_SUCCESS) {
                    return OMPI_ERROR;
                }

                /* if rank_idx and rank2_idx in same node, same_flag = 1 */
                same_node_flag = (memcmp(&ip_address, &ip_address2, sizeof(ip_address))) ? 0 : 1;
                if (same_node_flag == 1 && node_index[rank2_idx] == init_node_idx) {
                    node_index[rank2_idx] = node_idx;
                }
            }
            node_idx++;
        }
    }
    
    /* make sure every rank has its node_index */
    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        /* some rank do NOT have node_index */
        if (node_index[rank_idx] == init_node_idx) {
            return OMPI_ERROR;
        }
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_create_topo_map(const uint16_t *node_index, const char *topo_info, unsigned loc_size, unsigned rank_cnt)
{
    mca_coll_ucx_component.topo_map = (char**)malloc(sizeof(char*) * rank_cnt);
    if (mca_coll_ucx_component.topo_map == NULL) {
        return OMPI_ERROR;
    }

    unsigned i, j;
    for (i = 0; i < rank_cnt; i++) {
        mca_coll_ucx_component.topo_map[i] = (char*)malloc(sizeof(char) * rank_cnt);
        if (mca_coll_ucx_component.topo_map[i] == NULL) {
            for (j = 0; j < i; j++) {
                free(mca_coll_ucx_component.topo_map[j]);
                mca_coll_ucx_component.topo_map[j] = NULL;
            }
            free(mca_coll_ucx_component.topo_map);
            mca_coll_ucx_component.topo_map = NULL;
            return OMPI_ERROR;
        }
        for (j = 0; j <= i; j++) {
            if (i == j) {
                mca_coll_ucx_component.topo_map[i][j] = (char)UCG_GROUP_MEMBER_DISTANCE_SELF;
                continue;
            }

            if (node_index[i] != node_index[j]) {
                mca_coll_ucx_component.topo_map[i][j] = (char)UCG_GROUP_MEMBER_DISTANCE_NET;
                mca_coll_ucx_component.topo_map[j][i] = (char)UCG_GROUP_MEMBER_DISTANCE_NET;
                continue;
            }

            opal_hwloc_locality_t rel_loc = opal_hwloc_compute_relative_locality(topo_info + i * loc_size, topo_info + j * loc_size);
            enum ucg_group_member_distance distance;
            if (OPAL_PROC_ON_LOCAL_L3CACHE(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
            } else if (OPAL_PROC_ON_LOCAL_SOCKET(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
            } else if (OPAL_PROC_ON_LOCAL_HOST(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
            } else {
                distance = UCG_GROUP_MEMBER_DISTANCE_NET;
            }
            mca_coll_ucx_component.topo_map[i][j] = (char)distance;
            mca_coll_ucx_component.topo_map[j][i] = (char)distance;
        }
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_print_topo_map(unsigned rank_cnt, char **topo_map)
{
    int status = OMPI_SUCCESS;

    /* Print topo map for rank 0. */
    if (ompi_comm_rank(MPI_COMM_WORLD) == 0) {
        unsigned i;
        for (i = 0; i < rank_cnt; i++) {
            char *topo_print = (char*)malloc(rank_cnt + 1);
            if (topo_print == NULL) {
                status = OMPI_ERROR;
                return status;
            }
            for (unsigned j = 0; j < rank_cnt; j++) {
                topo_print[j] = '0' + (int)topo_map[i][j];
            }
            topo_print[rank_cnt] = '\0';
            COLL_UCX_VERBOSE(8, "%s\n", topo_print);
            free(topo_print);
            topo_print = NULL;
        }
    }
    return status;
}

static int mca_coll_ucx_init_global_topo(mca_coll_ucx_module_t *module)
{
    if (mca_coll_ucx_component.topo_map != NULL) {
        return OMPI_SUCCESS;
    }

    /* Derive the 'loc' string from pmix and gather all 'loc' string from all the ranks in the world. */
    int status = OMPI_SUCCESS;
    uint16_t *node_index = NULL;
    unsigned LOC_SIZE = 64;
    unsigned rank_cnt = mca_coll_ucx_component.world_member_count = ompi_comm_size(MPI_COMM_WORLD);
    char *topo_info = (char*)malloc(sizeof(char) * LOC_SIZE * rank_cnt);
    if (topo_info == NULL) {
        status = OMPI_ERROR;
        goto end;
    }
    memset(topo_info, 0, sizeof(char) * LOC_SIZE * rank_cnt);
    int ret;
    char *val = NULL;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_LOCALITY_STRING,
                                   &opal_proc_local_get()->proc_name, &val, OPAL_STRING);
    if (val == NULL || ret != OMPI_SUCCESS) {
        status = OMPI_ERROR;
        goto end;
    }

    ret = ompi_coll_base_allgather_intra_bruck(val, LOC_SIZE, MPI_CHAR, topo_info, LOC_SIZE, MPI_CHAR, MPI_COMM_WORLD, &module->super);
    if (ret != OMPI_SUCCESS) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("ompi_coll_base_allgather_intra_bruck failed");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to init topo map");
    }

    /* Obtain node index to indicate each 'loc' belongs to which node,
       as 'loc' only has info of local machine and contains no network info. */
    node_index = (uint16_t*)malloc(rank_cnt * sizeof(uint16_t));
    if (node_index == NULL) {
        status = OMPI_ERROR;
        goto end;
    }

    ret = mca_coll_ucg_obtain_node_index(rank_cnt, MPI_COMM_WORLD, node_index);
    if (ret != OMPI_SUCCESS) {
        status = OMPI_ERROR;
        goto end;
    }

    /* Create a topo matrix. As it is Diagonal symmetry, only half of the matrix will be computed. */
    ret = mca_coll_ucx_create_topo_map(node_index, topo_info, LOC_SIZE, rank_cnt);
    if (ret != OMPI_SUCCESS) {
        status = OMPI_ERROR;
        goto end;
    }

    ret = mca_coll_ucx_print_topo_map(rank_cnt, mca_coll_ucx_component.topo_map);
    if (ret != OMPI_SUCCESS) {
        status = OMPI_ERROR;
    }

end:
    if (val) {
        free(val);
        val = NULL;
    }

    if (node_index) {
        free(node_index);
        node_index = NULL;
    }
    if (topo_info) {
        free(topo_info);
        topo_info = NULL;
    }
    return status;
}

static int mca_coll_ucx_find_rank_in_comm_world(struct ompi_communicator_t *comm, int comm_rank)
{
    struct ompi_proc_t *proc = (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, comm_rank);
    if (proc == NULL) {
        return -1;
    }

    unsigned i;
    for (i = 0; i < ompi_comm_size(MPI_COMM_WORLD); i++) {
        struct ompi_proc_t *rank_iter = (struct ompi_proc_t*)ompi_comm_peer_lookup(MPI_COMM_WORLD, i);
        if (rank_iter == proc) {
            return i;
        }
    }
    
    return -1;
}

static int mca_coll_ucx_create_comm_topo(ucg_group_params_t *args, struct ompi_communicator_t *comm)
{
    int status;
    if (comm == MPI_COMM_WORLD) {
        if (args->topo_map != NULL) {
            free(args->topo_map);
        }
        args->topo_map = mca_coll_ucx_component.topo_map;
        return OMPI_SUCCESS;
    }

    /* Create a topo matrix. As it is Diagonal symmetry, only half of the matrix will be computed. */
    unsigned i;
    for (i = 0; i < args->member_count; i++) {
        /* Find the rank in the MPI_COMM_WORLD for rank i in the comm. */
        int world_rank_i = mca_coll_ucx_find_rank_in_comm_world(comm, i);
        if (world_rank_i == -1) {
            return OMPI_ERROR;
        }
        for (unsigned j = 0; j <= i; j++) {
            int world_rank_j = mca_coll_ucx_find_rank_in_comm_world(comm, j);
            if (world_rank_j == -1) {
                return OMPI_ERROR;
            }
            args->topo_map[i][j] = mca_coll_ucx_component.topo_map[world_rank_i][world_rank_j];
            args->topo_map[j][i] = mca_coll_ucx_component.topo_map[world_rank_j][world_rank_i];
        }
    }

    status = mca_coll_ucx_print_topo_map(args->member_count, args->topo_map);
    return status;
}

static void mca_coll_ucg_create_distance_array(struct ompi_communicator_t *comm, ucg_group_member_index_t my_idx, ucg_group_params_t *args)
{
    ucg_group_member_index_t rank_idx;
    for (rank_idx = 0; rank_idx < args->member_count; rank_idx++) {
        struct ompi_proc_t *rank_iter = (struct ompi_proc_t*)ompi_comm_peer_lookup(comm, rank_idx);
        rank_iter->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_COLL] = NULL;
        if (rank_idx == my_idx) {
            args->distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_SELF;
        } else if (OPAL_PROC_ON_LOCAL_L3CACHE(rank_iter->super.proc_flags)) {
            args->distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
        } else if (OPAL_PROC_ON_LOCAL_SOCKET(rank_iter->super.proc_flags)) {
            args->distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
        } else if (OPAL_PROC_ON_LOCAL_HOST(rank_iter->super.proc_flags)) {
            args->distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_HOST;
        } else {
            args->distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_NET;
        }
    }
}

static int mca_coll_ucg_datatype_convert(ompi_datatype_t *mpi_dt,
                                         ucp_datatype_t *ucp_dt)
{
    *ucp_dt = mca_coll_ucx_get_datatype(mpi_dt);
    return 0;
}

static ptrdiff_t coll_ucx_datatype_span(void *dt_ext, int count, ptrdiff_t *gap)
{
    struct ompi_datatype_t *dtype = (struct ompi_datatype_t *)dt_ext;
    ptrdiff_t dsize, gp= 0;

    dsize = opal_datatype_span(&dtype->super, count, &gp);
    *gap = gp;
    return dsize;
}

static ucg_group_member_index_t mca_coll_ucx_get_global_member_idx(void *cb_group_obj,
                                                                   ucg_group_member_index_t index)
{
    ompi_communicator_t* comm = (ompi_communicator_t*)cb_group_obj;
    return (ucg_group_member_index_t)mca_coll_ucx_find_rank_in_comm_world(comm, (int)index);
}

static void mca_coll_ucg_init_group_param(struct ompi_communicator_t *comm, ucg_group_params_t *args)
{
    args->member_count      = ompi_comm_size(comm);
    args->cid               = ompi_comm_get_cid(comm);
    args->mpi_reduce_f      = ompi_op_reduce;
    args->resolve_address_f = mca_coll_ucx_resolve_address;
    args->release_address_f = mca_coll_ucx_release_address;
    args->cb_group_obj      = comm;
    args->op_is_commute_f   = ompi_op_is_commute;
    args->mpi_dt_convert    = mca_coll_ucg_datatype_convert;
    args->mpi_datatype_span = coll_ucx_datatype_span;
    args->mpi_global_idx_f  = mca_coll_ucx_get_global_member_idx;
}

static void mca_coll_ucg_arg_free(struct ompi_communicator_t *comm, ucg_group_params_t *args)
{
    unsigned i;

    if (args->distance != NULL) {
        free(args->distance);
        args->distance = NULL;
    }

    if (args->node_index != NULL) {
        free(args->node_index);
        args->node_index = NULL;
    }

    if (comm != MPI_COMM_WORLD && args->topo_map != NULL) {
        for (i = 0; i < args->member_count; i++) {
            if (args->topo_map[i] != NULL) {
                free(args->topo_map[i]);
                args->topo_map[i] = NULL;
            }
        }
        free(args->topo_map);
        args->topo_map = NULL;
    }
}

static void mca_coll_ucg_init_is_socket_balance(ucg_group_params_t *group_params, mca_coll_ucx_module_t *module,
                                                struct ompi_communicator_t *comm)
{
    unsigned pps = ucg_builtin_calculate_ppx(group_params, UCG_GROUP_MEMBER_DISTANCE_SOCKET);
    unsigned ppn = ucg_builtin_calculate_ppx(group_params, UCG_GROUP_MEMBER_DISTANCE_HOST);
    char is_socket_balance = (pps == (ppn - pps) || pps == ppn);
    char result = is_socket_balance;
    int status = ompi_coll_base_allreduce_intra_basic_linear(&is_socket_balance, &result, 1, MPI_CHAR, MPI_MIN,
                                                             comm, &module->super);
    if (status != OMPI_SUCCESS) {
        int error = MPI_ERR_INTERN;
        COLL_UCX_ERROR("ompi_coll_base_allreduce_intra_basic_linear failed");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &error, "Failed to init is_socket_balance");
    }
    group_params->is_socket_balance = result;
    return;
}

static int mca_coll_ucg_create(mca_coll_ucx_module_t *module, struct ompi_communicator_t *comm)
{
    ucs_status_t error;
    ucg_group_params_t args;
    ucg_group_member_index_t my_idx;
    int status = OMPI_SUCCESS;
    unsigned i;

#if OMPI_GROUP_SPARSE
    COLL_UCX_ERROR("Sparse process groups are not supported");
    return UCS_ERR_UNSUPPORTED;
#endif

    /* Fill in group initialization parameters */
    my_idx                 = ompi_comm_rank(comm);
    mca_coll_ucg_init_group_param(comm, &args);
    args.distance          = malloc(args.member_count * sizeof(*args.distance));
    args.node_index        = malloc(args.member_count * sizeof(*args.node_index));
    args.is_bind_to_none   = (OPAL_BIND_TO_NONE == OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy));
    args.topo_map          = NULL;
    
    if (args.distance == NULL || args.node_index == NULL) {
        MCA_COMMON_UCX_WARN("Failed to allocate memory for %lu local ranks", args.member_count);
        status = OMPI_ERROR;
        goto out;
    }

    if (mca_coll_ucx_component.enable_topo_map && (OPAL_BIND_TO_CORE == OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy))) {
        /* Initialize global topology map. */
        args.topo_map = (char**)malloc(sizeof(char*) * args.member_count);
        if (args.topo_map == NULL) {
            MCA_COMMON_UCX_WARN("Failed to allocate memory for %lu local ranks", args.member_count);
            status = OMPI_ERROR;
            goto out;
        }

        for (i = 0; i < args.member_count; i++) {
            args.topo_map[i] = (char*)malloc(sizeof(char) * args.member_count);
            if (args.topo_map[i] == NULL) {
                MCA_COMMON_UCX_WARN("Failed to allocate memory for %lu local ranks", args.member_count);
                status = OMPI_ERROR;
                goto out;
            }
        }

        status = mca_coll_ucx_init_global_topo(module);
        if (status != OMPI_SUCCESS) {
            MCA_COMMON_UCX_WARN("Failed to create global topology.");
            status = OMPI_ERROR;
            goto out;
        }

        if (status == OMPI_SUCCESS) {
            status = mca_coll_ucx_create_comm_topo(&args, comm);
            if (status != OMPI_SUCCESS) {
                MCA_COMMON_UCX_WARN("Failed to create communicator topology.");
                status = OMPI_ERROR;
                goto out;
            }
        }
    }

    /* Generate (temporary) rank-distance array */
    mca_coll_ucg_create_distance_array(comm, my_idx, &args);

    /* Generate node_index for each process */
    status = mca_coll_ucg_obtain_node_index(args.member_count, comm, args.node_index);

    if (status != OMPI_SUCCESS) {
        status = OMPI_ERROR;
        goto out;
    }
 
    mca_coll_ucg_init_is_socket_balance(&args, module, comm);
    error = ucg_group_create(mca_coll_ucx_component.ucg_worker, &args, &module->ucg_group);

    /* Examine comm_new return value */
    if (error != UCS_OK) {
        MCA_COMMON_UCX_WARN("ucg_new failed: %s", ucs_status_string(error));
        status = OMPI_ERROR;
        goto out;
    }

    ucs_list_add_tail(&mca_coll_ucx_component.group_head, &module->ucs_list);
    status = OMPI_SUCCESS;

out:
    mca_coll_ucg_arg_free(comm, &args);
    return status;
}

/*
 * Initialize module on the communicator
 */
static int mca_coll_ucx_module_enable(mca_coll_base_module_t *module,
                                      struct ompi_communicator_t *comm)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*) module;
    int rc;

    if (mca_coll_ucx_component.datatype_attr_keyval == MPI_KEYVAL_INVALID) {
        /* Create a key for adding custom attributes to datatypes */
        ompi_attribute_fn_ptr_union_t copy_fn;
        ompi_attribute_fn_ptr_union_t del_fn;
        copy_fn.attr_datatype_copy_fn  =
                        (MPI_Type_internal_copy_attr_function*)MPI_TYPE_NULL_COPY_FN;
        del_fn.attr_datatype_delete_fn = mca_coll_ucx_datatype_attr_del_fn;
        rc = ompi_attr_create_keyval(TYPE_ATTR, copy_fn, del_fn,
                                     &mca_coll_ucx_component.datatype_attr_keyval,
                                     NULL, 0, NULL);
        if (rc != OMPI_SUCCESS) {
            COLL_UCX_ERROR("Failed to create keyval for UCX datatypes: %d", rc);
            return rc;
        }

        COLL_UCX_FREELIST_INIT(&mca_coll_ucx_component.convs,
                               mca_coll_ucx_convertor_t,
                               128, -1, 128);
    }

    /* prepare the placeholder for the array of request* */
    module->base_data = OBJ_NEW(mca_coll_base_comm_t);
    if (NULL == module->base_data) {
        return OMPI_ERROR;
    }

    rc = mca_coll_ucg_create(ucx_module, comm);
    if (rc != OMPI_SUCCESS) {
        OBJ_RELEASE(module->base_data);
        return rc;
    }

    COLL_UCX_VERBOSE(1, "UCX Collectives Module initialized");
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_ft_event(int state)
{
    return OMPI_SUCCESS;
}

static void mca_coll_ucx_module_construct(mca_coll_ucx_module_t *module)
{
    size_t nonzero = sizeof(module->super.super);
    memset((void*)module + nonzero, 0, sizeof(*module) - nonzero);

    module->super.coll_module_enable  = mca_coll_ucx_module_enable;
    module->super.ft_event            = mca_coll_ucx_ft_event;
    module->super.coll_allreduce      = mca_coll_ucx_allreduce;
    module->super.coll_barrier        = mca_coll_ucx_barrier;
    module->super.coll_bcast          = mca_coll_ucx_bcast;
    ucs_list_head_init(&module->ucs_list);
}

static void mca_coll_ucx_module_destruct(mca_coll_ucx_module_t *module)
{
    if (module->ucg_group) {
        ucg_group_destroy(module->ucg_group);
    }
    ucs_list_del(&module->ucs_list);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucx_module_construct,
                   mca_coll_ucx_module_destruct);
