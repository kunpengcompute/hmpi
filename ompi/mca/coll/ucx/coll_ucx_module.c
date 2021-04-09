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

static int mca_coll_ucx_obtain_addr_from_hostname(const char *hostname,
                                                  struct in_addr *ip_addr)
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

static uint16_t* mca_coll_ucx_obtain_node_index(struct ompi_communicator_t *comm)
{
    int status;
    unsigned member_count = ompi_comm_size(comm);
    uint16_t* node_idx = malloc(sizeof(uint16_t) * member_count);
    if (node_idx == NULL) {
        return NULL;
    }
    uint16_t invalid_node_idx = (uint16_t)-1;
    for (unsigned i = 0; i < member_count; ++i) {
        node_idx[i] = invalid_node_idx;
    }

    /* get ip address */
    struct in_addr *ip_address = malloc(sizeof(struct in_addr) * member_count);
    if (ip_address == NULL) {
        goto err_free_node_idx;
    }
    for (unsigned i = 0; i < member_count; ++i) {
        ompi_proc_t *rank = ompi_comm_peer_lookup(comm, i);
        status = mca_coll_ucx_obtain_addr_from_hostname(rank->super.proc_hostname,
                                                        ip_address + i);
        if (status != OMPI_SUCCESS) {
            goto err_free_ip_addr;
        }
    }

    /* assign node index, starts from 0 */
    uint16_t last_node_idx = 0;
    for (unsigned i = 0; i < member_count; ++i) {
        if (node_idx[i] == invalid_node_idx) {
            node_idx[i] = last_node_idx;
            /* find the node with same ipaddr, assign the same node idx */
            for (unsigned j = i + 1; j < member_count; ++j) {
                if (memcmp(&ip_address[i], &ip_address[j], sizeof(struct in_addr)) == 0) {
                    node_idx[j] = last_node_idx;
                }
            }
            ++last_node_idx;
        }
    }
    free(ip_address);
    return node_idx;

err_free_ip_addr:
    free(ip_address);
err_free_node_idx:
    free(node_idx);
    return NULL;
}

static enum ucg_group_member_distance* mca_coll_ucx_obtain_distance(struct ompi_communicator_t *comm)
{
    int my_idx = ompi_comm_rank(comm);
    int member_cnt = ompi_comm_size(comm);
    enum ucg_group_member_distance *distance = malloc(member_cnt * sizeof(enum ucg_group_member_distance));
    if (distance == NULL) {
        return NULL;
    }

    struct ompi_proc_t *rank_iter;
    for (int rank_idx = 0; rank_idx < member_cnt; ++rank_idx) {
        rank_iter = ompi_comm_peer_lookup(comm, rank_idx);
        rank_iter->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_COLL] = NULL;
        if (rank_idx == my_idx) {
            distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_SELF;
        } else if (OPAL_PROC_ON_LOCAL_L3CACHE(rank_iter->super.proc_flags)) {
            distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
        } else if (OPAL_PROC_ON_LOCAL_SOCKET(rank_iter->super.proc_flags)) {
            distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
        } else if (OPAL_PROC_ON_LOCAL_HOST(rank_iter->super.proc_flags)) {
            distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_HOST;
        } else {
            distance[rank_idx] = UCG_GROUP_MEMBER_DISTANCE_NET;
        }
    }
    return distance;
}

static void mca_coll_ucx_deallocate_topo_map(char **topo_map, unsigned member_count)
{
    if (topo_map == NULL) {
        return;
    }
    for (unsigned i = 0; i < member_count; ++i) {
        if (topo_map[i] == NULL) {
            /* The following are NULL too, so break */
            break;
        }
        free(topo_map[i]);
        topo_map[i] = NULL;
    }
    free(topo_map);
    topo_map = NULL;
    return;
}

static char** mca_coll_ucx_allocate_topo_map(unsigned member_count)
{
    char **topo_map = malloc(sizeof(char*) * member_count);
    if (topo_map == NULL) {
        return NULL;
    }
    memset(topo_map, 0, sizeof(char*) * member_count);

    for (unsigned i = 0; i < member_count; ++i) {
        topo_map[i] = malloc(sizeof(char) * member_count);
        if (topo_map[i] == NULL) {
            goto err;
        }
    }

    return topo_map;
err:
    mca_coll_ucx_deallocate_topo_map(topo_map, member_count);
    return NULL;
}

static char** mca_coll_ucx_create_topo_map(const uint16_t *node_index,
                                           char *localities,
                                           unsigned loc_size,
                                           unsigned member_count)
{
    char **topo_map = mca_coll_ucx_allocate_topo_map(member_count);
    if (topo_map == NULL) {
        return NULL;
    }

    unsigned i, j;
    enum ucg_group_member_distance distance;
    opal_hwloc_locality_t rel_loc;
    for (i = 0; i < member_count; ++i) {
        for (j = 0; j <= i; j++) {
            if (i == j) {
                topo_map[i][j] = (char)UCG_GROUP_MEMBER_DISTANCE_SELF;
                continue;
            }

            if (node_index[i] != node_index[j]) {
                topo_map[i][j] = (char)UCG_GROUP_MEMBER_DISTANCE_NET;
                topo_map[j][i] = (char)UCG_GROUP_MEMBER_DISTANCE_NET;
                continue;
            }

            rel_loc = opal_hwloc_compute_relative_locality(localities + i * loc_size,
                                                           localities + j * loc_size);
            if (OPAL_PROC_ON_LOCAL_L3CACHE(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
            } else if (OPAL_PROC_ON_LOCAL_SOCKET(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
            } else if (OPAL_PROC_ON_LOCAL_HOST(rel_loc)) {
                distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
            } else {
                distance = UCG_GROUP_MEMBER_DISTANCE_NET;
            }
            topo_map[i][j] = (char)distance;
            topo_map[j][i] = (char)distance;
        }
    }
    return topo_map;
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

static int mca_coll_ucx_convert_to_global_rank(struct ompi_communicator_t *comm, int rank)
{
    struct ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);
    if (proc == NULL) {
        return -1;
    }

    unsigned i;
    unsigned member_count = ompi_comm_size(MPI_COMM_WORLD);
    for (i = 0; i < member_count; ++i) {
        struct ompi_proc_t *global_proc = ompi_comm_peer_lookup(MPI_COMM_WORLD, i);
        if (global_proc == proc) {
            return i;
        }
    }

    return -1;
}

static int mca_coll_ucx_create_global_topo_map(mca_coll_ucx_module_t *module,
                                               struct ompi_communicator_t *comm)
{
    if (mca_coll_ucx_component.topo_map != NULL) {
        return OMPI_SUCCESS;
    }
    /* get my locality string */
    int ret;
    char *locality = NULL;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_LOCALITY_STRING,
                                   &opal_proc_local_get()->proc_name, &locality, OPAL_STRING);
    if (locality == NULL || ret != OMPI_SUCCESS) {
        free(locality);
        return OMPI_ERROR;
    }
    int locality_size = strlen(locality);

    /* gather all members locality */
    int member_count = ompi_comm_size(comm);
    COLL_UCX_ASSERT(locality_size <= 64);
    unsigned one_locality_size = 64 * sizeof(char);
    unsigned total_locality_size = one_locality_size * member_count;
    char *localities = (char*)malloc(total_locality_size);
    if (localities == NULL) {
        ret = OMPI_ERROR;
        goto err_free_locality;
    }
    memset(localities, 0, total_locality_size);
    ret = ompi_coll_base_allgather_intra_bruck(locality, locality_size, MPI_CHAR,
                                               localities, one_locality_size, MPI_CHAR,
                                               MPI_COMM_WORLD, &module->super);
    if (ret != OMPI_SUCCESS) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("ompi_coll_base_allgather_intra_bruck failed");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to init topo map");
    }
    /* get node index */
    uint16_t* node_idx = mca_coll_ucx_obtain_node_index(comm);
    if (node_idx == NULL) {
        ret = OMPI_ERROR;
        goto err_free_localities;
    }

    /* create topology map */
    char **topo_map = mca_coll_ucx_create_topo_map(node_idx,
                                                   localities,
                                                   one_locality_size,
                                                   member_count);
    if (topo_map == NULL) {
        ret = OMPI_ERROR;
        goto err_free_node_idx;
    }

    /* save to global variable */
    mca_coll_ucx_component.topo_map = topo_map;
    mca_coll_ucx_component.world_member_count = member_count;
    ret = OMPI_SUCCESS;

err_free_node_idx:
    free(node_idx);
err_free_localities:
    free(localities);
err_free_locality:
    free(locality);

    return ret;
}

static char** mca_coll_ucx_obtain_topo_map(mca_coll_ucx_module_t *module,
                                           struct ompi_communicator_t *comm)
{
    if (mca_coll_ucx_component.topo_map == NULL) {
        /* global topo map is always needed. */
        if (OMPI_SUCCESS != mca_coll_ucx_create_global_topo_map(module, comm)) {
            return NULL;
        }
    }

    if (comm == MPI_COMM_WORLD) {
        return mca_coll_ucx_component.topo_map;
    }

    unsigned member_count = ompi_comm_size(comm);
    char **topo_map = mca_coll_ucx_allocate_topo_map(member_count);
    if (topo_map == NULL) {
        return NULL;
    }
    /* Create a topo matrix. As it is Diagonal symmetry， only half of the matrix will be computed. */
    for (unsigned i = 0; i < member_count; ++i) {
        /* Find the rank in the MPI_COMM_WORLD for rank i in the comm. */
        int i_global_rank = mca_coll_ucx_convert_to_global_rank(comm, i);
        if (i_global_rank == -1) {
            goto err_free_topo_map;
        }
        for (unsigned j = 0; j <= i; ++j) {
            int j_global_rank = mca_coll_ucx_convert_to_global_rank(comm, j);
            if (j_global_rank == -1) {
                goto err_free_topo_map;
            }
            topo_map[i][j] = mca_coll_ucx_component.topo_map[i_global_rank][j_global_rank];
            topo_map[j][i] = mca_coll_ucx_component.topo_map[j_global_rank][i_global_rank];
        }
    }

    mca_coll_ucx_print_topo_map(member_count, topo_map);

    return topo_map;

err_free_topo_map:
    mca_coll_ucx_deallocate_topo_map(topo_map, member_count);
    return NULL;
}

static void mca_coll_ucx_free_topo_map(char **topo_map, unsigned member_count)
{
    /* mca_coll_ucx_component.topo_map will be freed in mca_coll_ucx_module_destruct() */
    if (topo_map != mca_coll_ucx_component.topo_map) {
        mca_coll_ucx_deallocate_topo_map(topo_map, member_count);
    }

    return;
}

static void mca_coll_ucg_init_is_socket_balance(ucg_group_params_t *group_params, mca_coll_ucx_module_t *module,
                                                struct ompi_communicator_t *comm)
{
    unsigned pps = ucg_builtin_calculate_ppx(group_params, UCG_GROUP_MEMBER_DISTANCE_SOCKET);
    unsigned ppn = ucg_builtin_calculate_ppx(group_params, UCG_GROUP_MEMBER_DISTANCE_HOST);
    char is_socket_balance = (pps == (ppn - pps) || pps == ppn);
    char result = is_socket_balance;
    int status = ompi_coll_base_barrier_intra_basic_linear(comm, &module->super);
    if (status != OMPI_SUCCESS) {
        int error = MPI_ERR_INTERN;
        COLL_UCX_ERROR("ompi_coll_base_barrier_intra_basic_linear failed");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &error, "Failed to init is_socket_balance");
    }
    status = ompi_coll_base_allreduce_intra_basic_linear(&is_socket_balance, &result, 1, MPI_CHAR, MPI_MIN,
                                                         comm, &module->super);
    if (status != OMPI_SUCCESS) {
        int error = MPI_ERR_INTERN;
        COLL_UCX_ERROR("ompi_coll_base_allreduce_intra_basic_linear failed");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &error, "Failed to init is_socket_balance");
    }
    group_params->is_socket_balance = result;
    return;
}

static int mca_coll_ucx_init_ucg_group_params(mca_coll_ucx_module_t *module,
                                              struct ompi_communicator_t *comm,
                                              ucg_group_params_t *params)
{
    memset(params, 0, sizeof(*params));
    uint16_t binding_policy = OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy);
    params->field_mask = UCG_GROUP_PARAM_FIELD_UCP_WORKER |
                         UCG_GROUP_PARAM_FIELD_ID |
                         UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |
                         UCG_GROUP_PARAM_FIELD_DISTANCE |
                         UCG_GROUP_PARAM_FIELD_NODE_INDEX |
                         UCG_GROUP_PARAM_FIELD_BIND_TO_NONE |
                         UCG_GROUP_PARAM_FIELD_CB_GROUP_IBJ |
                         UCG_GROUP_PARAM_FIELD_IS_SOCKET_BALANCE;
    params->ucp_worker = mca_coll_ucx_component.ucp_worker;
    params->group_id = ompi_comm_get_cid(comm);
    params->member_count = ompi_comm_size(comm);
    params->distance = mca_coll_ucx_obtain_distance(comm);
    if (params->distance == NULL) {
        return OMPI_ERROR;
    }
    params->node_index = mca_coll_ucx_obtain_node_index(comm);
    if (params->node_index == NULL) {
        goto err_free_distane;
    }
    params->is_bind_to_none   = binding_policy == OPAL_BIND_TO_NONE;
    params->cb_group_obj      = comm;
    mca_coll_ucg_init_is_socket_balance(params, module, comm);
    if (mca_coll_ucx_component.enable_topo_map && binding_policy == OPAL_BIND_TO_CORE) {
        params->field_mask |= UCG_GROUP_PARAM_FIELD_TOPO_MAP;
        params->topo_map = mca_coll_ucx_obtain_topo_map(module, comm);
        if (params->topo_map == NULL) {
            goto err_node_idx;
        }
    }
    return OMPI_SUCCESS;
err_node_idx:
    free(params->node_index);
    params->node_index = NULL;
err_free_distane:
    free(params->distance);
    params->distance = NULL;
    return OMPI_ERROR;
}

static void mca_coll_ucx_cleanup_group_params(ucg_group_params_t *params)
{
    if (params->topo_map != NULL) {
        mca_coll_ucx_free_topo_map(params->topo_map, params->member_count);
        params->topo_map = NULL;
    }
    if (params->node_index != NULL) {
        free(params->node_index);
        params->node_index = NULL;
    }
    if (params->distance != NULL) {
        free(params->distance);
        params->distance = NULL;
    }
    return;
}

static int mca_coll_ucg_create(mca_coll_ucx_module_t *module, struct ompi_communicator_t *comm)
{
#if OMPI_GROUP_SPARSE
    COLL_UCX_ERROR("Sparse process groups are not supported");
    return UCS_ERR_UNSUPPORTED;
#endif
    ucg_group_params_t params;
    if (OMPI_SUCCESS != mca_coll_ucx_init_ucg_group_params(module, comm, &params)) {
        return OMPI_ERROR;
    }

    ucs_status_t status = ucg_group_create(mca_coll_ucx_component.ucg_context,
                                           &params,
                                           &module->ucg_group);
    if (status != UCS_OK) {
        COLL_UCX_ERROR("Failed to create ucg group, %s", ucs_status_string(status));
        goto err_cleanup_params;
    }

    ucs_list_add_tail(&mca_coll_ucx_component.group_head, &module->ucs_list);
    return OMPI_SUCCESS;

err_cleanup_params:
    mca_coll_ucx_cleanup_group_params(&params);
    return OMPI_ERROR;
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
}

static void mca_coll_ucx_module_destruct(mca_coll_ucx_module_t *module)
{
    if (module->ucg_group != NULL) {
        ucg_group_destroy(module->ucg_group);
    }
    ucs_list_del(&module->ucs_list);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucx_module_construct,
                   mca_coll_ucx_module_destruct);
