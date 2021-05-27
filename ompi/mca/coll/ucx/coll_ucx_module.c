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

static inline int mca_coll_ucx_get_world_rank(ompi_communicator_t *comm, int rank)
{
    ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);

    return ((ompi_process_name_t*)&proc->super.proc_name)->vpid;
}

static inline int mca_coll_ucx_get_node_nums(uint32_t *node_nums)
{
    int rc;
    opal_process_name_t wildcard_rank;

    wildcard_rank.jobid = ORTE_PROC_MY_NAME->jobid;
    wildcard_rank.vpid = ORTE_NAME_WILDCARD->vpid;

    /* get number of nodes in the job */
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_NUM_NODES,
                                   &wildcard_rank, &node_nums, OPAL_UINT32);

    return rc;
}

static inline int mca_coll_ucx_get_nodeid(ompi_communicator_t *comm, int rank, uint32_t *nodeid)
{
    int rc;
    ompi_proc_t *proc;

    proc = ompi_comm_peer_lookup(comm, rank);
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_NODEID,
                                   &(proc->super.proc_name), &nodeid, OPAL_UINT32);

    return rc;
}

static int mca_coll_ucx_fill_loc_detail(mca_coll_ucx_module_t *module, rank_location_t *locs, int size)
{
    int i, rc;
    char *val, *beg;
    uint8_t sockid, max_sockid, *sockids;

    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_LOCALITY_STRING,
                                   &(opal_proc_local_get()->proc_name), &val, OPAL_STRING);
    if (rc != OMPI_SUCCESS || val == NULL) {
        COLL_UCX_ERROR("fail to get locality string, error code:%d", rc);
        return OMPI_ERROR;
    }

    sockids = (uint8_t *)malloc(sizeof(uint8_t) * size);
    if (sockids == NULL) {
        free(val);
        COLL_UCX_ERROR("fail to alloc sockid array, rank_nums:%d", size);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    beg = strstr(val, "SK") + strlen("SK");
    sockid = (uint8_t)atoi(beg);
    rc = ompi_coll_base_allgather_intra_bruck(&sockid, 1, MPI_UINT8_T, sockids, 1, MPI_UINT8_T,
                                              MPI_COMM_WORLD, &module->super);
    if (rc != OMPI_SUCCESS) {
        free(val);
        free(sockids);
        COLL_UCX_ERROR("ompi_coll_base_allgather_intra_bruck fail");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &rc, "fail to gather sockids");
    }

    max_sockid = 0;
    for (i = 0; i < size; i++) {
        locs[i].sock_id = sockids[i];
        if (sockids[i] > max_sockid) {
            max_sockid = sockids[i];
        }
    }
    mca_coll_ucx_component.topo.sock_nums = max_sockid + 1;

    free(val);
    free(sockids);
    return OMPI_SUCCESS;
}

static inline coll_ucx_topo_level_t mca_coll_ucx_get_topo_level()
{
    if (mca_coll_ucx_component.topo_aware_level >= COLL_UCX_TOPO_LEVEL_SOCKET &&
        OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) >= OPAL_BIND_TO_SOCKET) {
        return COLL_UCX_TOPO_LEVEL_SOCKET;
    }

    return COLL_UCX_TOPO_LEVEL_NODE;
}

static inline int mca_coll_ucx_get_topo_child_nums(coll_ucx_topo_level_t level)
{
    if (level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        return mca_coll_ucx_component.topo.sock_nums;
    }

    return mca_coll_ucx_component.topo.node_nums;
}

static inline coll_ucx_topo_tree_t *mca_coll_ucx_get_topo_child(coll_ucx_topo_tree_t *root,
                                                                coll_ucx_topo_level_t level,
                                                                int rank)
{
    int nodeid, sockid;

    if (level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        sockid = mca_coll_ucx_component.topo.locs[rank].sock_id;
        return &root->inter.child[sockid];
    }

    nodeid = mca_coll_ucx_component.topo.locs[rank].node_id;
    return &root->inter.child[nodeid];
}

static int mca_coll_ucx_build_topo_tree(coll_ucx_topo_tree_t *root,
                                        coll_ucx_topo_level_t level)
{
    int i, rc, child_nums;
    coll_ucx_topo_tree_t *child;

    if (level >= mca_coll_ucx_component.topo.level) {
        root->leaf.rank_nums = 0;
        return OMPI_SUCCESS;
    }

    level++;

    root->inter.rank_nums = 0;
    root->inter.child = NULL;
    child_nums = mca_coll_ucx_get_topo_child_nums(level);
    child = (coll_ucx_topo_tree_t *)malloc(sizeof(*child) * child_nums);
    if (child == NULL) {
        COLL_UCX_ERROR("fail to alloc children, child_nums:%d, child_level:%d, component_level:%d",
                       child_nums, level, mca_coll_ucx_component.topo.level);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    root->inter.child_nums = child_nums;
    root->inter.child = child;
    for (i = 0; i < child_nums; i++) {
        rc = mca_coll_ucx_build_topo_tree(&child[i], level);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    return OMPI_SUCCESS;
}

static void mca_coll_ucx_destroy_topo_tree(coll_ucx_topo_tree_t *root,
                                           coll_ucx_topo_level_t level)
{
    int i, child_nums;
    coll_ucx_topo_tree_t *child;

    if (level >= mca_coll_ucx_component.topo.level) {
        return;
    }

    level++;

    child = root->inter.child;
    if (child == NULL) {
        return;
    }

    child_nums = root->inter.child_nums;
    for (i = 0; i < child_nums; i++) {
        mca_coll_ucx_destroy_topo_tree(&child[i], level);
    }

    free(child);
    root->inter.child = NULL;
}

static void mca_coll_ucx_update_topo_tree(coll_ucx_topo_tree_t *root,
                                          coll_ucx_topo_level_t level,
                                          int rank)
{
    int i, rc, child_nums;
    coll_ucx_topo_tree_t *child;

    if (level >= mca_coll_ucx_component.topo.level) {
        if (root->leaf.rank_nums == 0) {
            root->leaf.rank_min = rank;
        }
        root->leaf.rank_max = rank;
        root->leaf.rank_nums++;
        return;
    }

    root->inter.rank_nums++;
    level++;

    child = mca_coll_ucx_get_topo_child(root, level, rank);
    return mca_coll_ucx_update_topo_tree(child, level, rank);
}

static int mca_coll_ucx_init_global_topo(mca_coll_ucx_module_t *module)
{
    int i, rc, rank_nums;
    uint32_t node_nums, nodeid;
    rank_location_t *locs;
    coll_ucx_topo_tree_t *root;

    if (mca_coll_ucx_component.topo.locs != NULL) {
        return OMPI_SUCCESS;
    }

    rank_nums = ompi_comm_size(MPI_COMM_WORLD);
    locs = (rank_location_t *)malloc(sizeof(*locs) * rank_nums);
    if (locs == NULL) {
        COLL_UCX_ERROR("fail to alloc rank location array, rank_nums:%d", rank_nums);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    mca_coll_ucx_component.topo.locs = locs;
    mca_coll_ucx_component.topo.rank_nums = rank_nums;

    rc = mca_coll_ucx_get_node_nums(&node_nums);
    if (rc != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to get node_nums, error code:%d", rc);
        return rc;
    }

    mca_coll_ucx_component.topo.node_nums = node_nums;

    mca_coll_ucx_component.topo.level = mca_coll_ucx_get_topo_level();
    if (mca_coll_ucx_component.topo.level > COLL_UCX_TOPO_LEVEL_NODE) {
        rc = mca_coll_ucx_fill_loc_detail(module, locs, rank_nums);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    root = &mca_coll_ucx_component.topo.tree;
    rc = mca_coll_ucx_build_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    if (rc != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to init global topo tree");
        return rc;
    }

    for (i = 0; i < rank_nums; i++) {
        rc = mca_coll_ucx_get_nodeid(MPI_COMM_WORLD, i, &nodeid);
        if (rc != OMPI_SUCCESS) {
            COLL_UCX_ERROR("fail to get nodeid, error code:%d", rc);
            return rc;
        }
        locs[i].node_id = nodeid;
        mca_coll_ucx_update_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT, i);
    }

    return OMPI_SUCCESS;
}

void mca_coll_ucx_destroy_global_topo()
{
    rank_location_t *locs;
    coll_ucx_topo_tree_t *root;

    root = &mca_coll_ucx_component.topo.tree;
    mca_coll_ucx_destroy_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);

    locs = mca_coll_ucx_component.topo.locs;
    if (locs != NULL) {
        free(locs);
        mca_coll_ucx_component.topo.locs = NULL;
    }
}

static int mca_coll_ucx_init_comm_topo(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    int i, rc, rank_nums, global_rank;
    coll_ucx_topo_tree_t *root;

    if (comm == MPI_COMM_WORLD) {
        module->topo_tree = &mca_coll_ucx_component.topo.tree;
        return OMPI_SUCCESS;
    }

    root = (coll_ucx_topo_tree_t *)malloc(sizeof(*root));
    if (root == NULL) {
        COLL_UCX_ERROR("fail to alloc communicator topo tree root");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    module->topo_tree = root;
    rc = mca_coll_ucx_build_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    if (rc != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to init communicator topo tree");
        return rc;
    }

    rank_nums = ompi_comm_size(comm);
    for (i = 0; i < rank_nums; i++) {
        global_rank = mca_coll_ucx_get_world_rank(comm, i);
        mca_coll_ucx_update_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT, global_rank);
    }

    return OMPI_SUCCESS;
}

static void mca_coll_ucx_destroy_comm_topo(mca_coll_ucx_module_t *module)
{
    coll_ucx_topo_tree_t *root = module->topo_tree;

    if (root == NULL || root == &mca_coll_ucx_component.topo.tree) {
        return;
    }

    mca_coll_ucx_destroy_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    free(root);
    module->topo_tree = NULL;
}

static int mca_coll_ucx_init_topo_info(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    int rc;

    if (comm == MPI_COMM_WORLD) {
        rc = mca_coll_ucx_init_global_topo(module);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    return mca_coll_ucx_init_comm_topo(module, comm);
}

static void mca_coll_ucx_check_node_aware_tree(coll_ucx_topo_tree_t *root, ucg_topo_args_t *arg)
{
    int i;
    coll_ucx_topo_tree_t *node;

    arg->rank_continuous_in_node = 1;
    arg->rank_continuous_in_sock = 0;
    arg->rank_balance_in_node = 1;
    arg->rank_balance_in_sock = 0;

    for (i = 0; i < root->inter.child_nums; i++) {
        node = &root->inter.child[i];
        if (node->leaf.rank_nums == 0) {
            continue;
        }

        COLL_UCX_VERBOSE(1, "node%d:rank_nums=%d,min_rank=%d,max_rank=%d",
                         i, node->leaf.rank_nums, node->leaf.rank_min, node->leaf.rank_max);

        if (node->leaf.rank_max - node->leaf.rank_min + 1 != node->leaf.rank_nums) {
            arg->rank_continuous_in_node = 0;
            return;
        }
    }
}

static void mca_coll_ucx_check_sock_aware_tree(coll_ucx_topo_tree_t *root, ucg_topo_args_t *arg)
{
    int i, j, sock_nums, min, max, rank_nums1, rank_nums2;
    coll_ucx_topo_tree_t *node;
    coll_ucx_topo_tree_t *sock;

    arg->rank_continuous_in_node = 1;
    arg->rank_continuous_in_sock = 1;
    arg->rank_balance_in_node = 1;
    arg->rank_balance_in_sock = 1;

    for (i = 0; i < root->inter.child_nums; i++) {
        node = &root->inter.child[i];
        if (node->inter.rank_nums == 0) {
            continue;
        }
        sock_nums = 0;
        rank_nums1 = 0;
        rank_nums2 = 0;
        for (j = 0; j < node->inter.child_nums; j++) {
            sock = &node->inter.child[j];
            if (sock->leaf.rank_nums == 0) {
                continue;
            }
            if (sock_nums == 0) {
                min = sock->leaf.rank_min;
                max = sock->leaf.rank_max;
                rank_nums1 = sock->leaf.rank_nums;
            } else {
                min = sock->leaf.rank_min < min ? sock->leaf.rank_min : min;
                max = sock->leaf.rank_max > max ? sock->leaf.rank_max : max;
                rank_nums2 = sock->leaf.rank_nums;
            }
            sock_nums++;
            if (sock->leaf.rank_max - sock->leaf.rank_min + 1 != sock->leaf.rank_nums) {
                arg->rank_continuous_in_sock = 0;
            }
        }

        COLL_UCX_VERBOSE(1, "node%d:rank_nums=%d,min_rank=%d,max_rank=%d,sock_num=%d,sock1_nums=%d,sock2_nums=%d",
                         i, node->inter.rank_nums, min, max, sock_nums, rank_nums1, rank_nums2);

        if (max - min + 1 != node->inter.rank_nums) {
            arg->rank_continuous_in_node = 0;
        }
        if (sock_nums > 2 || (sock_nums == 2 && rank_nums1 != rank_nums2)) {
            arg->rank_balance_in_sock = 0;
        }
    }
}

static void mca_coll_ucx_print_ucg_topo_args(const ucg_topo_args_t *arg)
{
    COLL_UCX_VERBOSE(1, "ucg_topo_args:rank_continuous_in_node=%d", arg->rank_continuous_in_node);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:rank_continuous_in_sock=%d", arg->rank_continuous_in_sock);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:rank_balance_in_node=%d", arg->rank_balance_in_node);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:rank_balance_in_sock=%d", arg->rank_balance_in_sock);
}

static void mca_coll_ucx_set_ucg_topo_args(mca_coll_ucx_module_t *module, ucg_topo_args_t *arg)
{
    if (mca_coll_ucx_component.topo.level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        mca_coll_ucx_check_sock_aware_tree(module->topo_tree, arg);
    } else {
        mca_coll_ucx_check_node_aware_tree(module->topo_tree, arg);
    }

    mca_coll_ucx_print_ucg_topo_args(arg);
}

static void mca_coll_ucx_print_topo_tree(coll_ucx_topo_tree_t *root,
                                         coll_ucx_topo_level_t level)
{
    int i, child_nums;
    coll_ucx_topo_tree_t *child;

    if (level >= mca_coll_ucx_component.topo.level) {
        COLL_UCX_VERBOSE(1, "ranks info:nums=%d,min=%d,max=%d",
                         root->leaf.rank_nums,
                         root->leaf.rank_min,
                         root->leaf.rank_max);
        return;
    }

    level++;

    child = root->inter.child;
    if (child == NULL) {
        return;
    }

    child_nums = root->inter.child_nums;
    for (i = 0; i < child_nums; i++) {
        COLL_UCX_VERBOSE(1, "%s %d/%d:rank_nums=%d", level == COLL_UCX_TOPO_LEVEL_NODE ?
                         "node" : "socket", i, child_nums, child[i].inter.rank_nums);
        mca_coll_ucx_print_topo_tree(&child[i], level);
    }
}

static void mca_coll_ucx_print_global_topo()
{
    int i, j, rows, cols, len, rank_nums;
    char logbuf[512];
    char *buf = logbuf;
    rank_location_t *locs;
    coll_ucx_topo_tree_t *root;

    locs = mca_coll_ucx_component.topo.locs;
    if (locs == NULL) {
        return;
    }

    cols = 32;
    rank_nums = mca_coll_ucx_component.topo.rank_nums;
    rows = rank_nums / cols;
    for (i = 0; i < rows; i++) {
        for (j = 0; j < cols; j++) {
            len = sprintf(buf, "(%u,%u)", locs->node_id, locs->sock_id);
            locs++;
            buf += len;
        }
        *buf = '\0';
        buf = logbuf;
        COLL_UCX_VERBOSE(1, "rank %d~%d location:%s", i * cols, (i + 1) * cols - 1, buf);
    }

    for (j = rows * cols; j < rank_nums; j++) {
        len = sprintf(buf, "(%u,%u)", locs->node_id, locs->sock_id);
        locs++;
        buf += len;
    }
    *buf = '\0';
    buf = logbuf;
    COLL_UCX_VERBOSE(1, "rank %d~%d location:%s", rows * cols, rank_nums - 1, buf);
}

static void mca_coll_ucx_print_comm_topo(mca_coll_ucx_module_t *module)
{
    coll_ucx_topo_tree_t *root = module->topo_tree;

    if (root == NULL) {
        return;
    }

    mca_coll_ucx_print_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
}

static void mca_coll_ucx_print_topo_info(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    if (comm == MPI_COMM_WORLD) {
        mca_coll_ucx_print_global_topo();
    }

    return mca_coll_ucx_print_comm_topo(module);
}

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
    mca_coll_ucx_set_ucg_topo_args(module, &params->topo_args);
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

    if (OMPI_SUCCESS != mca_coll_ucx_init_topo_info(module, comm)) {
        COLL_UCX_ERROR("fail to init topo info");
        return OMPI_ERROR;
    }

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

    ucs_list_head_init(&module->ucs_list);
}

static void mca_coll_ucx_module_destruct(mca_coll_ucx_module_t *module)
{
    if (module->ucg_group != NULL) {
        ucg_group_destroy(module->ucg_group);
    }

    ucs_list_del(&module->ucs_list);

    mca_coll_ucx_destroy_comm_topo(module);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucx_module_construct,
                   mca_coll_ucx_module_destruct);
