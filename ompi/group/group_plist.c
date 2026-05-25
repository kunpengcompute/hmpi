/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/class/opal_bitmap.h"
#include "ompi/group/group.h"
#include "ompi/constants.h"
#include "ompi/proc/proc.h"
#include "ompi/runtime/params.h"
#include "mpi.h"

#include <math.h>

/* only the vpid and jobid of the proc name are required. ompi_proc_lookup is deleted when proc is sentinel proc*/
static inline opal_process_name_t ompi_group_get_proc_name_for_group_union (ompi_group_t *group, int rank)
{

    ompi_proc_t *proc = NULL;
#if OMPI_GROUP_SPARSE
    do {
        if (OMPI_GROUP_IS_DENSE(group)) {
            proc = group->grp_proc_pointers[rank];
            break;
        }
        int ranks1 = rank;
        ompi_group_translate_ranks (group, 1, &ranks1, group->grp_parent_group_ptr, &rank);
        group = group->grp_parent_group_ptr;
    } while (1);
#else
    proc = group->grp_proc_pointers[rank];
#endif
    if (ompi_proc_is_sentinel (proc)) {
        return ompi_proc_sentinel_to_name ((intptr_t) proc);
    }

    return proc->super.proc_name;

}
/* Optimized version with O(m+n) time complexity using hash table */
static int ompi_group_dense_overlap_opt (ompi_group_t *group1, ompi_group_t *group2, opal_bitmap_t *bitmap)
{
    proc_hash_entry_t **hash_table;
    ompi_process_name_t proc_name;
    unsigned int hash;
    int rc, overlap_count;
    int i, j;

    overlap_count = 0;

    /* Allocate and initialize hash table */
    hash_table = (proc_hash_entry_t **)calloc(ompi_group_union_opt_hash_size, sizeof(proc_hash_entry_t *));
    if (NULL == hash_table) {
#if OPAL_ENABLE_DEBUG
        opal_output(0, "hash_table alloc memory fail");
#endif
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* Build hash table from group2 - O(n) */
    for (j = 0; j < group2->grp_proc_count; ++j) {
        proc_name = ompi_group_get_proc_name_for_group_union(group2, j);
        hash = proc_hash_func(proc_name);

        proc_hash_entry_t *entry = (proc_hash_entry_t *)malloc(sizeof(proc_hash_entry_t));
        if (NULL == entry) {
            /* Clean up hash table on allocation failure */
            for (i = 0; i < ompi_group_union_opt_hash_size; i++) {
                proc_hash_entry_t *next;
                proc_hash_entry_t *current = hash_table[i];
                while (current != NULL) {
                    next = current->next;
                    free(current);
                    current = next;
                }
            }
            free(hash_table);
#if OPAL_ENABLE_DEBUG
            opal_output(0, "entry alloc memory fail");
#endif
            return OPAL_ERR_OUT_OF_RESOURCE;
        }

        entry->proc_name = proc_name;
        entry->proc_index = j;
        entry->next = hash_table[hash];
        hash_table[hash] = entry;
    }

    /* Lookup group1 processes in hash table - O(m) */
    for (i = 0; i < group1->grp_proc_count; ++i) {
        proc_name = ompi_group_get_proc_name_for_group_union(group1, i);
        hash = proc_hash_func(proc_name);

        proc_hash_entry_t *entry = hash_table[hash];
        while (entry != NULL) {
            if (0 == opal_compare_proc(proc_name, entry->proc_name)) {
                rc = opal_bitmap_set_bit(bitmap, entry->proc_index);
                if (OPAL_SUCCESS != rc) {
                    /* Clean up hash table on error */
                    for (j = 0; j < ompi_group_union_opt_hash_size; j++) {
                        proc_hash_entry_t *next;
                        proc_hash_entry_t *current = hash_table[j];
                        while (current != NULL) {
                            next = current->next;
                            free(current);
                            current = next;
                        }
                    }
                    free(hash_table);
#if OPAL_ENABLE_DEBUG
                    opal_output(0, "opal_bitmap_set_bit error is %d", rc);
#endif
                    return rc;
                }
                ++overlap_count;
                break;
            }
            entry = entry->next;
        }
    }

    /* Clean up hash table */
    for (i = 0; i < ompi_group_union_opt_hash_size; i++) {
        proc_hash_entry_t *next;
        proc_hash_entry_t *current = hash_table[i];
        while (current != NULL) {
            next = current->next;
            free(current);
            current = next;
        }
    }
    free(hash_table);

    return overlap_count;
}

static int ompi_group_dense_overlap (ompi_group_t *group1, ompi_group_t *group2, opal_bitmap_t *bitmap)
{
    ompi_process_name_t proc1_name, proc2_name;
    int rc, overlap_count;

    overlap_count = 0;

    for (int proc1 = 0 ; proc1 < group1->grp_proc_count ; ++proc1) {
        proc1_name = ompi_group_get_proc_name(group1, proc1);

        /* check to see if this proc is in group2 */
        for (int proc2 = 0 ; proc2 < group2->grp_proc_count ; ++proc2) {
            proc2_name = ompi_group_get_proc_name(group2, proc2);
            if(0 == opal_compare_proc(proc1_name, proc2_name)) {
                rc = opal_bitmap_set_bit (bitmap, proc2);
                if (OPAL_SUCCESS != rc) {
                    return rc;
                }
                ++overlap_count;

                break;
            }
        }  /* end proc1 loop */
    }  /* end proc loop */

    return overlap_count;
}

static struct ompi_proc_t *ompi_group_dense_lookup_raw (ompi_group_t *group, const int peer_id)
{
    if (OPAL_UNLIKELY(ompi_proc_is_sentinel (group->grp_proc_pointers[peer_id]))) {
        ompi_proc_t *proc =
            (ompi_proc_t *) ompi_proc_lookup (ompi_proc_sentinel_to_name ((uintptr_t) group->grp_proc_pointers[peer_id]));
        if (NULL != proc) {
            /* replace sentinel value with an actual ompi_proc_t */
            group->grp_proc_pointers[peer_id] = proc;
            /* retain the proc */
            OBJ_RETAIN(group->grp_proc_pointers[peer_id]);
        }
    }

    return group->grp_proc_pointers[peer_id];
}

ompi_proc_t *ompi_group_get_proc_ptr_raw (ompi_group_t *group, int rank)
{
#if OMPI_GROUP_SPARSE
    do {
        if (OMPI_GROUP_IS_DENSE(group)) {
            return ompi_group_dense_lookup_raw (group, rank);
        }
        int ranks1 = rank;
        ompi_group_translate_ranks (group, 1, &ranks1, group->grp_parent_group_ptr, &rank);
        group = group->grp_parent_group_ptr;
    } while (1);
#else
    return ompi_group_dense_lookup_raw (group, rank);
#endif
}

int ompi_group_calc_plist ( int n , const int *ranks ) {
    return sizeof(char *) * n ;
}

int ompi_group_incl_plist(ompi_group_t* group, int n, const int *ranks,
                          ompi_group_t **new_group)
{
    /* local variables */
    int my_group_rank;
    ompi_group_t *group_pointer, *new_group_pointer;

    group_pointer = (ompi_group_t *)group;

    if ( 0 == n ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        return OMPI_SUCCESS;
    }

    /* get new group struct */
    new_group_pointer=ompi_group_allocate(n);
    if( NULL == new_group_pointer ) {
        return MPI_ERR_GROUP;
    }

    /* put group elements in the list */
    for (int proc = 0; proc < n; proc++) {
        new_group_pointer->grp_proc_pointers[proc] =
            ompi_group_get_proc_ptr_raw (group_pointer, ranks[proc]);
    }                           /* end proc loop */

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    my_group_rank=group_pointer->grp_my_rank;
    if (MPI_UNDEFINED != my_group_rank) {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    } else {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    }

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}

/*
 * Group Union has to use the dense format since we don't support
 * two parent groups in the group structure and maintain functions
 */
int ompi_group_union (ompi_group_t* group1, ompi_group_t* group2,
                      ompi_group_t **new_group)
{
    /* local variables */
    int new_group_size, cnt, rc, overlap_count;
    ompi_group_t *new_group_pointer;
    ompi_proc_t *proc2_pointer;
    opal_bitmap_t bitmap;

    /*
     * form union
     */

    /* get new group size */
    OBJ_CONSTRUCT(&bitmap, opal_bitmap_t);
    rc = opal_bitmap_init (&bitmap, 32);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }
    if (ompi_use_group_union_opt) {
        overlap_count = ompi_group_dense_overlap_opt (group1, group2, &bitmap);
    }
    else {
        overlap_count = ompi_group_dense_overlap (group1, group2, &bitmap);
    }
   
    if (0 > overlap_count) {
        OBJ_DESTRUCT(&bitmap);
        return overlap_count;
    }

    new_group_size = group1->grp_proc_count + group2->grp_proc_count - overlap_count;
    if ( 0 == new_group_size ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        OBJ_DESTRUCT(&bitmap);
        return MPI_SUCCESS;
    }

    /* get new group struct */
    new_group_pointer = ompi_group_allocate(new_group_size);
    if (NULL == new_group_pointer) {
        OBJ_DESTRUCT(&bitmap);
        return MPI_ERR_GROUP;
    }

    /* fill in the new group list */

    /* put group1 elements in the list */
    for (int proc1 = 0; proc1 < group1->grp_proc_count; ++proc1) {
        new_group_pointer->grp_proc_pointers[proc1] =
            ompi_group_get_proc_ptr_raw (group1, proc1);
    }
    cnt = group1->grp_proc_count;

    /* check group2 elements to see if they need to be included in the list */
    for (int proc2 = 0; proc2 < group2->grp_proc_count; ++proc2) {
        if (opal_bitmap_is_set_bit (&bitmap, proc2)) {
            continue;
        }

        proc2_pointer = ompi_group_get_proc_ptr_raw (group2, proc2);
        new_group_pointer->grp_proc_pointers[cnt++] = proc2_pointer;
    }                           /* end proc loop */

    OBJ_DESTRUCT(&bitmap);

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    if (MPI_UNDEFINED != group1->grp_my_rank || MPI_UNDEFINED != group2->grp_my_rank) {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    } else {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    }

    *new_group = (MPI_Group) new_group_pointer;

    return OMPI_SUCCESS;
}

/*
 * Group Difference has to use the dense format since we don't support
 * two parent groups in the group structure and maintain functions
 */
int ompi_group_difference(ompi_group_t* group1, ompi_group_t* group2,
                          ompi_group_t **new_group) {

    /* local varibles */
    int new_group_size, overlap_count, rc;
    ompi_group_t *new_group_pointer;
    ompi_proc_t *proc1_pointer;
    opal_bitmap_t bitmap;

    /*
     * form union
     */

    /* get new group size */
    OBJ_CONSTRUCT(&bitmap, opal_bitmap_t);
    rc = opal_bitmap_init (&bitmap, 32);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    /* check group2 elements to see if they need to be included in the list */
    overlap_count = ompi_group_dense_overlap (group2, group1, &bitmap);
    if (0 > overlap_count) {
        OBJ_DESTRUCT(&bitmap);
        return overlap_count;
    }

    new_group_size = group1->grp_proc_count - overlap_count;
    if ( 0 == new_group_size ) {
        *new_group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        OBJ_DESTRUCT(&bitmap);
        return MPI_SUCCESS;
    }

    /* allocate a new ompi_group_t structure */
    new_group_pointer = ompi_group_allocate(new_group_size);
    if( NULL == new_group_pointer ) {
        OBJ_DESTRUCT(&bitmap);
        return MPI_ERR_GROUP;
    }

    /* fill in group list */
    /* loop over group1 members */
    for (int proc1 = 0, cnt = 0 ; proc1 < group1->grp_proc_count ; ++proc1) {
        if (opal_bitmap_is_set_bit (&bitmap, proc1)) {
            continue;
        }

        proc1_pointer = ompi_group_get_proc_ptr_raw (group1, proc1);
        new_group_pointer->grp_proc_pointers[cnt++] = proc1_pointer;
    }  /* end proc loop */

    OBJ_DESTRUCT(&bitmap);

    /* increment proc reference counters */
    ompi_group_increment_proc_count(new_group_pointer);

    /* find my rank */
    if (MPI_UNDEFINED == group1->grp_my_rank || MPI_UNDEFINED != group2->grp_my_rank) {
        new_group_pointer->grp_my_rank = MPI_UNDEFINED;
    } else {
        ompi_set_group_rank(new_group_pointer, ompi_proc_local_proc);
    }

    *new_group = (MPI_Group)new_group_pointer;

    return OMPI_SUCCESS;
}
