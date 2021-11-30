/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021.  All rights reserved.
 * Description: Algorithm auto select
 * Author: shizhibao
 * Create: 2021-07-16
 */

#include <ucs/debug/log.h>
#include <ucp/dt/dt.h>

#include "src/ucg/builtin/ops/builtin_ops.h"
#include "builtin_algo_decision.h"
#include "builtin_plan.h"

typedef enum {
    SIZE_LEVEL_4B, /* 0-4 byte */
    SIZE_LEVEL_8B, /* 5-8 byte */
    SIZE_LEVEL_16B, /* 9-16 byte */
    SIZE_LEVEL_32B, /* 17-32 byte */
    SIZE_LEVEL_64B, /* 33-64 byte */
    SIZE_LEVEL_128B, /* 65-128 byte */
    SIZE_LEVEL_256B, /* 129-256 byte */
    SIZE_LEVEL_512B, /* 257-512 byte */
    SIZE_LEVEL_1KB, /* 513-1024 byte */
    SIZE_LEVEL_2KB, /* 1025-2048 byte */
    SIZE_LEVEL_4KB, /* 2049-4096 byte */
    SIZE_LEVEL_8KB, /* 4097-8192 byte */
    SIZE_LEVEL_1MB, /* 8193-1048576 byte */
    SIZE_LEVEL_LG, /* > 1048576 byte */
    /* The new size level must be added above */
    SIZE_LEVEL_NUMS
} size_level_t;

typedef enum {
    PPN_LEVEL_4, /* 1-4 */
    PPN_LEVEL_8, /* 5-8 */
    PPN_LEVEL_16, /* 9-16 */
    PPN_LEVEL_32, /* 17-32 */
    PPN_LEVEL_64, /* 33-64 */
    PPN_LEVEL_LG, /* >64 */
    /* The new ppn level must be added above */
    PPN_LEVEL_NUMS
} ppn_level_t;

typedef enum {
    NODE_LEVEL_4, /* 1-4 */
    NODE_LEVEL_8, /* 5-8 */
    NODE_LEVEL_16, /* 9-16 */
    NODE_LEVEL_32, /* 17-32 */
    NODE_LEVEL_LG, /* >32 */
    /* The new node level must be added above */
    NODE_LEVEL_NUMS
} node_level_t;

const static int barrier_algo_tbl[PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    /* NODE_LEVEL_4, 8, 16, 32, LG */
    {10, 7, 2, 6, 4}, /* PPN_LEVEL_4 */
    {10, 10, 2, 6, 7}, /* PPN_LEVEL_8 */
    {10, 10, 10, 7, 7}, /* PPN_LEVEL_16 */
    {10, 10, 10, 10, 6}, /* PPN_LEVEL_32 */
    {10, 10, 10, 10, 6}, /* PPN_LEVEL_64 */
    {10, 10, 10, 10, 5}, /* PPN_LEVEL_LG */
};

const static int bcast_algo_tbl[SIZE_LEVEL_NUMS][PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    {
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 4, 3, 3, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_8B*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_16B*/
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_4 */
        {3, 4, 3, 3, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_LG */
    }, {/* SIZE_LEVEL_32B*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 4, 3, 3, 3}, /* PPN_LEVEL_8 */
        {3, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_64B*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_128B*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {3, 4, 3, 3, 3}, /* PPN_LEVEL_8 */
        {3, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_LG */
    }, {/* SIZE_LEVEL_256B*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_8 */
        {3, 4, 4, 4, 3}, /* PPN_LEVEL_16 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_512B*/
        {4, 3, 4, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_8 */
        {3, 3, 4, 4, 4}, /* PPN_LEVEL_16 */
        {4, 3, 4, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_1KB*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 3, 3, 4, 3}, /* PPN_LEVEL_32 */
        {3, 3, 4, 4, 3}, /* PPN_LEVEL_64 */
        {3, 3, 4, 3, 3}, /* PPN_LEVEL_LG */
    }, {/* SIZE_LEVEL_2KB*/
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 4, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 4}, /* PPN_LEVEL_16 */
        {4, 3, 4, 4, 4}, /* PPN_LEVEL_32 */
        {3, 4, 3, 4, 3}, /* PPN_LEVEL_64 */
        {3, 4, 4, 4, 3}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_4KB*/
        {3, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 4, 4, 4, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 3, 3}, /* PPN_LEVEL_16 */
        {3, 4, 4, 4, 3}, /* PPN_LEVEL_32 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_64 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_8K*/
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 4, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_16 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_32 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_64 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_1M*/
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 4, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_16 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_32 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_64 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_LG */
    }, {/* SIZE_LEVEL_LG*/
        {4, 3, 3, 3, 3}, /* PPN_LEVEL_4 */
        {4, 3, 3, 4, 3}, /* PPN_LEVEL_8 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_16 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_32 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_64 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_LG */
    }
};

const static int allreduce_algo_tbl[SIZE_LEVEL_NUMS][PPN_LEVEL_NUMS][NODE_LEVEL_NUMS] = {
    {
        {11, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 7}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_8B*/
        {11, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 7}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_16B*/
        {11, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 7}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_32B*/
        {11, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 7}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_64B*/
        {11, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 7}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 11}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_128B*/
        {14, 8, 8, 8, 7}, /* PPN_LEVEL_4 */
        {11, 11, 8, 8, 8}, /* PPN_LEVEL_8 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 8}, /* PPN_LEVEL_32 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_256B*/
        {14, 13, 13, 13, 13}, /* PPN_LEVEL_4 */
        {11, 11, 13, 13, 13}, /* PPN_LEVEL_8 */
        {11, 11, 11, 14, 14}, /* PPN_LEVEL_16 */
        {11, 11, 11, 11, 7}, /* PPN_LEVEL_32 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_64 */
        {11, 11, 11, 7, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_512B*/
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_4 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_8 */
        {14, 14, 11, 13, 13}, /* PPN_LEVEL_16 */
        {14, 11, 14, 14, 14}, /* PPN_LEVEL_32 */
        {11, 11, 11, 8, 7}, /* PPN_LEVEL_64 */
        {11, 11, 7, 8, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_1KB*/
        {13, 8, 8, 8, 8}, /* PPN_LEVEL_4 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_8 */
        {14, 14, 14, 14, 14}, /* PPN_LEVEL_16 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_32 */
        {14, 14, 14, 14, 7}, /* PPN_LEVEL_64 */
        {11, 11, 7, 8, 8}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_2KB*/
        {13, 13, 13, 13, 8}, /* PPN_LEVEL_4 */
        {13, 13, 12, 12, 12}, /* PPN_LEVEL_8 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_16 */
        {14, 14, 14, 14, 14}, /* PPN_LEVEL_32 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_64 */
        {14, 11, 7, 7, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_4KB*/
        {13, 13, 13, 13, 12}, /* PPN_LEVEL_4 */
        {13, 13, 13, 12, 12}, /* PPN_LEVEL_8 */
        {12, 12, 12, 12, 12}, /* PPN_LEVEL_16 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_32 */
        {14, 14, 13, 14, 14}, /* PPN_LEVEL_64 */
        {13, 13, 13, 13, 7}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_8K*/
        {13, 13, 13, 13, 12}, /* PPN_LEVEL_4 */
        {13, 13, 13, 12, 12}, /* PPN_LEVEL_8 */
        {13, 12, 12, 12, 12}, /* PPN_LEVEL_16 */
        {12, 12, 12, 12, 12}, /* PPN_LEVEL_32 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_64 */
        {12, 12, 5, 5, 6}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_1M*/
        {13, 13, 13, 13, 12}, /* PPN_LEVEL_4 */
        {13, 13, 13, 12, 12}, /* PPN_LEVEL_8 */
        {13, 12, 12, 12, 12}, /* PPN_LEVEL_16 */
        {12, 12, 12, 12, 12}, /* PPN_LEVEL_32 */
        {13, 13, 13, 13, 13}, /* PPN_LEVEL_64 */
        {12, 12, 13, 13, 13}, /* PPN_LEVEL_LG */
    }, { /* SIZE_LEVEL_LG*/
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_4 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_8 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_16 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_32 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_64 */
        {4, 4, 4, 4, 4}, /* PPN_LEVEL_LG */
    }
};

static inline int log2_n(unsigned int n, unsigned int begin)
{
    int index = 0;
    
    while (n >= begin) {
        n = n >> 1;
        index++;
    }
    
    return index;
}

static size_level_t ucg_builtin_get_size_level(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    const int size_lev_small = 4;
    const int size_lev_mid = 8192;
    const int size_lev_large = 1048576;
    int size;
    int dt_len;
    ucp_datatype_t ucp_datatype;
    
    group_params->mpi_dt_convert(coll_params->send.dt_ext, &ucp_datatype);
    dt_len = UCP_DT_IS_CONTIG(ucp_datatype) ? coll_params->send.dt_len :
             ucg_builtin_get_dt_len(ucp_dt_to_generic(ucp_datatype));
    size = dt_len * coll_params->send.count;
    ucs_info("The SIZE parameter of auto select algorithm is %d", size);
    
    if (size <= size_lev_small) {
        return SIZE_LEVEL_4B;
    }
    if (size > size_lev_mid && size <= size_lev_large) {
        return SIZE_LEVEL_1MB;
    }
    if (size > size_lev_large) {
        return SIZE_LEVEL_LG;
    }
    
    size--;
    return (size_level_t)log2_n(size, size_lev_small);
}

static ppn_level_t ucg_builtin_get_ppn_level(const ucg_group_params_t *group_params)
{
    const int ppn_lev_small = 4;
    const int ppn_lev_large = 64;
    int ppn_max;
    
    ppn_max = group_params->topo_args.ppn_max;
    ucs_info("The PPN parameter of auto select algorithm is %d", ppn_max);
    if (ppn_max <= ppn_lev_small) {
        return PPN_LEVEL_4;
    }
    if (ppn_max > ppn_lev_large) {
        return PPN_LEVEL_LG;
    }
    
    ppn_max--;
    return (ppn_level_t)log2_n(ppn_max, ppn_lev_small);
}

static node_level_t ucg_builtin_get_node_level(const ucg_group_params_t *group_params)
{
    const int node_lev_small = 4;
    const int node_lev_large = 32;
    int node_nums;
    
    node_nums = group_params->topo_args.node_nums;
    ucs_info("The NODE parameter of auto select algorithm is %d", node_nums);
    if (node_nums <= node_lev_small) {
        return NODE_LEVEL_4;
    }
    if (node_nums > node_lev_large) {
        return NODE_LEVEL_LG;
    }
    
    node_nums--;
    return (node_level_t)log2_n(node_nums, node_lev_small);
}

static int ucg_builtin_barrier_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    
    return barrier_algo_tbl[ppn_lev][node_lev];
}

static int ucg_builtin_bcast_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    size_level_t size_lev;
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    size_lev = ucg_builtin_get_size_level(group_params, coll_params);
    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    
    return bcast_algo_tbl[size_lev][ppn_lev][node_lev];
}

static int ucg_builtin_allreduce_algo_select(const ucg_group_params_t *group_params,
                                                const ucg_collective_params_t *coll_params)
{
    size_level_t size_lev;
    ppn_level_t ppn_lev;
    node_level_t node_lev;

    size_lev = ucg_builtin_get_size_level(group_params, coll_params);
    ppn_lev = ucg_builtin_get_ppn_level(group_params);
    node_lev = ucg_builtin_get_node_level(group_params);
    
    return allreduce_algo_tbl[size_lev][ppn_lev][node_lev];
}

static int ucg_builtin_alltoallv_algo_select(const ucg_group_params_t *group_params,
                                             const ucg_collective_params_t *coll_params)
{
    return UCG_ALGORITHM_ALLTOALLV_NODE_AWARE_PLUMMER;
}

typedef int (*algo_select_f)(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params);

static algo_select_f algo_select[COLL_TYPE_NUMS] = {
    ucg_builtin_barrier_algo_select, /* COLL_TYPE_BARRIER */
    ucg_builtin_bcast_algo_select, /* COLL_TYPE_BCAST */
    ucg_builtin_allreduce_algo_select, /* COLL_TYPE_ALLREDUCE */
    ucg_builtin_alltoallv_algo_select, /* COLL_TYPE_ALLTOALLV */
};

int ucg_builtin_algo_auto_select(const ucg_group_params_t *group_params,
                                const ucg_collective_params_t *coll_params)
{
    return algo_select[coll_params->coll_type](group_params, coll_params);
}
