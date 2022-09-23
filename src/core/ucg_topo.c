/*
 *Copyright (C) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_topo.h"
#include "ucg_rank_map.h"
#include "ucg_group.h"

#include "util/ucg_malloc.h"
#include "util/ucg_hepler.h"
#include "util/ucg_array.h"
#include "util/ucg_log.h"
#include "util/ucg_math.h"

/* Array of ucg rank. */
UCG_ARRAYX_DECLARE(ucg_rank_t);
/* Array for storing IDs (e.g. node id) to filter leader. */
UCG_ARRAY_DECLARE(int32_t);

/* The rank_map is a vgroup rank to group rank mapping table. */
static int32_t ucg_topo_is_self(const ucg_topo_t *topo,
                                const ucg_rank_map_t *rank_map,
                                ucg_rank_t rank)
{
    ucg_rank_t group_rank = ucg_rank_map_eval(rank_map, rank);
    ucg_assert(group_rank != UCG_INVALID_RANK);

    return group_rank == topo->myrank;
}

/* The rank_map is a vgroup rank to group rank mapping table. */