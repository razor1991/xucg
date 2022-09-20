/*
 *Copyright (C) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ALGO_RECURSIVE_HALVING_H_
#define UCG_ALGO_RECURSIVE_HALVING_H_

#include "ucg/api/ucg.h"
#include "util/ucg_math.h"

/**
 * @brief Recursive havling algorithm phase
 */
typedef enum {
    UCG_RH_PHASE_PRE  = UCG_BIT(0),
    UCG_RH_PHASE_LOOP = UCG_BIT(1),
    UCG_RH_PHASE_POST = UCG_BIT(2),
} ucg_algo_rh_phase_t;

/**
 * @brief Recursive havling algorithm rank type
 */
typedef enum {
    UCG_ALGO_RH_ITER_BASE  = UCG_BIT(0), /* quadratic rank that performs the normal rd_algo */
    UCG_ALGO_RH_ITER_PROXY = UCG_BIT(1), /* "base rank" that interacts with "extra rank" */
    UCG_ALGO_RH_ITER_EXTRA = UCG_BIT(2), /* non-quadratic rank */
} ucg_algo_rd_rank_type_t;

/**
 * @brief Recursive havling algorithm iterator
 */
typedef struct ucg_algo_rh_iterator {
    /* <= maximum quadratic power of group_size */
    int adjust_group_size;
    ucg_rank_t my_rank;
    ucg_algo_rd_rank_type_t my_type;
    /* iteration step */
    int iteration;
    /* maximum iteration step */
    int max_iteration;
} ucg_algo_rh_iterator_t;

/**
 * @brief Init recursive havling algorithm iterator
 */
void ucg_algo_rh_iter_init(ucg_algo_rh_iterator_t *p, int group_size, ucg_rank_t my_rank);

/**
 * @brief Return the extra rank. Only for proxy rank
 */
void ucg_algo_rh_get_extra(ucg_algo_rh_iterator_t *p, ucg_rank_t *peer);

/**
 * @brief Return the proxy rank. Only for extra rank
 */
void ucg_algo_rh_get_proxy(ucg_algo_rh_iterator_t *p, ucg_rank_t *peer);

/**
 * @brief Iteratively return the base rank. Only for base rank
 */
void ucg_algo_rh_get_next_base(ucg_algo_rh_iterator_t *p, ucg_rank_t *peer);

#endif