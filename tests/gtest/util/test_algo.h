/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#ifndef TEST_ALGO_H_
#define TEST_ALGO_H_

#include <gtest/gtest.h>

extern "C" {
#include "util/algo/ucg_kntree.h"
#include "util/algo/ucg_rd.h"
#include "util/algo/ucg_rh.h"
#include "util/algo/ucg_ring.h"
}

#define MAX_PEER 100

typedef struct test_algo_kntree_data {
    ucg_rank_t up_peer;
    ucg_rank_t down_peer[MAX_PEER];
} test_algo_kntree_data_t;

typedef struct test_algo_rd_data {
    ucg_algo_rd_iter_type_t type;
    ucg_rank_t peer[MAX_PEER];
} test_algo_rd_data_t;

typedef struct test_algo_rh_data {
    ucg_rank_t extra_peer;
    ucg_rank_t proxy_peer;
    ucg_rank_t base_peer[MAX_PEER];
} test_algo_rh_data_t;

typedef struct test_algo_ring_data {
    ucg_rank_t left_peer;
    ucg_rank_t right_peer;
} test_algo_ring_data_t;

#endif