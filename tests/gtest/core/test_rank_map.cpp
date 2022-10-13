/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "core/ucg_rank_map.h"
}

using namespace test;

class test_ucg_rank_map : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init();
    }

    static void TearDownTestSuite()
    {
        stub::cleanup();
    }
};

static ucg_rank_t test_mapping_cb(void *arg, ucg_rank_t rank)
{
    ucg_rank_t *ranks = (ucg_rank_t *)arg;
    return ranks[rank];
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_T(test_ucg_rank_map, init_by_array_invalid_args)
{
    ucg_rank_map_t map;
    const int size = 4;
    ucg_rank_t *ranks = (ucg_rank_t*)malloc(size * sizeof(ucg_rank_t));
    ASSERT_EQ(ucg_rank_map_init_by_array(NULL, &ranks, size, 0), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_rank_map_init_by_array(&map, NULL, size, 0), UCG_ERR_INVALID_PARAM);
    free(ranks);

    ucg_rank_t *invalid_ranks = NULL;
    ASSERT_EQ(ucg_rank_map_init_by_array(&map, &invalid_ranks, size, 0), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_rank_map, copy_invalid_args)
{
    ucg_rank_map_t dst;
    ucg_rank_map_t src;
    ASSERT_EQ(ucg_rank_map_copy(NULL, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_rank_map_copy(&dst, NULL), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_rank_map_copy(NULL, &src), UCG_ERR_INVALID_PARAM);
}

TEST_T(test_ucg_rank_map, cleanup_invalid_args)
{
    // expect no segmentation fault
    ucg_rank_map_cleanup(NULL);
}

TEST_T(test_ucg_rank_map, eval_invalid_args)
{
    ASSERT_EQ(ucg_rank_map_eval(NULL, 0), UCG_INVALID_RANK);

    ucg_rank_map_t array_map = {
        .type = UCG_RANK_MAP_TYPE_FULL,
        .size = 10,
    };
    // out of range
    EXPECT_EQ(ucg_rank_map_eval(&array_map, -1), UCG_INVALID_RANK);
    EXPECT_EQ(ucg_rank_map_eval(&array_map, 10), UCG_INVALID_RANK);
}
#endif

TEST_T(test_ucg_rank_map, init_by_array)
{
    ucg_rank_map_t map;

    int size = 6;
    ucg_rank_t *ranks;

    // rank array that cannot be optimized
    ranks = (ucg_rank_t*)malloc(size * sizeof(ucg_rank_t));
    for (int i = 0; i < size; ++i) {
        ranks[i] = i * i;
    }
    ASSERT_EQ(ucg_rank_map_init_by_array(&map, &ranks, size, 0), UCG_OK);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_ARRAY);
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(map.array[i], ranks[i]);
    }
    ucg_rank_map_cleanup(&map);

    ASSERT_EQ(ucg_rank_map_init_by_array(&map, &ranks, size, 1), UCG_OK);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_ARRAY);
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(map.array[i], i * i);
    }
    ASSERT_TRUE(ranks == NULL);
    ucg_rank_map_cleanup(&map);

    // rank array that cannot be optimized
    ranks = (ucg_rank_t*)malloc(size * sizeof(ucg_rank_t));
    for (int i = 0; i < size; ++i) {
        ranks[i] = i;
    }
    ASSERT_EQ(ucg_rank_map_init_by_array(&map, &ranks, size, 0), UCG_OK);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_FULL);
    ucg_rank_map_cleanup(&map);

    ASSERT_EQ(ucg_rank_map_init_by_array(&map, &ranks, size, 1), UCG_OK);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_FULL);
    ASSERT_TRUE(ranks == NULL);
    ucg_rank_map_cleanup(&map);
}

#ifdef UCG_ENABLE_DEBUG
TEST_T(test_ucg_rank_map, init_by_arrayfail_malloc)
{
    int size = 6;
    ucg_rank_t *ranks = (ucg_rank_t*)malloc(size * sizeof(ucg_rank_t));
    for (int i = 0; i < size; ++i) {
        ranks[i] = i * i;
    }
    ucg_rank_map_t map;
    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg rank-map array");
    ASSERT_NE(ucg_rank_map_init_by_array(&map, &ranks, size, 0), UCG_OK);
    free(ranks);
}
#endif

TEST_T(test_ucg_rank_map, optimize_full)
{
    ucg_rank_t *ranks;
    ucg_rank_map_t map;

    // Cannot optimize, remain as is
    map.type = UCG_RANK_MAP_TYPE_FULL;
    ucg_rank_map_optimize(&map, &ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_FULL);
}

TEST_T(test_ucg_rank_map, optimize_array)
{
    int size = 6;
    ucg_rank_t *ranks = (ucg_rank_t*)malloc(size * sizeof(ucg_rank_t));
    ucg_rank_t *original_ranks = NULL;

    ucg_rank_map_t map;
    map.size = size;

    // to full-type
    for (int i = 0; i < size; ++i) {
        ranks[i] = i;
    }
    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.array = ranks;
    ucg_rank_map_optimize(&map, &original_ranks);
    ASSERT_EQ(original_ranks, ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_FULL);
    ASSERT_EQ(map.size, size);

    // to stride-type
    int start = 1;
    int stride = 2;
    for (int i = 0; i < size; ++i) {
        ranks[i] = start + i * stride;
    }
    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.array = ranks;
    ucg_rank_map_optimize(&map, &original_ranks);
    ASSERT_EQ(original_ranks, ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_STRIDE);
    ASSERT_EQ(map.size, size);
    ASSERT_EQ(map.strided.start, start);
    ASSERT_EQ(map.strided.start, stride);

    // to stride-type
    start = 10;
    stride = -2;
    for (int i = 0; i < size; ++i) {
        ranks[i] = start + i * stride;
    }
    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.array = ranks;
    ucg_rank_map_optimize(&map, &original_ranks);
    ASSERT_EQ(original_ranks, ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_STRIDE);
    ASSERT_EQ(map.size, size);
    ASSERT_EQ(map.strided.start, start);
    ASSERT_EQ(map.strided.start, stride);

    // cannot optimize
    for (int i = 0; i < size; ++i) {
        ranks[i] = i * i;
    }
    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.array = ranks;
    ucg_rank_map_optimize(&map, &original_ranks);
    ASSERT_EQ(original_ranks, ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_ARRAY);
    ASSERT_EQ(map.size, size);
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(map.array[i], i * i);
    }

    free(ranks);
}

TEST_T(test_ucg_rank_map, optimize_stride)
{
    ucg_rank_t *ranks;
    ucg_rank_map_t map;

    // to full-type
    map.type = UCG_RANK_MAP_TYPE_STRIDE;
    map.strided.start = 0;
    map.strided.stride = 1;
    ucg_rank_map_optimize(&map, &ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_FULL);

    // start is not 0, cannot optimize
    map.type = UCG_RANK_MAP_TYPE_STRIDE;
    map.strided.start = 10;
    map.strided.stride = 1;
    ucg_rank_map_optimize(&map, &ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_STRIDE);

    // start is not 1, cannot optimize
    map.type = UCG_RANK_MAP_TYPE_STRIDE;
    map.strided.start = 0;
    map.strided.stride = 2;
    ucg_rank_map_optimize(&map, &ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_STRIDE);
}

TEST_T(test_ucg_rank_map, optimize_cb)
{
    ucg_rank_t *ranks;
    ucg_rank_map_t map;

    //Cannot optimize, remain as is
    map.type = UCG_RANK_MAP_TYPE_CB;
    ucg_rank_map_optimize(&map, &ranks);
    ASSERT_EQ(map.type, UCG_RANK_MAP_TYPE_CB);
}

TEST_T(test_ucg_rank_map, copy)
{
    ucg_rank_map_t dst;
    const int size = 5;
    ucg_rank_t ranks[size] = {1, 2, 3, 4, 5};
    ucg_rank_map_t src;
    src.size = size;

    src.type = UCG_RANK_MAP_TYPE_ARRAY;
    src.array = ranks;
    ASSERT_EQ(ucg_rank_map_eval(&dst, &src), UCG_OK);
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&dst, i), ucg_rank_map_eval(&src, i));
    }

    src.type = UCG_RANK_MAP_TYPE_FULL;
    ASSERT_EQ(ucg_rank_map_eval(&dst, &src), UCG_OK);
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&dst, i), ucg_rank_map_eval(&src, i));
    }
}

TEST_T(test_ucg_rank_map, eval)
{
    const int size = 5;
    ucg_rank_t ranks[size];
    int start = 1;
    int stride = 2;
    for (int i = 0; i < size; ++i) {
        ranks[i] = start + i * stride;
    }

    ucg_rank_map_t map;
    map.size = size;

    map.type = UCG_RANK_MAP_TYPE_FULL;
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&map, i), i);
    }

    map.type = UCG_RANK_MAP_TYPE_STRIDE;
    map.strided.start = start;
    map.strided.stride = stride;
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&map, i), ranks[i]);
    }

    map.type = UCG_RANK_MAP_TYPE_ARRAY;
    map.array = ranks
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&map, i), ranks[i]);
    }

    map.type = UCG_RANK_MAP_TYPE_CB;
    map.cb.mapping = test_mapping_cb;
    map.cb.arg = ranks;
    for (int i = 0; i < size; ++i) {
        ASSERT_EQ(ucg_rank_map_eval(&map, i), ranks[i]);
    }
}

TEST_T(test_ucg_rank_map, eval_unknow_type)
{
    ucg_rank_map_t map;
    map.type = (ucg_rank_map_type_t)123;
    map.size = 0;
    ASSERT_EQ(ucg_rank_map_eval(&map, 0), UCG_INVALID_RANK);
}