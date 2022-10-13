/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include "test_algo.h"


TEST(test_ucg_algo, kntree_leftmost) {
    /**
     * group_size = 10, degree = 4
     *          0
     *     /   / \ \ \
     *    4   8   1 2 3
     *   /|\  |
     *  5 6 7 9
     */
    const int group_size = 10;
    const int degree = 4;
    const int root = 0;
    ucg_rank_t peer;
    ucg_algo_kntree_iter_t iter[group_size];
    test_algo_kntree_data_t expect_data[group_size] = {
        {UCG_INVALID_RANK, {4, 8, 1, 2, 3, UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
        {0, {5, 6, 7, UCG_INVALID_RANK}},
        {4, {UCG_INVALID_RANK}},
        {4, {UCG_INVALID_RANK}},
        {4, {UCG_INVALID_RANK}},
        {0, {9, UCG_INVALID_RANK}},
        {8, {UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_init(&iter[i], group_size, degree, root, i, 1);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }

    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_reset(&iter[i]);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
}

TEST(test_ucg_algo, kntree_leftmost_non_zero_root) {
    /**
     * group_size = 10, degree = 4
     *          0
     *     /   / \ \ \
     *    5   9   2 3 4
     *   /|\  |
     *  6 7 8 0
     */
    const int group_size = 10;
    const int degree = 4;
    const int root = 1;
    ucg_rank_t peer;
    ucg_algo_kntree_iter_t iter[group_size];
    test_algo_kntree_data_t expect_data[group_size] = {
        {9, {UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, {5, 9, 2, 3, 4, UCG_INVALID_RANK}},
        {1, {UCG_INVALID_RANK}},
        {1, {UCG_INVALID_RANK}},
        {1, {UCG_INVALID_RANK}},
        {1, {6, 7, 8, UCG_INVALID_RANK}},
        {5, {UCG_INVALID_RANK}},
        {5, {UCG_INVALID_RANK}},
        {5, {UCG_INVALID_RANK}},
        {1, {0, UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_init(&iter[i], group_size, degree, root, i, 1);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_reset(&iter[i]);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
}

TEST(test_ucg_algo, kntree_rightmost) {
    /**
     * group_size = 10, degree = 3
     *        0
     *   / /  |  \ \
     *  2 1   6   3 9
     *        |\  |\
     *        8 7 5 4
     */
    const int group_size = 10;
    const int degree = 3;
    const int root = 0;
    ucg_rank_t peer;
    ucg_algo_kntree_iter_t iter[group_size];
    test_algo_kntree_data_t expect_data[group_size] = {
        {UCG_INVALID_RANK, {2, 1, 6, 3, 9, UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
        {0, {5, 4, UCG_INVALID_RANK}},
        {3, {UCG_INVALID_RANK}},
        {3, {UCG_INVALID_RANK}},
        {0, {8, 7, UCG_INVALID_RANK}},
        {6, {UCG_INVALID_RANK}},
        {6, {UCG_INVALID_RANK}},
        {0, {UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_init(&iter[i], group_size, degree, root, i, 0);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }

    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_reset(&iter[i]);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
}

TEST(test_ucg_algo, kntree_rightmost_non_zero_root) {
    /**
     * group_size = 10, degree = 3
     *        0
     *   / /  |  \ \
     *  3 2   7   4 0
     *        |\  |\
     *        9 8 6 5
     */
    const int group_size = 10;
    const int degree = 3;
    const int root = 1;
    ucg_rank_t peer;
    ucg_algo_kntree_iter_t iter[group_size];
    test_algo_kntree_data_t expect_data[group_size] = {
        {1, {UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, {3, 2, 7, 4, 0, UCG_INVALID_RANK}},
        {1, {UCG_INVALID_RANK}},
        {1, {UCG_INVALID_RANK}},
        {1, {6, 5, UCG_INVALID_RANK}},
        {4, {UCG_INVALID_RANK}},
        {4, {UCG_INVALID_RANK}},
        {1, {9, 8, UCG_INVALID_RANK}},
        {7, {UCG_INVALID_RANK}},
        {7, {UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_init(&iter[i], group_size, degree, root, i, 0);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_kntree_iter_reset(&iter[i]);

        peer = ucg_algo_kntree_iter_parent_value(&iter[i]);
        ASSERT_EQ(expect_data[i].up_peer, peer);

        ucg_rank_t *down_peer_ptr = expect_data[i].down_peer;
        while (1) {
            peer = ucg_algo_kntree_iter_child_value(&iter[i]);
            ASSERT_EQ(*down_peer_ptr, peer);
            ++down_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_kntree_iter_child_inc(&iter[i]);
        }
    }
}

Test(test_ucg_algo, rd_power_of_2) {
    /**
     * group_size = 4
     * 0     1     2     3
     * 0-----1     2-----3
     * 0-----------2
     *       1-----------3
     */
    const int group_size = 4;
    ucg_rank_t peer;
    ucg_algo_rd_iter_t iter[group_size];
    test_algo_rd_data_t expect_data[group_size] = {
        {UCG_ALGO_RD_ITER_BASE, {1, 2, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_BASE, {0, 3, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_BASE, {3, 0, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_BASE, {2, 1, UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rd_iter_init(&iter[i], group_size, i);
        ASSERT_EQ(expect_data[i].type, ucg_algo_rd_iter_type(&iter[i]));
        ucg_rank_t *peer_ptr = expect_data[i].peer;
        while (1) {
            peer = ucg_algo_rd_iter_value(&iter[i]);
            ASSERT_EQ(*peer_ptr, peer);
            ++peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_rd_iter_inc(&iter[i]);
        }
    }
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rd_iter_reset(&iter[i]);
        ASSERT_EQ(expect_data[i].type, ucg_algo_rd_iter_type(&iter[i]));
        ucg_rank_t *peer_ptr = expect_data[i].peer;
        while (1) {
            peer = ucg_algo_rd_iter_value(&iter[i]);
            ASSERT_EQ(*peer_ptr, peer);
            ++peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_rd_iter_inc(&iter[i]);
        }
    }
}

Test(test_ucg_algo, rd_non_power_of_2) {
    /**
     * group_size = 7
     * 0     1     2     3     4     5     6
     * 0-----1     
     *             2-----3
     *                         4-----5
     *       1-----------3
     *                               5-----6
     *       1-----------------------5
     *                   3-----------------6
     * 0<----1
     *             2<----3
     *                         4<----5
     */
    const int group_size = 7;
    ucg_rank_t peer;
    ucg_algo_rd_iter_t iter[group_size];
    test_algo_rd_data_t expect_data[group_size] = {
        {UCG_ALGO_RD_ITER_EXTRA, {1, 1, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_PROXY, {0, 3, 5, 0, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_EXTRA, {3, 3, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_PROXY, {2, 1, 6, 2, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_EXTRA, {5, 5, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_PROXY, {4, 6, 1, 4, UCG_INVALID_RANK}},
        {UCG_ALGO_RD_ITER_BASE,  {5, 3, UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rd_iter_init(&iter[i], group_size, i);
        ASSERT_EQ(expect_data[i].type, ucg_algo_rd_iter_type(&iter[i]));
        ucg_rank_t *peer_ptr = expect_data[i].peer;
        while (1) {
            peer = ucg_algo_rd_iter_value(&iter[i]);
            ASSERT_EQ(*peer_ptr, peer);
            ++peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_rd_iter_inc(&iter[i]);
        }
    }
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rd_iter_reset(&iter[i]);
        ASSERT_EQ(expect_data[i].type, ucg_algo_rd_iter_type(&iter[i]));
        ucg_rank_t *peer_ptr = expect_data[i].peer;
        while (1) {
            peer = ucg_algo_rd_iter_value(&iter[i]);
            ASSERT_EQ(*peer_ptr, peer);
            ++peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
            ucg_algo_rd_iter_inc(&iter[i]);
        }
    }
}

/**
 * @brief Test for recursive halving algorithm. Quadratic power
 */
Test(test_ucg_algo, rh_quadratic) {
    ucg_rank_t peer;
    ucg_algo_rh_iterator_t iter;
    /**
     * group_size = 4
     * 0     1     2     3
     * 0-----------2
     *       1-----------3
     * 0-----1     2-----3
     */
    const int group_size = 4;
    test_algo_rh_data_t expect_data[group_size] = {
        {UCG_INVALID_RANK, UCG_INVALID_RANK, {2, 1, UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, UCG_INVALID_RANK, {3, 0, UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, UCG_INVALID_RANK, {0, 3, UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, UCG_INVALID_RANK, {1, 2, UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rh_iter_init(&iter[i], group_size, i);

        ucg_algo_rh_get_extra(&iter, &peer);
        ASSERT_EQ(expect_data[i].extra_peer, peer);

        ucg_algo_rh_get_proxy(&iter, &peer);
        ASSERT_EQ(expect_data[i].proxy_peer, peer);

        ucg_rank_t *base_peer_ptr = expect_data[i].base_peer;
        while (1) {
            ucg_algo_rh_get_next_base(&iter, &peer);
            ASSERT_EQ(*base_peer_ptr, peer);
            ++base_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
        }
    }
}

/**
 * @brief Test for recursive halving algorithm. Non_quadratic power
 */
Test(test_ucg_algo, rh_non_quadratic) {
    ucg_rank_t peer;
    ucg_algo_rh_iterator_t iter;
    /**
     * group_size = 7
     * 0     1     2     3     4     5     6
     * 0<----------------------4
     *       1<----------------------5
     *             2<----------------------6
     * 0-----------2
     *       1-----------3
     * 0-----1     2-----3
     * 0---------------------->4
     *       1---------------------->5
     *             2---------------------->6
     */
    const int group_size = 7;
    test_algo_rh_data_t expect_data[group_size] = {
        {4, UCG_INVALID_RANK, {2, 1, UCG_INVALID_RANK}},
        {5, UCG_INVALID_RANK, {3, 0, UCG_INVALID_RANK}},
        {6, UCG_INVALID_RANK, {0, 3, UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, UCG_INVALID_RANK, {1, 2, UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, 0, {UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, 1, {UCG_INVALID_RANK}},
        {UCG_INVALID_RANK, 2, {UCG_INVALID_RANK}},
    };
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_algo_rh_iter_init(&iter[i], group_size, i);

        ucg_algo_rh_get_extra(&iter, &peer);
        ASSERT_EQ(expect_data[i].extra_peer, peer);

        ucg_algo_rh_get_proxy(&iter, &peer);
        ASSERT_EQ(expect_data[i].proxy_peer, peer);

        ucg_rank_t *base_peer_ptr = expect_data[i].base_peer;
        while (1) {
            ucg_algo_rh_get_next_base(&iter, &peer);
            ASSERT_EQ(*base_peer_ptr, peer);
            ++base_peer_ptr;
            if (peer == UCG_INVALID_RANK) {
                break;
            }
        }
    }
}

/**
 * @brief Test for ring algorithm
 */
Test(test_ucg_algo, ring) {
    ucg_algo_ring_iter_t iter;
    test_algo_ring_data_t expect_data[] = {
        {3, 1}, {0, 2}, {1, 3}, {2, 0}
    };
    int group_size = sizeof(expect_data) / sizeof(expect_data[0]);
    for (ucg_rank_t i = 0; i < group_size; ++i) {
        ucg_rank_t left_peer, right_peer;
        ucg_algo_ring_iter_init(&iter, group_size, i);
        while (1) {
            left_peer = ucg_algo_ring_iter_left_value(&iter);
            right_peer = ucg_algo_ring_iter_right_value(&iter);
            if (left_peer == UCG_INVALID_RANK) {
                ASSERT_EQ(right_peer, UCG_INVALID_RANK);
                break;
            }
            ASSERT_EQ(expect_data[i].left_peer, left_peer);
            ASSERT_EQ(expect_data[i].right_peer, right_peer);
            ucg_algo_ring_iter_inc(&iter);
        }
    }
}