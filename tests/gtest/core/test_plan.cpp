/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include "test_plan.h"

extern "C" {
#include "core/ucg_vgroup.h"
}

using namespace std;

#define OP_PTR(_val)  (ucg_plan_op_t *)(_val)
#define VGRP_PTR(_val) (ucg_vgroup_t *)(_val)

static ucg_dt_t dt = {UCG_DT_TYPE_INT8};
static ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
static ucg_mem_type_t mem_type = UCG_MEM_TYPE_HOST;

/**
 * For fallback
 */
static ucg_status_t prepare_unsupported(ucg_vgroup_t *vgroup,
                                        const ucg_coll_args_t *args,
                                        ucg_plan_op_t **op)
{
    return UCG_ERR_UNSUPPORTED;
}

static ucg_status_t prepare_ok(ucg_vgroup_t *vgroup,
                               const ucg_coll_args_t *args,
                               ucg_plan_op_t **op)
{
    /**
     * Just easy for test, we can use different value of group to
     * check ucg_plan_prepare() function
     */
    *op = (ucg_plan_op_t *)vgroup;
    return UCG_OK;
}

static void dump_plan(ucg_list_link_t *list)
{
#ifdef TEST_UCG_PLAN_ENABLE_DUMP_PLAN
    int len = ucg_list_length(list);
    ucg_plan_t *plan;
    ucg_plan_t *fallback;

    printf("There are %d plan\n", len);
    ucg_list_for_each(plan, list, list) {
        printf("score: %u range:[%lu %lu)\n", plan->score, plan->range.start, plan->range.end);
        ucg_list_for_each(fallback, &plan->fallback, fallback) {
            printf("\tfb score: %u range:[%lu %lu)\n", fallback->score, fallback->range.start,
                fallback->range.end);
        }
        printf("\n"); 
    }
#endif
}

static void check_expect_data(ucg_list_link_t *list, std::vertor<test_expect_plan_data> &expect)
{
    ucg_plan_t *plan = nullptr;
    ASSERT_NE(list, nullptr);

    dump_plan(list);
    int i = 0;
    ASSERT_EQ(ucg_list_length(list), expect.size());
    ucg_list_for_each(plan, list, list) {
        expect[i].check_plan(plan);
        ++i;
    }
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST(test_ucg_plan, invalid_args)
{
    ucg_plans_t *plans = nullptr;
    ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);

    /* nullptr args */
    EXPECT_EQ(ucg_plans_init(nullptr), UCG_ERR_INVALID_PARAM);
    EXPECT_EQ(ucg_plans_add(nullptr, nullptr), UCG_ERR_INVALID_PARAM);
    EXPECT_EQ(ucg_plans_merge(nullptr, nullptr), UCG_ERR_INVALID_PARAM);
    EXPECT_EQ(ucg_plans_prepare(nullptr, nullptr, 0, nullptr), UCG_ERR_INVALID_PARAM);

    ucg_plans_cleanup(plans);
}
#endif

TEST(test_ucg_plan, invalid_range)
{
    ucg_plans_t *plans = nullptr;
    ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);

    ucg_plan_params_t params;
    params.attr.range = {10, 10};
    EXPECT_EQ(ucg_plans_add(plans, &params), UCG_ERR_INVALID_PARAM);

    params.attr.range = {100, 10};
    EXPECT_EQ(ucg_plans_add(plans, &params), UCG_ERR_INVALID_PARAM);

    ucg_plans_cleanup(plans);
}

TEST(test_ucg_plan, add_same_score)
{
    ucg_plans_t *plans = nullptr;
    std::vector<ucg_plan_params_t> params {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 6000}, VGRP_PTR(100), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 4000}, VGRP_PTR(100), 10}},
    };
    ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);

    for (auto &p : params) {
        ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);
    }
    std::vector<test_expect_plan_data> e = {
        {
            assign_from_params(&params[0], 0, 1000), {}
        },
        {
            assign_from_params(&params[0], 1000, 4000),
            {
                assign_from_params(&params[1], 1000, 4000),
            }
        },
        {
            assign_from_params(&params[0], 4000, 6000), {}
        }
    };

    check_expect_data(&plans->plans[coll_type][mem_type], e);

    ucg_plans_cleanup(plans);
}

TEST(test_ucg_plan, call_fallback)
{
    ucg_plans_t *plans = nullptr;
    std::vector<ucg_plan_params_t> params {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4096}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4096}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_unsupported, 0, "", "", 0, {0, 4096}, VGRP_PTR(12), 12}},
    };
    ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);

    for (auto &p : params) {
        ASSERT_EQ(ucg_plans_add(plans, &p), UCG_OK);
    }

    std::vector<test_expect_plan_data> e = {
        {
            assign_from_params(&params[2], 0, 4096),
            {
                assign_from_params(&params[1], 0, 4096),
                assign_from_params(&params[0], 0, 4096),
            }
        },
    };
    check_expect_data(&plans->plans[coll_type][mem_type], e);

    ucg_plan_op_t *op = NULL;
    ucg_coll_args_t args = {
        .type = coll_type,
        .bcast = {
            .count = 128,
            .dt = &dt,
        },
    };
    uint32_t size = 128;
    ASSERT_EQ(ucg_plans_prepare(plans, &args, size, &op), UCG_OK);
    EXPECT_EQ(op, OP_PTR(11));

    ucg_plans_cleanup(plans);
}

TEST(test_ucg_plan, marge_list)
{
    ucg_plans_t *dst = nullptr;
    ucg_plans_t *src = nullptr;
    std::vector<test_expect_plan_data> e;
    std::vector<ucg_plan_params_t> params_dst = {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {2048, 4096}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4096}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {2048, 10240}, VGRP_PTR(12), 12}},
    };

    std::vector<ucg_plan_params_t> params_src = {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4096, 8192}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4096, 10240}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {10240, 102400}, VGRP_PTR(12), 12}},
    };
    
    ASSERT_EQ(ucg_plans_init(&dst), UCG_OK);
    for (auto &p : params_dst) {
        ASSERT_EQ(ucg_plans_add(dst, &p), UCG_OK);
    }
    e = {
        {
            assign_from_params(&params_dst[1], 0, 2048), {}
        },
        {
            assign_from_params(&params_dst[2], 2048, 4096),
            {
                assign_from_params(&params_dst[1], 2048, 4096),
                assign_from_params(&params_dst[0], 2048, 4096),
            }
        },
        {
            assign_from_params(&params_dst[2], 4096, 10240), {}
        },
    };
    check_expect_data(&dst->plans[coll_type][mem_type], e);

    ASSERT_EQ(ucg_plans_init(&src), UCG_OK);
    for (auto &p : params_src) {
        ASSERT_EQ(ucg_plans_add(src, &p), UCG_OK);
    }
    e = {
        {
            assign_from_params(&params_dst[1], 4096, 8192),
            {
                assign_from_params(&params_dst[0], 4096, 8192),
            }
        },
        {
            assign_from_params(&params_dst[1], 8192, 10240), {}
        },
        {
            assign_from_params(&params_dst[2], 10240, 102400), {}
        },
    };
    check_expect_data(&src->plans[coll_type][mem_type], e);

    ASSERT_EQ(ucg_plans_merge(&dst, src), UCG_OK);
    e = {
        {
            assign_from_params(&params_dst[1], 0, 2048), {}
        },
        {
            assign_from_params(&params_dst[2], 2048, 8192),
            {
                assign_from_params(&params_dst[1], 2048, 8192),
                assign_from_params(&params_dst[0], 2048, 8192),
            }
        },
        {
            assign_from_params(&params_dst[2], 8192, 10240),
            {
                assign_from_params(&params_dst[1], 8192, 10240),
            }

        },
        {
            assign_from_params(&params_src[2], 10240, 102400), {}
        },
    };
    check_expect_data(&dst->plans[coll_type][mem_type], e);

    ucg_plans_cleanup(dst);
    ucg_plans_cleanup(src);
}

TEST(test_ucg_plan, add_complex_plan)
{
    ucg_plans_t *plans = nullptr;
    std::vector<ucg_plan_params_t> params = {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 3000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(12), 12}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 2000}, VGRP_PTR(13), 13}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1500, 2500}, VGRP_PTR(14), 14}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5000, 5500}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5100, 6000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4000, 5000}, VGRP_PTR(12), 12}},
    };

    ASSERT_EQ(ucg_plans_init(&plans), UCG_OK);
    for (auto &p : params) {
        ASSERT_EQ(ucg_plans_add(plans, &p), UCG_OK);
    }
    std::vector<test_expect_plan_data> e = {
        {
            assign_from_params(&params[2], 0, 1000),
            {
                assign_from_params(&params[1], 0, 1000),
            }
        },
        {
            assign_from_params(&params[3], 1000, 1500),
            {
                assign_from_params(&params[2], 1000, 1500),
                assign_from_params(&params[1], 1000, 1500),
                assign_from_params(&params[0], 1000, 1500),
            }
        },
        {
            assign_from_params(&params[4], 1500, 2000),
            {
                assign_from_params(&params[3], 1500, 2000),
                assign_from_params(&params[2], 1500, 2000),
                assign_from_params(&params[1], 1500, 2000),
                assign_from_params(&params[0], 1500, 2000),
            }
        },
        {
            assign_from_params(&params[4], 2000, 2500),
            {
                assign_from_params(&params[2], 2000, 2500),
                assign_from_params(&params[1], 2000, 2500),
                assign_from_params(&params[0], 2000, 2500),
            }
        },
        {
            assign_from_params(&params[2], 2500, 3000),
            {
                assign_from_params(&params[1], 2500, 3000),
                assign_from_params(&params[0], 2500, 3000),
            }
        },
        {
            assign_from_params(&params[2], 3000, 4000),
            {
                assign_from_params(&params[1], 3000, 4000),
            }
        },
        {
            assign_from_params(&params[7], 4000, 5000), {}
        },
        {
            assign_from_params(&params[5], 5000, 5100), {}
        },
        {
            assign_from_params(&params[6], 5100, 5500),
            {
                assign_from_params(&params[5], 5100, 5500),
            }
        },
        {
            assign_from_params(&params[6], 5500, 6000), {}
        }
    };
    check_expect_data(&plans->plans[coll_type][mem_type], e);

    ucg_plans_cleanup(plans);
}

TEST(test_ucg_plan, add_plan_in_different_order)
{
    ucg_plans_t *plans1 = nullptr;
    ucg_plans_t *plans2 = nullptr;
    std::vector<ucg_plan_params_t> params1 = {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 2000}, VGRP_PTR(13), 13}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1500, 2500}, VGRP_PTR(14), 14}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 3000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(12), 12}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5100, 6000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5000, 5500}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4000, 5000}, VGRP_PTR(12), 12}},
    };

    std::vector<ucg_plan_params_t> params2 = {
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 3000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {0, 4000}, VGRP_PTR(12), 12}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 2000}, VGRP_PTR(13), 13}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1500, 2500}, VGRP_PTR(14), 14}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5000, 5500}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5100, 6000}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4000, 5000}, VGRP_PTR(12), 12}},
    };

    ASSERT_EQ(ucg_plans_init(&plans1), UCG_OK);
    for (auto &p : params1) {
        ASSERT_EQ(ucg_plans_add(plans1, &p), UCG_OK);
    }

    ASSERT_EQ(ucg_plans_init(&plans2), UCG_OK);
    for (auto &p : params2) {
        ASSERT_EQ(ucg_plans_add(plans2, &p), UCG_OK);
    }

    ucg_list_link_t *head1 = &plan1->plans[coll_type][mem_type];
    ucg_list_link_t *head2 = &plan2->plans[coll_type][mem_type];
    ASSERT_EQ(ucg_list_length(head1), ucg_list_length(head2));
    dump_plan(head1);
    dump_plan(head2);

    ucg_plan_t *plan;
    ucg_plan_t *fb;
    std::vector<ucg_plan_t> v1, v2;
    ucg_list_for_each(plan, head1, list) {
        v1.push_back(*plan);
        ucg_list_for_each(fb, &plan->fallback, fallback) {
            v1.push_back(*fb);
        }
    }

    ucg_list_for_each(plan, head2, list) {
        v2.push_back(*plan);
        ucg_list_for_each(fb, &plan->fallback, fallback) {
            v2.push_back(*fb);
        }
    }

    ASSERT_EQ(v1.size(), v2.size());
    for (size_t i = 0; i < v1.size(); ++i) {
        EXPECT_EQ(test_expect_plan_data::is_same_plan(&v1[i], &v2[i]), true);
    }

    ucg_plans_cleanup(plans1);
    ucg_plans_cleanup(plans2);
}

TEST(test_ucg_plan, merge_boundary_test_plans)
{
    ucg_plans_t *plans1 = nullptr;
    ucg_plans_t *plans2 = nullptr;
    std::vector<ucg_plan_params_t> params1 = {
        /* [500, 3500) */
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {1000, 2000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {2000, 3000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {3000, 3500}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {500, 900}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {900, 1000}, VGRP_PTR(10), 10}},

        /* [4000, 5500) */
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4000, 5000}, VGRP_PTR(10), 10}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {4000, 5500}, VGRP_PTR(11), 11}},
        {mem_type, coll_type, {prepare_ok, 0, "", "", 0, {5000, 6000}, VGRP_PTR(10), 10}},
    };

    ASSERT_EQ(ucg_plans_init(&plans1), UCG_OK);
    for (auto &p : params1) {
        ASSERT_EQ(ucg_plans_add(plans1, &p), UCG_OK);
    }
    ASSERT_EQ(ucg_plans_init(&plans2), UCG_OK);

    ASSERT_EQ(ucg_plans_merge(&plans1, plans2), UCG_OK);

    std::vector<test_expect_plan_data> e = {
        {
            assign_from_params(&params1[0], 500, 3500), {}
        },
        {
            assign_from_params(&params1[6], 4000, 5500),
            {
                assign_from_params(&params1[5], 4000, 5500),
            }
        },
        {
            assign_from_params(&params1[0], 5500, 6000), {}
        }
    };

    ucg_list_link_t *head = &plans1->plans[coll_type][mem_type];
    check_expect_data(head, e);

    ucg_plans_cleanup(plans1);
    ucg_plans_cleanup(plans2);
}

TEST(test_ucg_plan_attr_update, null)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    ASSERT_EQ(ucg_plan_attr_update(&attr, NULL), UCG_OK);
    ASSERT_EQ(ucg_plan_attr_update(&attr, ""), UCG_OK);
}

TEST(test_ucg_plan_attr_update, no_macthed_id)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:11S:100I:12S:12";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    /* nothing changed */
    ASSERT_TRUE(attr.score == 10);
}

TEST(test_ucg_plan_attr_update, score)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1S:100I:11S:110";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.score == 100);
}

TEST(test_ucg_plan_attr_update, score_invalid_fmt)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1S:xx100";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_ERR_INVALID_PARAMS);
}

TEST(test_ucg_plan_attr_update, range)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1R:10-100I:11R:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.range.start == 10);
    ASSERT_TRUE(attr.range.end == 100);

    update = "I:1R:20-I:11R:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.range.start == 20);
    ASSERT_TRUE(attr.range.end == UCG_PLAN_RANGE_MAX);

    update = "I:1R:30I:11R:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.range.start == 30);
    ASSERT_TRUE(attr.range.end == UCG_PLAN_RANGE_MAX);
}

TEST(test_ucg_plan_attr_update, range_invalid_fmt)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1R:xxx10-100I:11R:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_ERR_INVALID_PARAMS);
    /* start >= end */
    update = "I:1R:10-10I:11R:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_ERR_INVALID_PARAMS);
}

TEST(test_ucg_plan_attr_update, group_size)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1G:1-20I:11G:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.deprecated == 0);

    /* Can only be applied to groups with 100 to 200 members */
    update = "I:1G:100-200I:11G:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_OK);
    ASSERT_TRUE(attr.deprecated == 1);
}

TEST(test_ucg_plan_attr_update, group_size_invalid_fmt)
{
    ucg_vgroup_t vgroup = {
        .myrank = 0,
        .size = 10,
        .rank_map = {
            .type = UCG_RANK_MAP_TYPE_FULL,
            .size = 10,
        },
    };
    ucg_plan_attr_t attr = {prepare_ok, 1, "", "", 0, {1000, 2000}, &vgroup, 10};
    const char *update = "I:1G:xxx100-200I:11G:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_ERR_INVALID_PARAMS);

    /* start >= end */
    update = "I:1G:10-10I:11G:1-1000";
    ASSERT_EQ(ucg_plan_attr_update(&attr, update), UCG_ERR_INVALID_PARAMS);
}