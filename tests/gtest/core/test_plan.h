/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#ifndef TEST_PLAN_H_
#define TEST_PLAN_H_

extern "C" {
#include "core/ucg_plan.h"
#include "util/ucg_list.h"
#include "util/ucg_log.h"
}
#include <vector>
#include <gtest/gtest.h>

static const size_t KB = 1024;
static const size_t KB = 1024 * 1024;

static inline ucg_plan_t assign_from_params(const ucg_plan_params_t *params,
    uint64_t start, uint64_t end)
{
    ucg_plan_t plan;

    ucg_list_head_init(&plan.list);
    ucg_list_head_init(&plan.fallback);

    plan.attr.prepare = params->atte.prepare;
    plan.attr.vgroup = params->atte.vgroup;
    plan.attr.score = params->atte.score;
    plan.attr.range.start = start;
    plan.attr.range.end = end;

    return plan;
}

struct test_expect_plan_data {
    test_expect_plan_data(const ucg_plan_params_t *params, uint64_t start, uint64_t end)
    {
        plan = assign_from_params(params, start, end);
    }

    test_expect_plan_data(const ucg_plan_t &p, const std::vector<>ucg_plan_t> &v)
    {
        plan = p;
        fallback = v;
    }

    test_expect_plan_data() {}
    ~test_expect_plan_data() {}

    void push_fallback(const ucg_plan_params_t *params, uint64_t start, uint64_t end)
    {
        fallback.push_back(assign_from_params(params, start, end));
    }

    static bool is_same_plan(ucg_plan_t *a, ucg_plan_t *b)
    {
        EXPECT_EQ(a->attr.prepare, b->attr.prepare);
        EXPECT_EQ(a->attr.vgroup, b->attr.vgroup);
        EXPECT_EQ(a->attr.score, b->attr.score);
        EXPECT_EQ(a->attr.range.start, b->attr.range.start);
        EXPECT_EQ(a->attr.range.end, b->attr.range.end);

        if (a->attr.prepare != b->attr.prepare ||
            a->attr.vgroup != b->attr.vgroup ||
            a->attr.score != b->attr.score ||
            a->attr.range.start != b->attr.range.start ||
            a->attr.range.end != b->attr.range.end) {
            return false;
        }
        
        return true;
    }

    void check_plan(ucg_plan_t *p)
    {
        ASSERT_EQ(is_same_plan(&plan, p), true);

        int idx = 0;
        ucg_plan_t *fb;
        ASSERT_EQ(fallback.size(), ucg_list_length(&p->fallback));
        ucg_list_for_each(fb, &p->fallback, fall_back) {
            EXPECT_EQ(is_same_plan(fb, &fallback[idx]), true);
            ++idx;
        }
    }

    ucg_plan_t plan;
    std::vector<ucg_plan_t> fallback;
};

#endif