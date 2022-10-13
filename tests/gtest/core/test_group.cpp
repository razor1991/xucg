/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/
#include <gtest/gtest.h>
#include <ucg/api/ucg.h>

#include "stub.h"

extern "C" {
#include "core/ucg_group.h"
}

using namespace test;

class test_ucg_group : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        stub::init(true);
        
        // use plnc fake to test.
        setenv("UCG_PLANC", "fake, fake2", 1);
        ucg_config_h config;
        ucg_config_read(NULL, NULL, &config);
        ucg_init(&test_stub_context_params, config, &m_context);
        ucg_config_release(config);
    }

    static void TearDownTestSuite()
    {
        ucg_cleanup(m_context);
        stub::cleanup();
    }

    static ucg_context_h m_context;
};
ucg_context_h test_ucg_group::m_context;

TEST_F(test_ucg_group, create)
{
    ucg_group_h group = NULL;
    ASSERT_EQ(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);
    ASSERT_TRUE(group != NULL);
    ASSERT_TRUE(group->context == m_context);
    ASSERT_EQ(group->id, test_stub_group_params.id);
    ASSERT_EQ(group->size, test_stub_group_params.size);
    ASSERT_EQ(group->myrank, test_stub_group_params.myrank);
    // rank map may be optimized, cannot conpare the fields other than size.
    ASSERT_EQ(group->rank_map.size, test_stub_group_params.rank_map.size);
    ucg_group_destroy(group);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_ucg_group, create_invalid_args)
{
    ucg_group_params_t params;
    ucg_group_h group;
    ASSERT_EQ(ucg_group_create(NULL, &params, &group), UCG_ERR_INVALID_PARAMS);
    ASSERT_EQ(ucg_group_create(m_context, NULL, &group), UCG_ERR_INVALID_PARAMS);
    ASSERT_EQ(ucg_group_create(m_context, &params, NULL), UCG_ERR_INVALID_PARAMS);
}

TEST_T(test_ucg_group, destroy_invalid_args)
{
    // expect no segmentation fault
    ucg_group_destroy(NULL);
}
#endif

TEST_T(test_ucg_group, create_no_required_field)
{
    ucg_group_h group;

    ucg_group_params_t params;
    memcpy(&params, &test_stub_group_params, sizeof(ucg_group_params_t));
    // test required UCG_GROUP_PARAMS_FIELD_ID.
    params.field_mask = 0;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);

    // test required UCG_GROUP_PARAMS_FIELD_SIZE.
    params.field_mask |= UCG_GROUP_PARAMS_FIELD_ID;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);

    // test required UCG_GROUP_PARAMS_FIELD_MYRANK.
    params.field_mask |= UCG_GROUP_PARAMS_FIELD_SIZE;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);

    // test required UCG_GROUP_PARAMS_FIELD_RANK_MAP.
    params.field_mask |= UCG_GROUP_PARAMS_FIELD_MYRANK;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);

    // test required UCG_GROUP_PARAMS_FIELD_OOB_GROUP.
    params.field_mask |= UCG_GROUP_PARAMS_FIELD_RANK_MAP;
    params.rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);
}

TEST_T(test_ucg_group, create_invalid_field)
{
    ucg_group_h group;
    ucg_group_params_t params;
    memcpy(&params, &test_stub_group_params, sizeof(ucg_group_params_t));
    // rank map size is not equal to group size.
    params.size = params.rank_map.size + 1;
    ASSERT_EQ(ucg_group_create(m_context, &params, &group), UCG_ERR_INVALID_PARAMS);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_group, create_fail_malloc)
{
    ucg_group_h group;
    stub::mock(stub::CALLOC, {stub::FAILURE}, "ucg group");
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);

    stub::mock(stub::CALLOC, {stub::FAILURE}, "planc groups");
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);

    stub::mock(stub::MALLOC, {stub::FAILURE}, "ucg plans");
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);
}
#endif

TEST_F(test_ucg_group, create_fail_planc_group)
{
    ucg_group_h group;
    stub::mock(stub::PLANC_GROUP_CREATE, {stub::FAILURE});
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);

    stub::mock(stub::PLANC_GROUP_CREATE, {stub::SUCCESS, stub::FAILURE});
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);
}

TEST_F(test_ucg_group, create_fail_get_plans)
{
    ucg_group_h group;
    stub::mock(stub::PLANC_GET_PLANS, {stub::FAILURE});
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);

    stub::mock(stub::PLANC_GET_PLANS, {stub::SUCCESS, stub::FAILURE});
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_group, create_fail_init_topo)
{
    ucg_group_h group;
    stub::mock(stub::CALLOC, {stub::FAILURE}, "ucg topo");
    ASSERT_NE(ucg_group_create(m_context, &test_stub_group_params, &group), UCG_OK);
}
#endif