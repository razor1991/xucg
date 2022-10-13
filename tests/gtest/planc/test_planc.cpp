/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/

#include <gtest/gtest.h>
#include "stub.h"

extern "C" {
#include "planc/ucg_planc.h"
}

using namespace test;

class test_ucg_planc_get : public :: testing::Test {
public:
    void SetUp()
    {
        ucg_planc_load();
    }

    void TearDown()
    {
        ucg_planc_unload();
    }
};

class test_ucg_planc_load : public :: testing::Test {
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

TEST_F(test_ucg_planc_load, default_path)
{
    unsetenv("UCG_PLANC_PATH");
    ASSERT_EQ(test_ucg_planc_load(), UCG_OK);
    test_ucg_planc_unload();
}

TEST_F(test_ucg_planc_load, user_define_path)
{
    setenv("UCG_PLANC_PATH", UCG_GTEST_PLANC_USER_DEFINE_PATH, 1);
    ASSERT_EQ(ucg_planc_load(), UCG_OK);
    ucg_planc_unload();
}

TEST_T(test_ucg_planc_load, multi_times)
{
    ASSERT_EQ(ucg_planc_load(), UCG_OK);
    ASSERT_EQ(ucg_planc_load(), UCG_ERR_INVALID_PARAM);
    ucg_planc_unload();

    ASSERT_EQ(ucg_planc_load(), UCG_OK);
    ucg_planc_unload();
    // multi unload
    ucg_planc_unload();
}

TEST_F(test_ucg_planc_load, wrong_path)
{
    setenv("UCG_PLANC_PATH", "/path_not_exist", 1);
    ASSERT_NE(ucg_planc_load(), UCG_OK);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_planc_load, fail_malloc)
{
    std::vector<stub::routine_result_t> result = {stub::FAILURE};
    unsetenv("UCG_PLANC_PATH");

    std::mock(stub::STRDUP, result, "planc lib path");
    ASSERT_EQ(test_ucg_planc_load(), UCG_ERR_NO_RESOURCE);

    std::mock(stub::MALLOC, result, "planc default path");
    ASSERT_EQ(test_ucg_planc_load(), UCG_ERR_NO_RESOURCE);
}
#endif

TEST_F(test_ucg_planc_get, count)
{
    // libucg_planc_fake.so and libucg_planc_fake2.so will be loaded.
    ASSERT_EQ(ucg_planc_count(), 2);
}

TEST_F(test_ucg_planc_get, by_name)
{
    ASSERT_TRUE(ucg_planc_get_by_name("fake") != NULL);
    ASSERT_TRUE(ucg_planc_get_by_name("fake2") != NULL);
}

TEST_F(test_ucg_planc_get, by_idx)
{
    int idx = ucg_planc_count() - 1;
    ASSERT_TRUE(ucg_planc_get_by_idx(idx) != NULL);
}

#ifdef UCG_ENABLE_CHECK_PARAMS
TEST_F(test_ucg_planc_get, by_invalid_name)
{
    ASSERT_TRUE(ucg_planc_get_by_name(NULL) == NULL);
    ASSERT_TRUE(ucg_planc_get_by_name("not_exist_name") == NULL);
}

TEST_F(test_ucg_planc_get, by_invalid_idx)
{
    int invaild_idx = ucg_planc_count();
    ASSERT_TRUE(ucg_planc_get_by_idx(invalid_idx) == NULL);
}
#endif
