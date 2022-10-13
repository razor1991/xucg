/*
* Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
*/
#include <gtest/gtest.h>

extern "C" {
    #include "core/ucg_global.h"
}
ucg_global_params_t params;

TEST(test_global, init)
{
    ASSERT_EQ(ucg_global_init(&params), UCG_OK);
    ucg_global_cleanup();
}

TEST(test_global, init_multi_times)
{
    ASSERT_EQ(ucg_global_init(&params), UCG_OK);
    ASSERT_EQ(ucg_global_init(&params), UCG_OK);
    ASSERT_EQ(ucg_global_init(&params), UCG_OK);
    ucg_global_cleanup();
}

TEST(test_global, fail_init)
{
    setenv("UCG_PLANC_PATH", "/no_exist", 1);
    ASSERT_NE(ucg_global_init(&params), UCG_OK);
    unsetenv("UCG_PLANC_PATH");
}