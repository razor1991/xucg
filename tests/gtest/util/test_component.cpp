#include <gtest/gtest.h>

#include "stub.h"

extern "C" {
#include "util/ucg_component.h"
}

using namespace test;

static const char *valid_path = UCG_GTEST_COMPONENT_PATH;
static const char *valid_pattern = "libucg_component_*.so";

class test_ucg_component : public :: testing::Test {
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

TEST_F(test_ucg_component, load)
{
    ucg_components_t components;
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_OK);
    // only the libucg_planc_test.so will be loaded
    ASSERT_EQ(components.num, 1);
    ASSERT_STREQ(components.components[0]->name, "fake");
    ucg_components_unload(&components);
}

TEST_F(test_ucg_component, load_specific_so)
{
    ucg_components_t components;
    ASSERT_EQ(ucg_components_load(valid_path, "libucg_component_fake.so", &components), UCG_OK);
    ASSERT_EQ(components.num, 1);
    ASSERT_STREQ(components.components[0]->name, "fake");
    ucg_components_unload(&components);
}

TEST_F(test_ucg_component, load_invalid_args)
{
    ucg_components_t components;

    ASSERT_EQ(ucg_components_load(NULL, valid_pattern, &components), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_components_load(valid_path, NULL, &components), UCG_ERR_INVALID_PARAM);
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, NULL), UCG_ERR_INVALID_PARAM);

    // not start with "lib"
    const char *invalid_pattern = "ucg_component_fake.so";
    ASSERT_EQ(ucg_components_load(valid_path, invalid_pattern, &components), UCG_ERR_INVALID_PARAM);
    // not end with ".so"
    invalid_pattern = "libucg_component_fake";
    ASSERT_EQ(ucg_components_load(valid_path, invalid_pattern, &components), UCG_ERR_INVALID_PARAM);
}

TEST_F(test_ucg_component, load_non_exist_so)
{
    ucg_components_t components;
    const char *wrong_path = "/path_not_exist";
    ASSERT_EQ(ucg_components_load(wrong_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);

    const char *wrong_pattern = "libno_match_*.so";
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);

    // incorrect library
    wrong_pattern = "libucg_component_fake_no.so";
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);
}

TEST_F(test_ucg_component, load_objname_exceeded_so)
{
    ucg_components_t components;
    // The library whose object symbol name exceeds the limit cannot be loaded.
    ASSERT_EQ(ucg_components_load(valid_path,
                                  "libucg_component_fake_looooooooooooooooong.so",
                                  &components),
              UCG_ERR_NO_RESOURCE);
}

#ifdef UCG_ENABLE_DEBUG
TEST_F(test_ucg_component, load_fail_malloc)
{
    ucg_components_t components;
    std::vector<stub::routine_result_t> result = {stub::FAILURE};

    stub::mock(stub::MALLOC, result, "full pattern");
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);

    stub::mock(stub::MALLOC, result, "components");
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);

    stub::mock(stub::MALLOC, result, "component path");
    ASSERT_EQ(ucg_components_load(valid_path, valid_pattern, &components), UCG_ERR_NO_RESOURCE);
}
#endif

TEST_F(test_ucg_component, unload_invalid_args)
{
    // Expect no segmentation fault.
    ucg_components_unload(NULL);
}
