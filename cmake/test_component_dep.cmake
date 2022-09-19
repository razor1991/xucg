#
# Copyright (C) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
#
# Dependencies of test_component.cpp

# Build a fake component.
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/component_fake.c
     "#include \"util/ucg_component.h\"
      ucg_component_t ucg_component_fake = {
          .name = \"fake\",
      };
      ucg_component_t ucg_component_fake_loooooooooooooooooog = {
          .name = \"fake_loooooooooooooooooog\"
      };")
# library name is same as object name
add_library(ucg_component_fake SHARED component_fake.c)
# library name isn't same as object name
add_library(ucg_component_fake_no SHARED component_fake.c)
# The length of object name extracted from library name exceeds the limit.
add_library(ucg_component_fake_loooooooooooooooooog SHARED component_fake.c)
add_definitions(-DUCG_GTEST_COMPONENT_PATH="${CMAKE_CURRENT_BINARY_DIR}")