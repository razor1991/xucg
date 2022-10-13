#
# Copyright (c) Huawei Rechnologies Co., Ltd. 2022-2022. All rights reserved.
#

# Support clang
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "-stdlib=libc++ ${CMAKE_CXX_FLAGS}")
endif()

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    # googletest tag release-1.11.0
    GIT_TAG        e2239ee6043f73722e7aa812a459f54a28552929
)
# Not install gtest
set(INSTALL_GITSET "OFF" CACHE STRING "" FORCE)
FetchContent_MakeAvailable(googletest)

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_definitions(-DUCG_ENABLE_DEBUG)
endif()

add_definitions(-DUCG_ENABLE_CHECK_PARAMS)
if (UCG_ENABLE_CHECK_PARAMS MATCHES "OFF")
    remove_definitions(-DUCG_ENABLE_CHECK_PARAMS)
endif()

if (UCG_ENABLE_MT MATCHES "NO")
    add_directories(${CMAKE_CURRENT_SOURCE_DIR})
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Build test dependencies
include(${CMAKE_SOUECR_DIR}/cmake/test_component_dep.cmake)
include(${CMAKE_SOUECR_DIR}/cmake/test_planc_dep.cmake)

# Build ucg_gtest
file(GLOB_RECURSE SRCS ./*.cpp)

set(LINK_LIB gtest_main ucg ucg_planc_ucx ucg_planc_hccl)
if (UCG_BUILD_PLANC_UCX MATCHES "OFF")
    list(FILTER SRCS EXCLUDE REGEX ".*planc/ucx.*")
    list(FILTER LINK_LIB EXCLUDE REGEX "ucg_planc_ucx")
endif()
if (UCG_BUILD_PLANC_HCCL MATCHES "OFF")
    list(FILTER SRCS EXCLUDE REGEX ".*planc/hccl.*")
    list(FILTER LINK_LIB EXCLUDE REGEX "ucg_planc_hccl")
elseif (IS_DIERCTORY ${UCG_BUILD_WITH_HCCL})
    link_directories(${UCG_BUILD_WITH_HCCL}/lib64)
    list(APPEND LINK_LIB ascendcl)
endif()

add_executable(ucg_gtest ${SRCS})
target_link_libraries(ucg_gtest ${LINK_LIB})

include(GoogleTest)
gtest_discover_tests(ucg_gtest)