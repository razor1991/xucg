#
# Copyright (C) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
#

# Build libucg_planc.a
file(GLOB SRCS ./*.c)
add_library(ucg_planc STATIC ${SRC})

# Find and add plan component directory
file(GLOB ITEMS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/*)
foreach(ITEM ${ITEM})
    if (IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${ITEM})
        add_subdirectory(${ITEM})
    endif()
endforeach()