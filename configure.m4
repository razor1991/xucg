#
# Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

#
# Enable fault-tolerance
#
AC_ARG_ENABLE([fault-tolerance],
              [AS_HELP_STRING([--enable-fault-tolerance],
                              [Enable fault-tolerance, default: NO])],
              [],
              [enable_fault_tolerance=no])

AS_IF([test "x$enable_fault_tolerance" = xyes],
      [AS_MESSAGE([enabling with fault-tolerance])
       AC_DEFINE([ENABLE_FAULT_TOLERANCE], [1], [Enable fault-tolerance])],
      [:])


# Set special flags for API incompatiblity detection (below)
SAVE_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="-Isrc/ $CPPFLAGS"

AC_ARG_ENABLE([ucg_hicoll],
              AS_HELP_STRING([--enable-ucg-hicoll],
                             [Enable the group collective operations with hicoll (experimental component), default: YES]),
              [],
              [enable_ucg_hicoll=yes])
AS_IF([test "x$enable_ucg_hicoll" != xno],
      [ucg_modules=":builtin:hicoll"
      AC_DEFINE([ENABLE_UCG_HICOLL], [1],
                [Enable Groups and collective operations support (UCG) with hicoll])
      AC_MSG_NOTICE([Building with Groups and collective operations support (UCG) with hicoll])
      ])

m4_include([src/ucg/base/configure.m4])
m4_include([src/ucg/builtin/configure.m4])
AC_DEFINE_UNQUOTED([ucg_MODULES], ["${ucg_modules}"], [UCG loadable modules])

AC_CONFIG_FILES([src/ucg/Makefile
                 src/ucg/api/ucg_version.h
                 src/ucg/base/ucg_version.c])