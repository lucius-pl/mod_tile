#
# Autoconf for Data Structures and Algorithms library
# 
# License: LGPL
# Contact: https://github.com/lucius-pl/libdsaa
# Version: 0.0.1

AC_DEFUN([LIBDSAA_CHECK_CONFIG],[

    AC_ARG_WITH([libdsaa],[AS_HELP_STRING([--with-libdsaa[=DIR]],[path to libdsaa])])
    with_libdsaa="$withval"

    if test "$with_libdsaa" == "no"; then
        AC_MSG_NOTICE([skipped checking for libdsaa])
    elif test "$with_libdsaa" == "yes"; then
        AC_CHECK_LIB(dsaa, list_init,
        [
            AC_DEFINE([HAVE_LIBDSAA], [1], [Have found libdsaa])
            LIBDSAA_LDFLAGS='-ldsaa'
            AC_SUBST(LIBDSAA_LDFLAGS)
        ])
    else
        AC_MSG_CHECKING([for libdsaa])
        if test -f "$with_libdsaa/include/libdsaa.h" && test -f "$with_libdsaa/lib/libdsaa.so"; then
          AC_DEFINE([HAVE_LIBDSAA], [1], [Have found libdsaa])
          LIBDSAA_LDFLAGS="-L$with_libdsaa/lib -ldsaa"
          AC_SUBST(LIBDSAA_LDFLAGS)
          LIBDSAA_CFLAGS="-I$with_libdsaa/include"
          AC_SUBST(LIBDSAA_CFLAGS)
          AC_MSG_RESULT([yes])
        else
          AC_MSG_RESULT([no])
        fi
    fi
    unset with_libdsaa

])dnl
