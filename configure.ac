dnl Process this file with autoconf to produce a configure script.
AC_INIT(cyrus-timezones, 1.0)
AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE
AM_CONFIG_HEADER([vzic/config.h])
AC_CONFIG_MACRO_DIR([m4])

dnl Checks for programs.
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_LIBTOOL

dnl Checks for headers
dnl AC_FUNC_ALLOCA
dnl AC_HEADER_STDC
dnl AC_CHECK_HEADERS([inttypes.h limits.h stdlib.h string.h sys/time.h])

dnl Checks for typedefs, structures, and compiler characteristics.
dnl AC_C_CONST
dnl AC_C_INLINE
dnl AC_TYPE_SIZE_T
dnl AC_HEADER_TIME
dnl AC_STRUCT_TM

dnl Checks for functions
dnl AC_FUNC_MALLOC
dnl AC_FUNC_REALLOC
dnl AC_FUNC_VPRINTF
dnl AC_CHECK_FUNCS([gettimeofday memset strchr strdup strpbrk])

dnl Checks for required libraries
PKG_CHECK_MODULES([ICAL], [libical])
AC_SUBST([ICAL_LIBS])
AC_SUBST([ICAL_CFLAGS])

PKG_CHECK_MODULES([GLIB], [glib-2.0])
AC_SUBST([GLIB_LIBS])
AC_SUBST([GLIB_CFLAGS])

AC_CONFIG_FILES([Makefile vzic/Makefile])
AC_OUTPUT()
