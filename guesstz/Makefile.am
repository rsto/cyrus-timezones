# Makefile for guesstz

bin_PROGRAMS = cyr_guesstz

EXTRA_DIST =

RPATHS = $(ICAL_LIBDIR):$(JANSSON_LIBDIR)

AM_CFLAGS = \
	$(ICAL_CFLAGS) \
	$(GLIB_CFLAGS) \
	-Wno-unused-variable \
	-Wno-unused-parameter \
	-Wno-sign-compare \
	-Wno-parentheses \
	-Wno-unused-function \
	-fPIC \
	-Wall -Werror \
	-fstack-protector-all \
	-fno-semantic-interposition \
	-Bsymbolic \
	-fno-omit-frame-pointer

cyr_guesstz_SOURCES = \
	guesstz.c

cyr_guesstz_CFLAGS = \
	$(AM_CFLAGS) \
	-DBUILD_MAIN

cyr_guesstz_LDFLAGS = \
	$(ICAL_LIBS) \
	$(JANSSON_LIBS) \
	-Wl,-rpath,$(RPATHS)

lib_LTLIBRARIES = libguesstz.la

libguesstz_la_SOURCES = \
	guesstz.c

libguesstz_la_CFLAGS = \
	$(AM_CFLAGS)

include_HEADERS = \
	guesstz.h

TESTS = tests/us_eastern.sh tests/gmt.sh tests/berlin.sh
