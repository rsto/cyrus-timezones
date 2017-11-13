# Makefile for the Cyrus timezones build of vzic. See Makefile.vzic for the
# original Makefile of the vzic command.

# This is used as the PRODID property on the iCalendar files output.
# It identifies the product which created the iCalendar objects.
# So you need to substitute your own organization name and product.
PRODUCT_ID = -//CyrusIMAP.org//Cyrus %s//EN

# This is used to create unique IDs for each VTIMEZONE component.
# The prefix is put before each timezone city name. It should start and end
# with a '/'. The first part, i.e. 'myorganization.org' below, should be
# a unique vendor ID, e.g. use a hostname. The part after that can be
# anything you want. We use a date and version number for libical. The %D
# gets expanded to today's date. There is also a vzic-merge.pl which can be
# used to merge changes into a master set of VTIMEZONEs. If a VTIMEZONE has
# changed, it bumps the version number on the end of this prefix. */
TZID_PREFIX =

# You will need to set this to the directory that the Olson timezone data
# files are in.
OLSON_DIR = .

bin_PROGRAMS = cyr_vzic

EXTRA_DIST =

cyr_vzic_SOURCES = \
	vzic.c \
	vzic.h \
	vzic-parse.c \
	vzic-parse.h \
	vzic-dump.c \
	vzic-dump.h \
	vzic-output.c \
	vzic-output.h

cyr_vzic_CFLAGS = \
	$(ICAL_CFLAGS) \
	$(GLIB_CFLAGS) \
	-DOLSON_DIR=\"$(OLSON_DIR)\" \
	-DPRODUCT_ID='"$(PRODUCT_ID)"' \
	-DTZID_PREFIX='"$(TZID_PREFIX)"' \
	-Wno-unused-variable \
	-Wno-unused-parameter \
	-Wno-sign-compare \
	-Wno-parentheses \
	-Wno-unused-function

cyr_vzic_LDFLAGS = \
	$(ICAL_LIBS) \
	$(GLIB_LIBS)
