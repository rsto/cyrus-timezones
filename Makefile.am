# Makefile for the Cyrus timezones build of vzic. See vzic/Makefile.vzic for the
# original Makefile of the vzic command.
SUBDIRS = vzic

EXTRA_DIST = tzdata

# Install the pkg-config file
pkgconfigdir = $(libdir)/pkgconfig
nodist_pkgconfig_DATA = cyrus-timezones.pc
DISTCLEANFILES = cyrus-timezones.pc

all-local: zoneinfo

zoneinfo: vzic/cyr_vzic
	@echo "Generating zoneinfo files"
	./vzic/cyr_vzic --pure --olson-dir ${srcdir}/tzdata --output-dir zoneinfo

# Always use $datadir/cyrus-timezones rather than $pkgdatadir,
# so we can be sure to report the correct path in pkg-config.
mypkgdatadir = $(datadir)/cyrus-timezones

install-data-local: zoneinfo
	@echo "Installing zoneinfo in $(mypkgdatadir)"
	$(INSTALL) -d zoneinfo/ $(mypkgdatadir)
	find zoneinfo -exec $(INSTALL_DATA) -D {} $(mypkgdatadir)/{} \;

uninstall-local:
	rm -rf $(builddir)/zoneinfo
	rm -rf $(mypkgdatadir)/zoneinfo

export
