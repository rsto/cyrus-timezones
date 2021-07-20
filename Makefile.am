# Makefile for the Cyrus timezones build of vzic. See vzic/Makefile.vzic for the
# original Makefile of the vzic command.
SUBDIRS = vzic guesstz

EXTRA_DIST = tzdata

# Install the pkg-config file
pkgconfigdir = $(libdir)/pkgconfig
nodist_pkgconfig_DATA = cyrus-timezones.pc guesstz.pc
DISTCLEANFILES = cyrus-timezones.pc guesstz.pc

all-local: zoneinfo

zoneinfo: vzic/cyr_vzic guesstz/cyr_guesstz
	@echo "Generating zoneinfo files"
	./vzic/cyr_vzic --pure --olson-dir ${srcdir}/tzdata --output-dir zoneinfo
	$(INSTALL_DATA) ${srcdir}/tzdata/version zoneinfo
	@echo "Generating guesstz file"
	./guesstz/cyr_guesstz -z zoneinfo zoneinfo/guesstz.db

# Always use $datadir/cyrus-timezones rather than $pkgdatadir,
# so we can be sure to report the correct path in pkg-config.
mypkgdatadir = $(DESTDIR)$(datadir)/cyrus-timezones

install-data-local: zoneinfo
	@echo "Installing zoneinfo in $(mypkgdatadir)"
	$(INSTALL) -d zoneinfo/ $(mypkgdatadir)
	find zoneinfo -type f -exec $(INSTALL_DATA) -D {} $(mypkgdatadir)/{} \;

uninstall-local:
	rm -rf $(builddir)/zoneinfo
	rm -rf $(mypkgdatadir)/zoneinfo

export
