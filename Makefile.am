# Makefile for the Cyrus timezones build of vzic. See vzic/Makefile.vzic for the
# original Makefile of the vzic command.
SUBDIRS = vzic

EXTRA_DIST = tzdata

all-local: zoneinfo

zoneinfo: vzic/cyr_vzic
	@echo "Generating zoneinfo files"
	./vzic/cyr_vzic --pure --olson-dir ${srcdir}/tzdata --output-dir zoneinfo

install-data-local: zoneinfo
	@echo "Installing zoneinfo in $(pkgdatadir)"
	$(INSTALL) -d zoneinfo/ $(pkgdatadir)
	find zoneinfo -exec $(INSTALL_DATA) -D {} $(pkgdatadir)/{} \;

uninstall-local:
	rm -rf $(builddir)/zoneinfo
	rm -rf $(pkgdatadir)/zoneinfo

export
