export

DISCO_VERSION = 0.4.2
DISCO_RELEASE = 0.4.2

# standard make installation variables
sysconfdir    = /etc
prefix        = /usr/local
exec_prefix   = $(prefix)
localstatedir = $(prefix)/var
datarootdir   = $(prefix)/share
datadir       = $(datarootdir)
bindir        = $(exec_prefix)/bin
libdir        = $(exec_prefix)/lib

SHELL           = /bin/sh
INSTALL         = /usr/bin/install -c
INSTALL_PROGRAM = $(INSTALL)
INSTALL_DATA    = $(INSTALL) -m 644
INSTALL_TREE    = cp -r

# relative directory paths
RELBIN = $(bindir)
RELLIB = $(libdir)/disco
RELDAT = $(datadir)/disco
RELCFG = $(sysconfdir)/disco
RELSRV = $(localstatedir)/disco

# installation directories
TARGETBIN = $(DESTDIR)$(RELBIN)
TARGETLIB = $(DESTDIR)$(RELLIB)
TARGETDAT = $(DESTDIR)$(RELDAT)
TARGETCFG = $(DESTDIR)$(RELCFG)
TARGETSRV = $(DESTDIR)$(RELSRV)

# options to python and sphinx for building the lib and docs
PYTHONENVS = DISCO_VERSION=$(DISCO_VERSION) DISCO_RELEASE=$(DISCO_RELEASE)
SPHINXOPTS = -D version=$(DISCO_VERSION) -D release=$(DISCO_RELEASE)

# used to choose which conf file will be generated
UNAME = $(shell uname)

# utilities used for building disco
DIALYZER   = dialyzer
TYPER      = typer
PYTHON     = python
PY_INSTALL = $(PYTHONENVS) $(PYTHON) setup.py install --root=$(DESTDIR)/ --prefix=$(prefix) $(PY_INSTALL_OPTS)

WWW   = master/www
EBIN  = master/ebin
ESRC  = master/src
EDEP  = master/deps

DEPS     = mochiweb lager
EDEPS    = $(foreach dep,$(DEPS),$(EDEP)/$(dep)/ebin)
ELIBS    = $(ESRC) $(ESRC)/ddfs
ESOURCES = $(foreach lib,$(ELIBS),$(wildcard $(lib)/*.erl))

EPLT  = .dialyzer_plt

.PHONY: master clean test \
	contrib \
	dep dep-clean \
	debian debian-clean \
	doc doc-clean doc-test
.PHONY: install uninstall \
	install-core \
	install-discodb \
	install-examples \
	install-master uninstall-master \
	install-node uninstall-node \
	install-tests
.PHONY: dialyzer dialyzer-clean typer

master: dep
	@ (cd master && ./rebar compile)

clean:
	@ (cd master && ./rebar clean)
	- rm -Rf lib/build lib/disco.egg-info

test:
	@ (cd master && ./rebar -C eunit.config get-deps eunit)

contrib:
	git submodule init
	git submodule update

dep:
	@ (cd master && ./rebar get-deps)

dep-clean:
	- rm -Rf master/deps

debian:
	$(MAKE) -C pkg/debian

debian-clean:
	$(MAKE) -C pkg/debian clean

doc:
	$(MAKE) -C doc html -e

doc-clean:
	$(MAKE) -C doc clean -e

doc-test:
	$(MAKE) -C doc doctest -e

install: install-core install-node install-master

uninstall: uninstall-master uninstall-node

install-core:
	(cd lib && $(PY_INSTALL))

install-discodb: contrib
	$(MAKE) -C contrib/discodb install

install-examples: $(TARGETLIB)/examples

install-master: master \
	$(TARGETDAT)/$(WWW) \
	$(TARGETBIN)/disco $(TARGETBIN)/ddfs \
	$(TARGETCFG)/settings.py

uninstall-master:
	- rm -Rf $(TARGETDAT)
	- rm -f  $(TARGETBIN)/disco $(TARGETBIN)/ddfs
	- rm -Ri $(TARGETCFG)

install-node: master \
	$(TARGETLIB)/$(EBIN) \
	$(addprefix $(TARGETLIB)/,$(EDEPS)) \
	$(TARGETSRV)

uninstall-node:
	- rm -Rf $(TARGETLIB) $(TARGETSRV)

install-tests: $(TARGETLIB)/ext $(TARGETLIB)/tests

dialyzer: $(EPLT) master
	$(DIALYZER) --get_warnings -Wunmatched_returns -Werror_handling --plt $(EPLT) -r $(EBIN)

dialyzer-clean:
	- rm -Rf $(EPLT)

typer: $(EPLT)
	$(TYPER) --plt $(EPLT) -r $(ESRC)

$(EPLT):
	$(DIALYZER) --build_plt --output_plt $(EPLT) \
		    --apps stdlib kernel erts mnesia compiler crypto inets xmerl ssl syntax_tools

$(TARGETDAT)/% $(TARGETLIB)/%: %
	$(INSTALL) -d $(@D)
	$(INSTALL_TREE) $< $(@D)

$(TARGETBIN)/%: bin/%
	$(INSTALL) -d $(@D)
	$(INSTALL_PROGRAM) $< $@

$(TARGETCFG)/settings.py:
	$(INSTALL) -d $(@D)
	(cd conf && ./gen.settings.sys-$(UNAME) > $@ && chmod 644 $@)

$(TARGETSRV):
	$(INSTALL) -d $@
