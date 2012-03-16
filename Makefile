DISCO_VERSION = 0.4.1
DISCO_RELEASE = 0.4.1

# standard make installation variables
sysconfdir    = /etc
prefix        = /usr/local
exec_prefix   = $(prefix)
localstatedir = $(prefix)/var
bindir        = $(exec_prefix)/bin
libdir        = $(exec_prefix)/lib

SHELL           = /bin/sh
INSTALL         = /usr/bin/install -c
INSTALL_PROGRAM = $(INSTALL)
INSTALL_DATA    = $(INSTALL) -m 644
INSTALL_TREE    = cp -r

# installation directories
TARGETBIN = $(DESTDIR)$(bindir)
TARGETLIB = $(DESTDIR)$(libdir)/disco
TARGETCFG = $(DESTDIR)$(sysconfdir)/disco
TARGETSRV = $(DESTDIR)$(localstatedir)/disco

# options to python and sphinx for building the lib and docs
PYTHONENVS = DISCO_VERSION=$(DISCO_VERSION) DISCO_RELEASE=$(DISCO_RELEASE)
SPHINXOPTS = "-D version=$(DISCO_VERSION) -D release=$(DISCO_RELEASE)"

# used to choose which conf file will be generated
UNAME = $(shell uname)

# utilities used for building disco
DIALYZER   = dialyzer
TYPER      = typer
PYTHON     = python
PY_INSTALL = $(PYTHONENVS) $(PYTHON) setup.py install --root=$(DESTDIR)/ --prefix=$(prefix)

WWW   = master/www
EBIN  = master/ebin
ESRC  = master/src
EDEP  = master/deps

EDEPS    = $(shell find $(EDEP) -name ebin)
ELIBS    = $(ESRC) $(ESRC)/ddfs
ESOURCES = $(foreach lib,$(ELIBS),$(wildcard $(lib)/*.erl))

EPLT  = .dialyzer_plt

.PHONY: master clean test dist-clean doc doc-clean doc-test
.PHONY: install \
	install-master \
	install-core \
	install-node \
	install-examples \
	install-tests \
	uninstall
.PHONY: dialyzer typer

master: Makefile
	@ (cd master && ./rebar get-deps compile)

clean:
	@ (cd master && ./rebar clean)
	- rm -Rf lib/build lib/disco.egg-info

test:
	@ (cd master && ./rebar -C eunit.config get-deps eunit)

dist-clean: clean
	- rm -Rf $(EPLT)

doc:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) html)

doc-clean:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) clean)

doc-test:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) doctest)

install: install-core install-master

install-core:
	(cd lib && $(PY_INSTALL))

install-examples: $(TARGETLIB)/examples

install-master: install-node \
	$(TARGETLIB)/$(WWW) \
	$(TARGETBIN)/disco $(TARGETBIN)/ddfs \
	$(TARGETCFG)/settings.py \
	$(TARGETSRV)/ddfs

install-node: master \
	$(TARGETLIB)/$(EBIN) \
	$(addprefix $(TARGETLIB)/,$(EDEPS))

install-tests: $(TARGETLIB)/ext $(TARGETLIB)/tests

uninstall:
	- rm -f  $(TARGETBIN)/disco $(TARGETBIN)/ddfs
	- rm -Rf $(TARGETCFG) $(TARGETLIB) $(TARGETSRV)

dialyzer: $(EPLT) master
	$(DIALYZER) --get_warnings -Wunmatched_returns -Werror_handling --plt $(EPLT) -r $(EBIN)

typer: $(EPLT)
	$(TYPER) --plt $(EPLT) -r $(ESRC)

$(EPLT):
	$(DIALYZER) --build_plt --output_plt $(EPLT) \
		    --apps stdlib kernel erts mnesia compiler crypto inets xmerl ssl syntax_tools

$(TARGETLIB)/%: %
	$(INSTALL) -d $(@D)
	$(INSTALL_TREE) $< $@

$(TARGETBIN)/%: bin/%
	$(INSTALL) -d $(@D)
	$(INSTALL_PROGRAM) $< $@

$(TARGETCFG)/settings.py:
	$(INSTALL) -d $(@D)
	(cd conf && TARGETLIB=$(TARGETLIB) TARGETSRV=$(TARGETSRV) ./gen.settings.sys-$(UNAME) > $@ && chmod 644 $@)

$(TARGETSRV)/ddfs:
	$(INSTALL) -d $@
