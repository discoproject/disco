DISCO_VERSION = 0.5.4
DISCO_RELEASE = 0.5.4

# standard make installation variables
sysconfdir    = /etc
prefix        = /usr
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
export RELBIN = $(bindir)
export RELLIB = $(libdir)/disco
export RELDAT = $(datadir)/disco
export RELCFG = $(sysconfdir)/disco
export RELSRV = $(localstatedir)/disco

# installation directories
export TARGETBIN = $(DESTDIR)$(RELBIN)
export TARGETLIB = $(DESTDIR)$(RELLIB)
export TARGETDAT = $(DESTDIR)$(RELDAT)
export TARGETCFG = $(DESTDIR)$(RELCFG)
export TARGETSRV = $(DESTDIR)$(RELSRV)

export ABSTARGETLIB = $(RELLIB)
export ABSTARGETSRV = $(RELSRV)
export ABSTARGETDAT = $(RELDAT)

# options to python and sphinx for building the lib and docs
PYTHONENVS = DISCO_VERSION=$(DISCO_VERSION) DISCO_RELEASE=$(DISCO_RELEASE)
SPHINXOPTS = -D version=$(DISCO_VERSION) -D release=$(DISCO_RELEASE)

# utilities used for building disco
DIALYZER   = dialyzer
TYPER      = typer
PYTHON     = python
PY_INSTALL = $(PYTHONENVS) $(PYTHON) setup.py install --root=$(DESTDIR)/ --prefix=$(prefix) $(PY_INSTALL_OPTS)

export WWW   = master/www
EBIN  = master/ebin
ESRC  = master/src
EDEP  = master/deps

DEPS     = mochiweb lager goldrush folsom bear meck folsomite plists
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
	- find lib -name __pycache__ | xargs rm -rf

xref: master
	@ (cd master && ./rebar xref)

erlangtest:
	@ (cd master && ./rebar -C eunit.config get-deps compile &&\
	./rebar -C eunit.config eunit skip_deps=true && cd -)

pythontest:
	@ (cd lib && python setup.py install --user)
	@ (cd lib/test && pip install nose --user && PATH=${PATH}:~/.local/bin nosetests)

test: pythontest dialyzer erlangtest

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

install-core: $(TARGETBIN)/disco $(TARGETBIN)/ddfs
	(cd lib && $(PY_INSTALL))

install-discodb: contrib
	$(MAKE) -C contrib/discodb install

install-examples: $(TARGETLIB)/examples

install-master: master \
	$(TARGETDAT)/$(WWW) \
	$(TARGETCFG)/settings.py

uninstall-master:
	- rm -Rf $(TARGETDAT)
	- rm -f  $(TARGETBIN)/disco $(TARGETBIN)/ddfs
	- rm -Ri $(TARGETCFG)

install-node: master \
	$(TARGETLIB)/$(EBIN) \
	$(addprefix $(TARGETLIB)/,$(EDEPS)) \
	$(TARGETSRV)

$(TARGETLIB)/$(EBIN):
	$(INSTALL) -d $(@D)
	$(INSTALL_TREE) $(EBIN) $(@D)

uninstall-node:
	- rm -Rf $(TARGETLIB) $(TARGETSRV)

install-tests: $(TARGETLIB)/ext $(TARGETLIB)/tests

dialyzer: $(EPLT) master
	$(DIALYZER) --get_warnings -Wno_return -Wunmatched_returns -Werror_handling --plt $(EPLT) -r $(EBIN)

dialyzer-clean:
	- rm -Rf $(EPLT)

typer: $(EPLT)
	$(TYPER) --plt $(EPLT) -r $(ESRC)

$(EPLT):
	$(DIALYZER) --build_plt --output_plt $(EPLT) \
		    --apps stdlib kernel erts compiler crypto inets syntax_tools

$(TARGETDAT)/% $(TARGETLIB)/%: %
	$(INSTALL) -d $(@D)
	$(INSTALL_TREE) $< $(@D)

$(TARGETDAT)/$(WWW) $(TARGETLIB)/$(WWW): $(WWW)
	$(INSTALL) -d $(@D)
	$(INSTALL_TREE) $(WWW) $(@D)


$(TARGETBIN)/disco: bin/disco
	$(INSTALL) -d $(@D)
	$(INSTALL_PROGRAM) bin/disco $@

$(TARGETBIN)/ddfs: bin/ddfs
	$(INSTALL) -d $(@D)
	$(INSTALL_PROGRAM) bin/ddfs $@

$(TARGETCFG)/settings.py:
	$(INSTALL) -d $(@D)
	(cd conf && ./gen.settings.sys-`uname -s` > $@ && chmod 644 $@)

$(TARGETSRV):
	$(INSTALL) -d $@
