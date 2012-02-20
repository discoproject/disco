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
DIRNAME         = dirname

# installation directories
TARGETBIN = $(DESTDIR)$(bindir)
TARGETLIB = $(DESTDIR)$(libdir)/disco
TARGETCFG = $(DESTDIR)$(sysconfdir)/disco
TARGETSRV = $(DESTDIR)$(localstatedir)/disco

# options to python and sphinx for building the lib and docs
PYTHONENVS = DISCO_VERSION=$(DISCO_VERSION) DISCO_RELEASE=$(DISCO_RELEASE)
SPHINXOPTS = "-D version=$(DISCO_VERSION) -D release=$(DISCO_RELEASE)"

# utilities used for building disco
REBAR      = ./rebar    # relative to ./master
ERL        = erl
ERLC       = erlc
EOPT       = -W
DIALYZER   = dialyzer
TYPER      = typer
PYTHON     = python
PY_INSTALL = $(PYTHONENVS) $(PYTHON) setup.py install --root=$(DESTDIR)/ --prefix=$(prefix)

EHOME = master
WWW   = master/www
EBIN  = master/ebin
ESRC  = master/src
EDEP  = master/deps
ETEST = master/test

ELIBS    = $(ESRC) $(ESRC)/ddfs
ESOURCES = $(foreach lib,$(ELIBS),$(wildcard $(lib)/*.erl))
EOBJECTS = $(addprefix $(EBIN)/,$(notdir $(ESOURCES:.erl=.beam) disco.app))
ETARGETS = $(addprefix $(TARGETLIB)/,$(EOBJECTS))
EAPPCFG  = $(EHOME)/app.config

EDEPS         = $(foreach dep,$(wildcard $(EDEP)/*),$(notdir $(dep))/ebin)
ETARGETDEPS   = $(addprefix $(TARGETLIB)/$(EDEP)/,$(EDEPS))
ETARGETAPPCFG = $(TARGETLIB)/$(EAPPCFG)

ETESTSOURCES = $(wildcard $(ETEST)/*.erl)
ETESTOBJECTS = $(ETESTSOURCES:.erl=.beam)

EPLT  = .dialyzer_plt

# used to override default installation settings
UNAME = $(shell uname)
-include mk/mk.$(UNAME)
# default settings to be generated for installation
DISCO_HOME ?= $(TARGETLIB)
DISCO_ROOT ?= $(TARGETSRV)
DISCO_LOG_DIR ?= $(TARGETSRV)/log
DISCO_RUN_DIR ?= $(DISCO_ROOT)/run

# installation utilities
RE_VERSION = sed -e s/%DISCO_VERSION%/$(DISCO_VERSION)/
RE_INSTALL_LOG_DIR = sed -e s@%DISCO_LOG_DIR%@$(DISCO_LOG_DIR)@
RE_LOCAL_LOG_DIR = sed -e s@%DISCO_LOG_DIR%@root/log@

.PHONY: all master clean distclean doc docclean doctest
.PHONY: install \
	install-master \
	install-core \
	install-node \
	install-discodb \
	install-discodex \
	install-examples \
	install-tests \
	uninstall
.PHONY: test dialyzer typer

all: master

master: $(EAPPCFG) $(ESRC)/disco.app.src Makefile
	cd master && $(REBAR) get-deps && $(REBAR) compile

$(ESRC)/disco.app.src: $(ESRC)/disco.app.src.src
	- $(RE_VERSION) $< > $@

$(EAPPCFG): $(EAPPCFG).src
	$(RE_LOCAL_LOG_DIR) $< > $(EAPPCFG)

clean:
	- cd master && $(REBAR) clean
	- rm -Rf $(ESRC)/disco.app.src $(EBIN) $(ETESTOBJECTS)
	- rm -Rf lib/build lib/disco.egg-info
	- rm -Rf doc/.build

distclean: clean
	- rm -Rf $(EPLT)

doc:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) html)

docclean:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) clean)

doctest:
	(cd doc && $(MAKE) SPHINXOPTS=$(SPHINXOPTS) doctest)

install: install-core install-master install-node

install-core:
	(cd lib && $(PY_INSTALL))

install-discodb:
	(cd contrib/discodb && $(PY_INSTALL))

install-discodex:
	(cd contrib/discodex && $(PY_INSTALL))

install-examples: $(TARGETLIB)/examples

install-master: master \
	$(TARGETBIN)/disco $(TARGETBIN)/ddfs \
	$(TARGETLIB)/$(WWW) \
	$(TARGETCFG)/settings.py \
	$(TARGETSRV)/ddfs \
	$(ETARGETAPPCFG)

install-node: master $(ETARGETS) $(ETARGETDEPS)

install-tests: $(TARGETLIB)/ext $(TARGETLIB)/tests

uninstall:
	- rm -f  $(TARGETBIN)/disco $(TARGETBIN)/ddfs
	- rm -Rf $(TARGETCFG) $(TARGETLIB) $(TARGETSRV)

test: master $(ETESTOBJECTS)
	$(ERL) -noshell -pa $(ETEST) -s master_tests main -s init stop

dialyzer: EOPT = -W +debug_info
dialyzer: $(EPLT)
	$(DIALYZER) --get_warnings -Wunmatched_returns -Werror_handling --plt $(EPLT) --src -r $(ESRC)

typer: $(EPLT)
	$(TYPER) --plt $(EPLT) -r $(ESRC)

$(ETEST)/%.beam: $(ETEST)/%.erl
	$(ERLC) $(EOPT) -o $(dir $@) $<

$(EPLT):
	$(DIALYZER) --build_plt --output_plt $(EPLT) \
		    --apps stdlib kernel erts mnesia compiler crypto inets xmerl ssl syntax_tools

$(TARGETBIN) $(TARGETLIB):
	$(INSTALL) -d $@

$(TARGETBIN)/%: bin/% | $(TARGETBIN)
	$(INSTALL_PROGRAM) $< $@

$(TARGETCFG):
	$(INSTALL) -d $(TARGETCFG)

$(TARGETCFG)/settings.py: conf/gen.settings | $(TARGETCFG)
	(DISCO_HOME=$(DISCO_HOME) \
	 DISCO_ROOT=$(DISCO_ROOT) \
	 DISCO_USER=$(DISCO_USER) \
	 DISCO_PID_DIR=$(DISCO_PID_DIR) \
	 DISCO_RUN_DIR=$(DISCO_RUN_DIR) \
	 conf/gen.settings > $@ && chmod 644 $@)

$(ETARGETAPPCFG): $(EAPPCFG).src
	$(INSTALL) -d `$(DIRNAME) $@`
	- $(RE_INSTALL_LOG_DIR) $< > $@

$(TARGETLIB)/$(EBIN):
	$(INSTALL) -d $@

$(TARGETLIB)/$(EBIN)/%: $(EBIN)/% | $(TARGETLIB)/$(EBIN)
	$(INSTALL_DATA) $< $@

 $(TARGETLIB)/$(EDEP):
	$(INSTALL) -d $@

$(TARGETLIB)/$(EDEP)/%: $(EDEP)/% | $(TARGETLIB)/$(EDEP)
	$(INSTALL) -d `$(DIRNAME) $@`
	$(INSTALL_TREE) $< `$(DIRNAME) $@`

$(TARGETLIB)/%: % | $(TARGETLIB)
	$(INSTALL) -d `$(DIRNAME) $@`
	$(INSTALL_TREE) $< `$(DIRNAME) $@`

$(TARGETSRV)/ddfs:
	$(INSTALL) -d $(TARGETSRV)/ddfs
