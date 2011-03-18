DISCO_VERSION = 0.3.2
DISCO_RELEASE = 0.3.2

# standard make installation variables
prefix      = /usr/local
exec_prefix = $(prefix)
bindir      = $(exec_prefix)/bin
sysconfdir  = $(prefix)/etc

ERL        = erl
ERLC       = erlc
EOPT       = -W
PYTHON     = python
DIALYZER   = dialyzer
TYPER      = typer
INSTALL    = /usr/bin/install -c
PY_INSTALL = $(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(prefix)

EBIN = master/ebin
ESRC = master/src
ETEST = master/tests
EWWW = master/www

ERL_LIBDIR = /usr/lib/erlang

DISCO_HOME = $(prefix)/lib/disco
DISCO_CONF = $(sysconfdir)/disco
DISCO_ROOT = /srv/disco

TARGETBIN = $(DESTDIR)$(bindir)
TARGETCFG = $(DESTDIR)$(DISCO_CONF)
TARGETDIR = $(DESTDIR)$(DISCO_HOME)
TARGETSRV = $(DESTDIR)$(DISCO_ROOT)

SRC = $(wildcard $(ESRC)/*.erl)
TARGET = $(addsuffix .beam, $(basename \
             $(addprefix $(EBIN)/, $(notdir $(SRC)))))


SRC2 = $(wildcard $(ESRC)/mochiweb/*.erl)
MOCHI_TARGET = $(addsuffix .beam, $(basename \
             $(addprefix $(EBIN)/mochiweb/, $(notdir $(SRC2)))))

SRC3 = $(wildcard $(ESRC)/ddfs/*.erl)
DDFS_TARGET = $(addsuffix .beam, $(basename \
             $(addprefix $(EBIN)/ddfs/, $(notdir $(SRC3)))))

TESTSRC = $(wildcard $(ETEST)/*.erl)
TEST_TARGET = $(addsuffix .beam, $(basename $(TESTSRC)))

HTMLSRC = $(wildcard $(EWWW)/*.html.src)
HTML_TARGET = $(basename $(HTMLSRC) .src)

UNAME = $(shell uname)

SED_ARGS = -e s^%DISCO_VERSION%^$(DISCO_VERSION)^ \
	   -e s^%DISCO_RELEASE%^$(DISCO_RELEASE)^

.PHONY: build preprocess master clean

build: master

preprocess: $(EBIN) $(EBIN)/disco.app $(HTML_TARGET) doc/conf.py

%: %.src
	- sed $(SED_ARGS) $< > $@

master: preprocess $(TARGET) $(MOCHI_TARGET) $(DDFS_TARGET)

clean:
	- rm -Rf $(EBIN)
	- rm -Rf master/tests/*.beam
	- rm -Rf lib/build
	- rm -Rf lib/disco.egg-info
	- rm -f  $(HTML_TARGET)
	- rm -f  doc/conf.py

.PHONY: install \
	install-master \
	install-ebin \
	install-bin \
	install-config \
	install-lib \
	install-discodb \
	install-discodex \
	install-home \
	install-root \
	install-examples \
	install-ext \
	install-tests
	install-www \

install: install-master install-lib install-root install-tests

install-master: master install-ebin install-bin install-config install-www

# see if we can combine all these
install-ebin:
	$(INSTALL) -d $(TARGETDIR)/ebin/ddfs $(TARGETDIR)/ebin/mochiweb
	$(INSTALL) $(TARGET) $(TARGETDIR)/ebin
	$(INSTALL) $(MOCHI_TARGET) $(TARGETDIR)/ebin/mochiweb
	$(INSTALL) $(DDFS_TARGET) $(TARGETDIR)/ebin/ddfs
	$(INSTALL) $(EBIN)/disco.app $(TARGETDIR)/ebin

install-bin:
	$(INSTALL) -d $(TARGETBIN)
	$(INSTALL) bin/discocli.py $(TARGETBIN)/disco
	$(INSTALL) bin/ddfscli.py $(TARGETBIN)/ddfs

install-config:
	$(INSTALL) -d $(TARGETCFG)
	$(if $(wildcard $(TARGETCFG)/settings.py),\
		$(info disco config already exists, skipping),\
		(TARGETDIR=$(TARGETDIR) \
		 TARGETSRV=$(TARGETSRV) \
		 conf/gen.settings.sys-$(UNAME) > $(TARGETCFG)/settings.py || \
		 rm $(TARGETCFG)/settings.py; \
                 chmod 644 $(TARGETCFG)/settings.py))

install-lib:
	(cd lib; $(PY_INSTALL))

install-discodb:
	(cd contrib/discodb; $(PY_INSTALL))

install-discodex:
	(cd contrib/discodex; $(PY_INSTALL))

install-home:
	$(INSTALL) -d $(TARGETDIR)

install-root:
	$(INSTALL) -d $(TARGETSRV)/ddfs

install-examples: install-home
	cp -r examples $(TARGETDIR)

install-ext: install-home
	cp -r ext $(TARGETDIR)

install-tests: install-home install-ext
	cp -r tests $(TARGETDIR)

install-www: install-home
	cp -r $(EWWW) $(TARGETDIR)

$(EBIN):
	mkdir -p $(EBIN)/ddfs $(EBIN)/mochiweb

$(EBIN)/disco.app: $(ESRC)/disco.app
	- sed $(SED_ARGS) $< > $@

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(ERLC) $(EOPT) -o $(dir $@) $<

$(ETEST)/%.beam: $(ETEST)/%.erl
	$(ERLC) $(EOPT) -o $(ETEST) $<

DIALYZER_PLT = master/.dialyzer_plt

$(DIALYZER_PLT):
	$(DIALYZER) --build_plt --output_plt $(DIALYZER_PLT) -r \
		$(ERL_LIBDIR)/lib/stdlib*/ebin $(ERL_LIBDIR)/lib/kernel*/ebin \
		$(ERL_LIBDIR)/lib/erts*/ebin $(ERL_LIBDIR)/lib/mnesia*/ebin \
		$(ERL_LIBDIR)/lib/compiler*/ebin $(ERL_LIBDIR)/lib/crypto*/ebin \
		$(ERL_LIBDIR)/lib/inets*/ebin $(ERL_LIBDIR)/lib/xmerl*/ebin

.PHONY: dialyzer typer realclean master-tests

dialyzer: $(DIALYZER_PLT)
	$(DIALYZER) --get_warnings --plt $(DIALYZER_PLT) -c --src -r $(ESRC)

typer:
	$(TYPER) --plt $(DIALYZER_PLT) -r $(ESRC)

realclean: clean
	- rm -f $(DIALYZER_PLT)

master-tests: $(TEST_TARGET) master
	$(ERL) -noshell -pa $(ETEST) -pa $(EBIN) -pa $(EBIN)/ddfs -pa $(EBIN)/mochiweb -s master_tests main -s init stop
