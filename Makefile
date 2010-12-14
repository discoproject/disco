
#OPT = -W +native +"{hipe, [o3]}"
OPT = -W
CC  = erlc +debug_info
ERL = erl

PYTHON = python
DIALYZER = dialyzer
TYPER = typer

ESRC = master/src
EWWW = master/www
EBIN = master/ebin
ETEST = master/tests

DESTDIR=/
PREFIX=/usr/local
SYSCONFDIR=/etc
BIN_DIR = $(PREFIX)/bin/
ERL_LIBDIR = /usr/lib/erlang

INSTALL_DIR = $(PREFIX)/lib/disco/
CONFIG_DIR = $(SYSCONFDIR)/disco/
DISCO_ROOT = $(DESTDIR)/srv/disco/

TARGETDIR = $(DESTDIR)/$(INSTALL_DIR)
TARGETBIN = $(DESTDIR)/$(BIN_DIR)
TARGETCFG = $(DESTDIR)/$(CONFIG_DIR)

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

include version.mk

SED_ARGS = -e s^%DISCO_VERSION%^$(DISCO_VERSION)^ \
           -e s^%DISCO_RELEASE%^$(DISCO_RELEASE)^

build: master

preprocess: master/ebin/disco.app $(HTML_TARGET) doc/conf.py

%: %.src version.mk
	sed $(SED_ARGS) $< > $@

master: preprocess $(EBIN)/ddfs $(EBIN)/mochiweb $(TARGET) $(MOCHI_TARGET) $(DDFS_TARGET)

clean:
	- rm -Rf master/ebin/*.beam master/ebin/disco.app
	- rm -Rf master/ebin/mochiweb/*.beam
	- rm -Rf master/ebin/ddfs/*.beam
	- rm -Rf master/tests/*.beam
	- rm -Rf lib/build
	- rm -Rf lib/disco.egg-info
	- rm -Rf node/build
	- rm -Rf node/disco_node.egg-info
	- rm -f $(HTML_TARGET)
	- rm -f doc/conf.py

install: install-master install-lib install-node install-root install-tests

install-ebin:
	install -d $(TARGETDIR)/ebin $(TARGETDIR)/ebin/ddfs $(TARGETDIR)/ebin/mochiweb
	install -m 0755 $(TARGET) $(TARGETDIR)/ebin
	install -m 0755 $(MOCHI_TARGET) $(TARGETDIR)/ebin/mochiweb
	install -m 0755 $(DDFS_TARGET) $(TARGETDIR)/ebin/ddfs

install-master: master install-ebin install-config install-bin
	install -m 0755 master/ebin/disco.app $(TARGETDIR)/ebin

	cp -r master/www $(TARGETDIR)
	chmod -R u=rwX,g=rX,o=rX $(TARGETDIR)/www

install-node: master install-ebin install-config install-bin
	install -m 0755 node/disco-worker $(TARGETBIN)

install-bin:
	install -d $(TARGETBIN)
	install -m 0755 bin/discocli.py $(TARGETBIN)/disco
	install -m 0755 bin/ddfscli.py $(TARGETBIN)/ddfs

install-lib:
	(cd lib; $(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))

install-discodb:
	(cd contrib/discodb; \
	$(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))

install-discodex:
	(cd contrib/discodex; \
	$(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))

install-config:
	install -d $(TARGETCFG)

	$(if $(wildcard $(TARGETCFG)/settings.py),\
		$(info disco config already exists, skipping),\
		(DESTDIR=$(DESTDIR) \
		 TARGETDIR=$(TARGETDIR) \
		 TARGETBIN=$(TARGETBIN) \
		 DISCO_ROOT=$(DISCO_ROOT) \
		 conf/gen.settings.sys-$(UNAME) > $(TARGETCFG)/settings.py || \
		 rm $(TARGETCFG)/settings.py; \
                 chmod 644  $(TARGETCFG)/settings.py))

install-root:
	install -d $(DISCO_ROOT)ddfs

install-examples:
	cp -r examples $(TARGETDIR)

install-ext:
	cp -r ext $(TARGETDIR)

install-tests: install-ext
	cp -r tests $(TARGETDIR)

$(EBIN)/mochiweb/%.beam: $(ESRC)/mochiweb/%.erl
	$(CC) $(OPT) -o $(EBIN)/mochiweb/ $<

$(EBIN)/ddfs/%.beam: $(ESRC)/ddfs/%.erl
	$(CC) $(OPT) -o $(EBIN)/ddfs/ $<

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(CC) $(OPT) -o $(EBIN) $<

$(ETEST)/%.beam: $(ETEST)/%.erl
	$(CC) $(OPT) -o $(ETEST) $<

$(EBIN)/ddfs:
	- mkdir $(EBIN)/ddfs

$(EBIN)/mochiweb:
	- mkdir $(EBIN)/mochiweb

.PHONY: preprocess master dialyzer typer realclean

DIALYZER_PLT = master/.dialyzer_plt

$(DIALYZER_PLT):
	$(DIALYZER) --build_plt --output_plt $(DIALYZER_PLT) -r \
		$(ERL_LIBDIR)/lib/stdlib*/ebin $(ERL_LIBDIR)/lib/kernel*/ebin \
		$(ERL_LIBDIR)/lib/erts*/ebin $(ERL_LIBDIR)/lib/mnesia*/ebin \
		$(ERL_LIBDIR)/lib/compiler*/ebin $(ERL_LIBDIR)/lib/crypto*/ebin \
		$(ERL_LIBDIR)/lib/inets*/ebin $(ERL_LIBDIR)/lib/xmerl*/ebin

dialyzer: $(DIALYZER_PLT)
	$(DIALYZER) --get_warnings --plt $(DIALYZER_PLT) -c --src -r $(ESRC)

typer:
	$(TYPER) --plt $(DIALYZER_PLT) -r $(ESRC)

realclean: clean
	-rm -f $(DIALYZER_PLT)

master-tests: $(TEST_TARGET) master
	$(ERL) -noshell -pa $(ETEST) -pa $(EBIN) -pa $(EBIN)/ddfs -pa $(EBIN)/mochiweb -s master_tests main -s init stop

