
#OPT = -W +native +"{hipe, [o3]}"
OPT = -W
CC  = erlc
ERL = erl

PYTHON = python
DIALYZER = dialyzer
TYPER = typer

ESRC = master/src
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

UNAME = $(shell uname)

build: master

master: $(EBIN)/ddfs $(EBIN)/mochiweb $(TARGET) $(MOCHI_TARGET) $(DDFS_TARGET)

clean:
	- rm -Rf master/ebin/*.beam
	- rm -Rf master/ebin/mochiweb/*.beam
	- rm -Rf master/ebin/ddfs/*.beam
	- rm -Rf master/tests/*.beam
	- rm -Rf lib/build
	- rm -Rf lib/disco.egg-info
	- rm -Rf node/build
	- rm -Rf node/disco_node.egg-info

install: install-master install-lib install-node install-root install-tests

install-ebin:
	install -d $(TARGETDIR)/ebin $(TARGETDIR)/ebin/ddfs $(TARGETDIR)/ebin/mochiweb
	install -m 0755 $(TARGET) $(TARGETDIR)/ebin
	install -m 0755 $(MOCHI_TARGET) $(TARGETDIR)/ebin/mochiweb
	install -m 0755 $(DDFS_TARGET) $(TARGETDIR)/ebin/ddfs

install-master: master install-config install-bin
	install -m 0755 master/ebin/disco.app $(TARGETDIR)/ebin

	cp -r master/www $(TARGETDIR)
	chmod -R u=rwX,g=rX,o=rX $(TARGETDIR)/www

install-node: master install-ebin
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

.PHONY: dialyzer typer realclean

DIALYZER_PLT = master/.dialyzer_plt

$(DIALYZER_PLT):
	$(DIALYZER) --build_plt --output_plt $(DIALYZER_PLT) -r \
		$(ERL_LIBDIR)/lib/stdlib*/ebin $(ERL_LIBDIR)/lib/kernel*/ebin \
		$(ERL_LIBDIR)/lib/erts*/ebin $(ERL_LIBDIR)/lib/mnesia*/ebin \
		$(ERL_LIBDIR)/lib/compiler*/ebin $(ERL_LIBDIR)/lib/crypto*/ebin \
		$(ERL_LIBDIR)/lib/inets*/ebin $(ERL_LIBDIR)/lib/xmerl*/ebin

dialyzer: $(DIALYZER_PLT)
	$(DIALYZER) --plt $(DIALYZER_PLT) -c --src -r $(ESRC)

typer:
	$(TYPER) --plt $(DIALYZER_PLT) -r $(ESRC)

realclean: clean
	-rm -f $(DIALYZER_PLT)

master-tests: $(TEST_TARGET)
	$(ERL) -noshell -pa $(ETEST) -pa $(EBIN) -pa $(EBIN)/ddfs -s master_tests main -s init stop

