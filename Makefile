
#OPT = -W +native +"{hipe, [o3]}"
OPT = -W
CC  = erlc

PYTHON = python2.4
ESRC = master/src
EBIN = master/ebin

ifndef NODEDEST
	NODEDEST = $(DESTDIR)
endif
ifndef PYDISCODEST
	PYDISCODEST = $(DESTDIR)
endif

NODEDEST = $(shell cd $(NODEDEST); pwd)
PYDISCODEST = $(shell cd $(PYDISCODEST); pwd)

BIN_DIR = /usr/bin/
INSTALL_DIR = /usr/lib/disco/
TARGETDIR = $(DESTDIR)/$(INSTALL_DIR)
TARGETBIN = $(DESTDIR)/$(BIN_DIR)

SRC = $(wildcard $(ESRC)/*.erl)
TARGET = $(addsuffix .beam, $(basename \
             $(addprefix $(EBIN)/, $(notdir $(SRC)))))
BOOT = master/disco.boot
APP = master/ebin/disco.app

build: master

master: $(TARGET) $(APP)

clean:
	- rm $(TARGET) $(BOOT)
	- rm -Rf master/disco.rel master/disco.script 
	- rm -Rf pydisco/build
	- rm -Rf pydisco/disco.egg-info
	- rm -Rf node/build
	- rm -Rf node/disco_node.egg-info

install: install-master install-pydisco install-node
 
install-master: master
	install -d $(TARGETDIR)/ebin
	install -d $(TARGETBIN)
	cp $(TARGET) $(APP) $(TARGETDIR)/ebin
	cp $(BOOT) $(TARGETDIR)
	cp master/disco-master $(TARGETBIN)

install-node:
	(cd node; $(PYTHON) setup.py install --root=$(NODEDEST))

install-pydisco:
	(cd pydisco; $(PYTHON) setup.py install --root=$(PYDISCODEST))

$(APP): $(BOOT)

$(BOOT):
	(cd master; erl -pa ebin -noshell -run make_boot write_scripts)

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(CC) $(OPT) -o $(EBIN) $<

