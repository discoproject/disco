
#OPT = -W +native +"{hipe, [o3]}"
OPT = -W
CC  = erlc

PYTHON = python2.4
ESRC = master/src
EBIN = master/ebin

BIN_DIR = /usr/bin/
INSTALL_DIR = /usr/lib/disco/
CONFIG_DIR = /etc/disco/

#ifndef NODEDEST
#	NODEDEST = $(DESTDIR)
#endif
#ifndef PYDISCODEST
#	PYDISCODEST = $(DESTDIR)
#endif
#
#NODEDEST = $(shell cd $(NODEDEST); pwd)
#PYDISCODEST = $(shell cd $(PYDISCODEST); pwd)

TARGETDIR = $(DESTDIR)/$(INSTALL_DIR)
TARGETBIN = $(DESTDIR)/$(BIN_DIR)
TARGETCFG = $(DESTDIR)/$(CONFIG_DIR)

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
 
install-master: install-config master
	install -d $(TARGETDIR)/ebin
	install -d $(TARGETBIN)
	cp $(TARGET) $(APP) $(TARGETDIR)/ebin
	cp $(BOOT) $(TARGETDIR)
	cp -r master/www $(TARGETDIR)
	cp conf/lighttpd-master.conf $(TARGETCFG)
	cp master/disco-master $(TARGETBIN)

install-node: install-config
	(cd node; $(PYTHON) setup.py install --root=$(DESTDIR))
	cp conf/lighttpd-node.conf $(TARGETCFG)

install-pydisco:
	(cd pydisco; $(PYTHON) setup.py install --root=$(DESTDIR))

install-config:
	install -d $(TARGETCFG)
	cp conf/disco.conf.example $(TARGETCFG)/disco.conf

$(APP): $(BOOT)

$(BOOT):
	(cd master; erl -pa ebin -noshell -run make_boot write_scripts)

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(CC) $(OPT) -o $(EBIN) $<

