
#OPT = -W +native +"{hipe, [o3]}"
OPT = -W
CC  = erlc

PYTHON = python
ESRC = master/src
EBIN = master/ebin

PREFIX=/usr
SYSCONFDIR=/etc
BIN_DIR = $(PREFIX)/bin/
INSTALL_DIR = $(PREFIX)/lib/disco/
CONFIG_DIR = $(SYSCONFDIR)/disco/

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
	cp $(BOOT) $(TARGETDIR)
	cp $(APP) $(TARGETDIR)/ebin
	cp master/make-lighttpd-proxyconf.py $(TARGETDIR)
	cp -r master/www $(TARGETDIR)
	cp conf/lighttpd-master.conf $(TARGETCFG)
	cp master/disco-master $(TARGETBIN)

install-node: install-config master
	install -d $(TARGETDIR)/ebin
	cp $(TARGET) $(TARGETDIR)/ebin
	(cd node; $(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))
	cp conf/lighttpd-node.conf $(TARGETCFG)

install-pydisco:
	(cd pydisco; $(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))

install-config:
	install -d $(TARGETCFG)
	cp conf/disco.conf.example $(TARGETCFG)/disco.conf

$(APP): $(BOOT)

$(BOOT):
	(cd master; erl -pa ebin -noshell -run make_boot write_scripts)

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(CC) $(OPT) -o $(EBIN) $<

