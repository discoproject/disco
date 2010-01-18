
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

UNAME = $(shell uname)

build: master config

master: $(TARGET)

clean:
	- rm -Rf master/ebin/*.beam
	- rm -Rf pydisco/build
	- rm -Rf pydisco/disco.egg-info
	- rm -Rf node/build
	- rm -Rf node/disco_node.egg-info

install: install-master install-pydisco install-node

install-master: install-config install-bin master
	install -d $(TARGETDIR)/ebin
	install -m 0755 $(TARGET) $(TARGETDIR)/ebin
	install -m 0755 master/ebin/disco.app $(TARGETDIR)/ebin
	install -m 0755 master/make-lighttpd-proxyconf.py $(TARGETDIR)

	cp -r master/www $(TARGETDIR)
	chmod -R u=rwX,g=rX,o=rX $(TARGETDIR)/www

	$(if $(wildcard $(TARGETCFG)/lighttpd-master.conf),\
		$(info lighttpd-master config already exists, skipping),\
		install -m 0644 conf/lighttpd-master.conf $(TARGETCFG))

install-node: install-config install-bin master
	install -d $(TARGETDIR)/ebin
	install -m 0755 $(TARGET) $(TARGETDIR)/ebin
	install -m 0755 node/disco-worker $(TARGETBIN)

	$(if $(wildcard $(TARGETCFG)/lighttpd-worker.conf),\
		$(info lighttpd-worker config already exists, skipping),\
		install -m 0644 conf/lighttpd-worker.conf $(TARGETCFG))

install-bin:
	install -d $(TARGETBIN)
	install -m 0755 bin/disco.py $(TARGETBIN)/disco

install-pydisco:
	(cd pydisco; $(PYTHON) setup.py install --root=$(DESTDIR) --prefix=$(PREFIX))

install-config:
	install -d $(TARGETCFG)

	$(if $(wildcard $(TARGETCFG)/settings.py),\
		$(info disco config already exists, skipping),\
		install -m 0644 conf/settings.py.sys-$(UNAME) $(TARGETCFG)/settings.py)

$(EBIN)/%.beam: $(ESRC)/%.erl
	$(CC) $(OPT) -o $(EBIN) $<

config:
	$(if $(wildcard conf/settings.py),\
	        $(info not overwriting existing conf/settings.py),\
                 cp conf/settings.py.template conf/settings.py)
