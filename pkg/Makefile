export

INSTALL = /usr/bin/install -c

DEBIAN_TARGETS = debian/disco-master.init \
		 debian/disco-master.preinst debian/disco-master.prerm \
		 debian/disco-master.postinst debian/disco-node.postinst \
		 debian/disco-master.postrm debian/disco-node.postrm

.PHONY: clean debian debian-check debian-clean

clean:
	- rm -Rf build

debian/disco-master.init: debian/gen.disco-master.sh
	($< > $@ && chmod 755 $@)

debian/disco-master.preinst debian/disco-master.prerm: debian/gen.stop-master.sh
	($< > $@ && chmod 755 $@)

debian/disco-master.postinst debian/disco-node.postinst: debian/gen.create-disco-user.sh
	($< > $@ && chmod 755 $@)

debian/disco-master.postrm debian/disco-node.postrm: debian/gen.delete-disco-user.sh
	($< > $@ && chmod 755 $@)

debian: build/debian $(DEBIAN_TARGETS)
	dpkg-buildpackage -d
	mv ../*.{changes,dsc,deb,tar.gz} $<

debian-check: build/debian
	lintian $</*.deb

debian-clean: clean
	- rm -f  $(DEBIAN_TARGETS)
	- rm -Rf build/debian

build/debian/Packages.gz: build/debian
	(cd $(@D) && dpkg-scanpackages . /dev/null | gzip -9c > $(@F))

build/%:
	$(INSTALL) -d $@
