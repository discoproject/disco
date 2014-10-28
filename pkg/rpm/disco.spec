%define __python python
%define _unpackaged_files_terminate_build 0
Summary:  An open-source mapreduce framework.
Name: disco
Version: 0.5.4
Release: 1%{?dist}
License: BSD
Group: System Environment/Daemon
URL: http://www.discoproject.org
Source0: disco.tar.gz
BuildRoot: %{_tmppath}/disco-%{version}-root
Vendor: Disco Authors
Packager: Shayan Pooya <shayan@liveve.org>

%description
Disco is a lightweight, open-source framework for distributed computing based
on the MapReduce paradigm.

Disco is powerful and easy to use, thanks to Python. Disco distributes and
replicates your data, and schedules your jobs efficiently. Disco even includes
the tools you need to index billions of data points and query them in
real-time.

%package master
Summary: Disco Master
Group: System Environment/Daemon
Requires: erlang
Requires: python-%{name} == %{version}-%{release}
BuildRequires: erlang

%description master
This package contains the required files to run the disco master

%package node
Summary: Disco Node
Group: System Environment/Daemon
Requires: erlang
Requires: python-%{name} == %{version}-%{release}
BuildRequires: erlang

%description node
This package contains the required files to run the disco node

%package -n python-%{name}
Summary: Disco Python Libs
Group: Development/Languages
BuildRequires: python-devel, python-setuptools
Requires: python

%description -n python-%{name}
This package contains the disco python libraries for Python

%package cli
Summary: Disco CLI Utilities
Group: Development/Tools
Requires: python
Requires: python-%{name} = %{version}-%{release}

%description cli
This package contains the disco command-line tools ddfs and disco

%prep
%setup -n disco
%define prefix /usr

%build
%{__make}

%install
%{__make} install DESTDIR=$RPM_BUILD_ROOT
%{__make} install-node DESTDIR=$RPM_BUILD_ROOT
cd lib
%{__python} setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_PYTHON_FILES
cd ../
mkdir -p $RPM_BUILD_ROOT/%{prefix}/bin
cp bin/disco bin/ddfs $RPM_BUILD_ROOT/%{prefix}/bin

%clean
[ "%{buildroot}" != "/" ] && %{__rm} -rf %{buildroot}

%files master
%defattr(-,root,root)
%attr(0644,root,root) %config(noreplace) %{_sysconfdir}/disco/settings.py
%dir %{prefix}/*
%dir /etc/disco
%{prefix}/lib/*
%{prefix}/share/*
%{prefix}/var/*
%attr(0755,root,root) %{prefix}/bin/disco
%attr(0755,root,root) %{prefix}/bin/ddfs

%files node
%defattr(-,root,root)
%dir %{prefix}/*
%{prefix}/lib/*
%{prefix}/var/*

%files -n python-%{name} -f lib/INSTALLED_PYTHON_FILES
%defattr(-,root,root)

%files cli
%defattr(-,root,root)
%attr(0755,root,root) %{prefix}/bin/disco
%attr(0755,root,root) %{prefix}/bin/ddfs


%post master
if [ "$1" = 1 ]; then
/bin/ln -s %{prefix}/bin/disco /usr/bin/disco
/bin/ln -s %{prefix}/bin/ddfs /usr/bin/ddfs
fi
%postun master
if [ "$1" = 0 ]; then
       /bin/rm -f /usr/bin/disco
       /bin/rm -f /usr/bin/ddfs
fi

%post cli
if [ "$1" = 1 ]; then
        /bin/ln -s %{prefix}/bin/disco /usr/bin/disco
        /bin/ln -s %{prefix}/bin/ddfs /usr/bin/ddfs
fi
%postun cli
if [ "$1" = 0 ]; then
       /bin/rm -f /usr/bin/disco
       /bin/rm -f /usr/bin/ddfs
fi

%changelog

* Fri Aug 01 2014 Shayan Pooya <shayan@liveve.org> - 0.5.4-1
- Release version 0.5.4

* Fri Aug 01 2014 Shayan Pooya <shayan@liveve.org> - 0.5.3-1
- Release version 0.5.3

* Wed Jun 05 2014 Shayan Pooya <shayan@liveve.org> - 0.5.2-1
- Release version 0.5.2

* Wed Apr 16 2014 Shayan Pooya <shayan@liveve.org> - 0.5.1-1
- Release version 0.5.1

* Wed Apr 02 2014 Shayan Pooya <shayan@liveve.org> - 0.5.0-1
- Initial packaging
- Make python-disco a dependency of node and master subpackages
