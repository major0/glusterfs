#!/bin/sh

OPERATINGSYSTEM='fedora'
AUTOSTART='false'
CLEANUP_ON_EXIT='false'
PROVISION='false'
VERBOSE='false'
VM_OP='test'

info() { echo "INFO: $(date -u): $*" | tee -a "tests/vagrant-${BRANCHNAME}/run-tests.log"; }
err() { echo "ERROR: $*" >&2; }
die() { err "$*"; exit 1; }
vcmd()
{
	: "vcmd($*)"
	if ${VERBOSE}; then
		(cd "tests/vagrant-${BRANCHNAME}"; vagrant "$@")
	else
		(cd "tests/vagrant-${BRANCHNAME}"; vagrant "$@") >> "tests/vagrant-${BRANCHNAME}/run-tests.log" 2>&1
	fi
}
vssh() { : "vssh($*)"; vcmd ssh -c "cd /home/vagrant/glusterfs ; $*" -- -t; }
vsudo() { : "vsudo($*)"; vssh "sudo $*"; }

destroy_vm_and_exit()
{
	# FIXME captive interface... after the user explicitly asked for this?
	echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!CAUTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	echo "This will destroy VM and delete tests/vagrant-${BRANCHNAME} dir"
	printf '\nDo you want to continue? [y/n]: '
	while read LINE; do
		case "${LINE}" in
		([Yy]*) break;;
		([Nn]*) exit 0;;
		(*) printf 'Do you want to continue? [y/n]: '; continue;;
		esac
	done
	test -d "tests/vagrant-${BRANCHNAME}" || die "invalid directory 'tests/vagrant-${BRANCHNAME}'"
	vcmd destroy -f
	rm -rf "tests/vagrant-${BRANCHNAME}"
	exit 0
}


##
# Argument handling
eval set -- "$(getopt \
              --options a \
              --long autostart,os:,destroy-now,destroy-after-test,verbose,ssh,provision \
              -n 'run-tests-in-vagrant.sh' \
              --  "$@")"
while test "$#" -gt '0'; do
	case "${1}" in
        (-a|--autostart)	AUTOSTART='true';;
        (--destroy-after-test)	CLEANUP_ON_EXIT='true';;
        (--destroy-now)		VM_OP='destroy';;
        (--ssh)			VM_OP='ssh';;
	(--provision)		PROVISION='true';;
        (--os)			OPERATINGSYSTEM="${2}"; shift;;
        (--verbose)		VERBOSE='true';;
        (--)			shift ; break ;;
        (*)			break;;
        esac
	shift
done

##
# Check environment for dependencies
test -d 'tests/vagrant' || die "directory missing 'tests/vagrant'"
vagrant -v > /dev/null 2>&1 || die 'Vagrant not installed'
ansible --version > /dev/null 2>&1 || die 'Ansible not installed'
git --help > /dev/null 2>&1 || die 'Git not installed'

##
# Check our OS
case "${OPERATINGSYSTEM}" in
(fedora|centos6) ;;
(*)	die "unsupported operating system '${OPERATINGSYSTEM}'";;
esac
export OPERATINGSYSTEM

# We have one vm per git branch, query git branch
BRANCHNAME="$(git rev-parse --abbrev-ref HEAD)"
test "$?" -eq 0 || die 'Could not get branch name from git'

# What operation are we performing on the VM.
case "${VM_OP}" in
(destroy)
	destroy_vm_and_exit;;

(ssh)	# Can't use vcmd for this
	# FIXME does the VM even exist?
	cd "tests/vagrant-${BRANCHNAME}" || die 'failed to change directory'
	exec vagrant ssh ;;
(*)	;;# Default operation (make && test)
esac

# Make our test location
if ! test -d "tests/vagrant-${BRANCHNAME}"; then
	echo "Copying tests/vagrant dir to tests/vagrant-${BRANCHNAME} ..."
	cp -R 'tests/vagrant' "tests/vagrant-${BRANCHNAME}" || exit 1
fi
touch "tests/vagrant-${BRANCHNAME}/run-tests.log"
# note: info() can now be used

if ${PROVISION}; then
	info "Doing vagrant provision..."
	vcmd provision || die 'vagrant provision failed'
else
	info "Doing vagrant up...."
	vcmd up || die 'vagrant up failed'
fi

# FIXME do we really want to enable autostart of test VMs?
! ${autostart} || virsh autostart "${BRANCHNAME}_vagrant-testVM"

##
# Copy the source code
info "Copying source code from host machine to VM...."
SRCDIR="${PWD}"
# Careful, can't use vcmd here..
(cd "tests/vagrant-${BRANCHNAME}"
 vagrant ssh-config > ssh_config
 rsync -rpl -e 'ssh -F ssh_config' "${SRCDIR}/." 'vagrant-testVM:/home/vagrant/glusterfs/')
test "$?" -eq 0 || die 'Copy failed'

##
# Compile
info "Source compile and install Gluster...."
vssh 'make clean' || : # Make might not even exist at this point...
vssh './autogen.sh' || die 'autogen.sh failed'

# GCC on fedora complains about uninitialized variables and
custom_cflags='-g -O0 -Werror -Wall'
case "${os}" in
# GCC on centos6 does not understand don't warn on uninitialized variables flag.
(centos6);;

# Increase checks
(*) custom_cflags="${custom_cflags} -Wno-error=cpp -Wno-error=maybe-uninitialized";;
esac

vssh "CFLAGS='${custom_cflags}' \
	./configure \
	--prefix=/usr \
	--exec-prefix=/usr \
	--bindir=/usr/bin \
	--sbindir=/usr/sbin \
	--sysconfdir=/etc \
	--datadir=/usr/share \
	--includedir=/usr/include \
	--libdir=/usr/lib64 \
	--libexecdir=/usr/libexec \
	--localstatedir=/var \
	--sharedstatedir=/var/lib \
	--mandir=/usr/share/man \
	--infodir=/usr/share/info \
	--libdir=/usr/lib64 \
	--enable-debug"
test "$?" -eq 0 || die 'failed to configure source'
# FIXME make -j seems excessive...
vssh 'make -j' || die 'compile failed'
vsudo 'make install' || die 'install failed'

##
# Run the tests
vsudo './run-tests.sh' || die 'run-tests.sh failed'

# FIXME trap 0 cleanup?
! ${CLEANUP_ON_EXIT} || destroy_vm_and_exit
