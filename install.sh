#!/bin/bash
#

DEPENDENCY_DEB="rpcbind nfs-common openssl"
DEPENDENCY_RPM="rpcbind nfs-utils openssl"


# Refresh package manager and install package dependencies
installDependencies() {
    formatMsg "\nInstalling package dependencies ($DEPENDENCY)"
    case $OS in
    redhat)
        #yum -q clean metadata
        for i in $DEPENDENCY; do
            echo Checking $i ...
            yum -q -y install $i
        done
        ;;
    ubuntu)
        apt-get install -q -y $DEPENDENCY
        ;;
    esac
    if [ $? -ne 0 ]; then
        messenger $ERROR "Unable to install dependencies ($DEPENDENCY). Ensure that a core OS repo is enabled and retry $CMD"
    fi
    /etc/init.d/rpcbind start
}

# Determine current OS
checkOS() {
    if [ -z "$HOSTNAME" ]; then
        messenger $ERROR "Hostname not configured properly. Correct the problem and re-run $CMD"
    fi
    if [ -f /etc/redhat-release ]; then
        OS=redhat
    elif uname -a | grep -q -i "ubuntu"; then
        OS=ubuntu
    else
        messenger $ERROR "$CMD must be run on RedHat, CentOS, or Ubuntu Linux"
    fi
    if [ $(uname -p) != "x86_64" ]; then
        messenger $ERROR "$CMD must be run on a 64 bit version of Linux"
    fi
    case $OS in 
    redhat)
        DEPENDENCY=$DEPENDENCY_RPM
        ;;
    ubuntu)
        DEPENDENCY=$DEPENDENCY_DEB
        ;;
    esac
}

# check NTP and iptables
checkConfig() {
    case $OS in
      redhat)
        chkconfig ntpd
        /etc/init.d/iptables stop
        chkconfig iptables off
        ;;
      ubuntu)
        sudo ufw disable
        ;;
    esac
}

#Disable native nfs service when ddbs-dfs deployed
disableNfs() {
    formatMsg "\nDisable nfs service if any...\c"
    case $OS in
    redhat)
        chkconfig nfs off > /dev/null 2>&1
        chkconfig nfs --del > /dev/null 2>&1

        chkconfig nfslock off > /dev/null 2>&1
        chkconfig nfslock --del > /dev/null 2>&1
        ;;
    ubuntu)
        
        ;;
    esac
}

#Enable native nfs when ddbs-dfs removed
enableNfs() {
    formatMsg "\nEnable nfs service if any...\c"
    chkconfig nfs --add > /dev/null 2>&1
    chkconfig nfs on > /dev/null 2>&1
    chkconfig nfslock --add > /dev/null 2>&1
    chkconfig nfslock on > /dev/null 2>&1
}

disableNfs
checkOS
checkConfig
installDependencies

 DFS_DIR="/opt/ddbs/dfs"
mkdir -p $DFS_DIR
cp -rf conf $DFS_DIR
cp -rf bin $DFS_DIR
cp sbin/* $DFS_DIR/bin

echo Done.
