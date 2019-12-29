package unfs

/*
#include <stdio.h>
#include "unfs3/daemon.h"
#include "unfs3/daemon.c"
*/
import "C"
import (
	log "datahearts.com/logging"
	"datahearts.com/minfs"
)

var ns minfs.MinFS = nil //filesystem being shared

func Start(fs minfs.MinFS) {
	log.Infof("Starting NFS service...")
	ns = fs
	C.start()
}

func ShutDown() {
	if ns != nil {
		ns.Close()
		ns = nil
	}
	log.Infof("NFS shutdown.")
}
