package main

import (
	"r7data.com/dfs"
	log "r7data.com/logging"
	"r7data.com/unfs"
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	metaUrl := flag.String("meta", "127.0.0.1", "Meta URL")
	dataUrl := flag.String("data", "127.0.0.1", "Data URL")
	blockSize := flag.Int("blocksize", 0, "Set block size")
	flag.Parse()

	err := log.InitLogger()
	if err != nil {
		panic(err)
	}
	//Handle Ctrl-C so we can quit nicely
	cc := make(chan os.Signal, 1)
	signal.Notify(cc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		<-cc
		shutDown()
	}()
	log.Infof("dfs service start...")
	fs, err := dfs.New(*metaUrl, *dataUrl, *blockSize)
	if err != nil {
		log.Errorf("Failed to start DFS service, err=%v", err)
	} else {
		unfs.Start(fs)
	}
	unfs.ShutDown()
	log.Infof("dfs service exit.")
}

func shutDown() {
	unfs.ShutDown()
	log.Infof("dfs service shutdown.")
	os.Exit(1)
}
