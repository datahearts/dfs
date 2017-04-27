#!/bin/bash
#
CURRENT_DIR=`which $0`
BUILD_DIR=`dirname $CURRENT_DIR`

if [ "$1" = "update" ]; then
  rm -rf src/github.com/couchbase/*
  ./fetch-manifest.rb  manifest/2.6.xml
fi

GOPATH=$BUILD_DIR
go install r7data.com/dfs-service

cd ..
tar -czf tarot-dfs.tar.gz ./dfs/bin ./dfs/conf ./dfs/sbin ./dfs/install.sh