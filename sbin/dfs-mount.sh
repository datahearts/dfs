#!/bin/bash
DFS_MOUNT_DIR=${DFS_MOUNT_DIR:-/ddbs-dfs}

mount -t nfs -o nolock,noacl,wsize=1048576,rsize=1048576 localhost:/ $DFS_MOUNT_DIR
