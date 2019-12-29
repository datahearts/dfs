# DFS Server


## Preinstall
  Configure the Couchbase cluster to create volume1.meta and volume1.data buckets.
  Create a mount dir: mkdir /ddbs-dfs

## Run
  /opt/ddbs/dfs/bin/dfs-daemon.sh start
  /opt/ddbs/dfs/bin/dfs-mount.sh

## Stop
  umount /ddbs-dfs
  /opt/ddbs/dfs/bin/dfs-daemon.sh stop
