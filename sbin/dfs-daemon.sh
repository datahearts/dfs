#!/usr/bin/env bash

USAGE="Usage: dfs-daemon.sh (start|stop|status|restart)"

# if no args specified, show usage
if [ $# -eq 0 ]; then
  echo $USAGE
  exit 1
fi

DAEMON="/opt/ddbs/dfs/bin/dfs-service"
SERVER="localhost"
COMMAND="$DAEMON -meta $SERVER -data $SERVER"
# get log directory
if [ "$DFS_LOG_DIR" = "" ]; then
  export DFS_LOG_DIR="/opt/ddbs/dfs/logs"
fi
# some variables
LOGFILE_PREFIX=ddbs-dfs
LOG=$DFS_LOG_DIR/$LOGFILE_PREFIX.log
OUTLOG=$DFS_LOG_DIR/$LOGFILE_PREFIX.out
PID=$DFS_LOG_DIR/dddbs-dfs.pid

dfs_rotate_log ()
{
    NUM=5;
    if [ -f "$LOG" ]; then # rotate logs
	  while [ $NUM -gt 1 ]; do
	    PREV=`expr $NUM - 1`
	    [ -f "$LOG.$PREV" ] && mv "$LOG.$PREV" "$LOG.$NUM"
	    NUM=$PREV
	  done
	  mv "$LOG" "$LOG.$NUM"
    fi
}

# test access permission
mkdir -p "$DFS_LOG_DIR"
touch $DFS_LOG_DIR/.dfs_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f $DFS_LOG_DIR/.dfs_test
fi

# Set default scheduling priority
if [ "$DFS_NICENESS" = "" ]; then
    export DFS_NICENESS=0
fi
start() {
    dfs_rotate_log "$LOG"
    #echo starting $COMMAND, logging to $LOG
    nohup nice -n $DFS_NICENESS $COMMAND >> "$OUTLOG" 2>&1 < /dev/null &
    NEWPID=$!
    echo $NEWPID > $PID
    echo "ddbs-dfs started!"
}

stop() {
    if [ -f $PID ]; then
      TARGET_PID=`cat $PID`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        #echo stopping dfs-server
        kill $TARGET_PID
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "dfs-server did not stop gracefully: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no dfs-server to stop
      fi
    else
      echo no dfs-server to stop
    fi
}

running() {
    if [ -f $PID ]; then
      if kill -0 `cat $PID` > /dev/null 2>&1; then
        echo dfs-server running as process `cat $PID`.
        return 0
      fi
    fi
    return 2
}
run() {
    if [ -f $PID ]; then
      if kill -0 `cat $PID` > /dev/null 2>&1; then
        echo dfs-server running as process `cat $PID`.  Stop it first.
        exit 1
      fi
    fi
    dfs_rotate_log "$LOG"
    echo starting dfs-server, logging to $LOG

    $COMMAND >> "$LOG" 2>&1 < /dev/null &
    NEWPID=$!
    echo $NEWPID > $PID
}
mkroot() {

    $DAEMON -mkroot=true >> "$LOG" 2>&1 < /dev/null &
}
case $1 in
  start)
    if [ -f $PID ]; then
      if kill -0 `cat $PID` > /dev/null 2>&1; then
        echo dfs-server running as process `cat $PID`.  Stop it first.
        exit 0
      fi
    fi
    echo -n $'Starting dfs-server\n'
    start
    echo
    ;;
  stop)
    echo -n $'Stopping dfs-server\n'
    stop
    echo
    ;;
  restart)
    echo -n $'Stopping dfs-server\n'
    stop
    echo
    echo -n $'Starting dfs-server\n'
    start
    echo
    ;;
  status)
    if running ; then
      echo -n $'dfs-server is running\n'
      exit 0
    else
      echo -n $'dfs-server is not running\n'
      exit 3
    fi
   ;;
  *)
    echo $USAGE
    exit 1
    ;;

esac

