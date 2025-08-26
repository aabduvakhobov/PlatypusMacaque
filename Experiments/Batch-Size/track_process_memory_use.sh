#!/bin/bash

PROC=modelardbd
DATASET=$1
ERROR_BOUND=$2
BATCHSIZE=$3
LOGFILE="$DATASET-$ERROR_BOUND-$BATCHSIZE-mem_usage.log"

if [ $# -ne 3 ]; 
then
    echo "Usage: script.sh dataset error_bound batch_size"
    exit 0
fi

echo "Timestamp   PID   RSS(KB)   VSZ(KB)   %MEM   CMD" > "$LOGFILE"

while pgrep -f "$PROC" > /dev/null; do
  ps -C "$PROC" -o pid,rss,vsz,%mem,cmd --no-headers | \
    awk -v date="$(date '+%Y-%m-%d %H:%M:%S')" \
        '{printf "%s   %s   %s   %s   %s   %s\n", date, $1, $2, $3, $4, $5}' \
        >> "$LOGFILE"
  sleep 1
done

echo "Processes [$PROC] finished. Log saved to $LOGFILE"