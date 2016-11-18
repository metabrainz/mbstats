#!/bin/bash
set -xe

INTERPRETER=$1
[ -z "$INTERPRETER" ] && INTERPRETER="python"

STATSLOG_SOURCE=~/src/mbstats.testlogs/stats.log
NOW=$(date -u +'%FT%TZ')
TESTTMPDIR=$(mktemp -d -t tmp.XXXXXXXXXX)
function finish {
  rm -rf "$TESTTMPDIR"
}
trap finish EXIT

STATSLOG=$TESTTMPDIR/stats.log

head -5000000 $STATSLOG_SOURCE > $STATSLOG

COMMON_OPTS=" stats.parser.py -f $STATSLOG -w $TESTTMPDIR -n $NOW"
CMD="time $INTERPRETER $COMMON_OPTS"
$CMD -m 1000 --deletedatabase --startover;
$CMD -m 300000;
for i in $(seq 1 10); do
	$CMD -m 70000;
done
$CMD -m 2000000;
