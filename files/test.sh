#!/bin/bash
set -xe

INTERPRETER=$1
[ -z "$INTERPRETER" ] && INTERPRETER="python"

STATSLOG=~/src/mbstats.testlogs/stats.log
COMMON_OPTS=" stats.parser.py -f $STATSLOG "
CMD="$INTERPRETER $COMMON_OPTS"
$CMD -m 1000 --deletedatabase --startover;
$CMD -m 300000;
for i in $(seq 1 10); do
	$CMD -m 70000;
done
$CMD -m 2000000;
