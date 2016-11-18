#!/bin/bash
set -xe

INTERPRETER=$1
[ -z "$INTERPRETER" ] && INTERPRETER="python"

STATSLOG=~/src/mbstats.testlogs/stats.log
COMMON_OPTS=" stats.parser.py -f $STATSLOG "
CMD="$INTERPRETER $COMMON_OPTS"
$CMD -m 1000 --deletedatabase --startover;
$CMD -m 100000;
$CMD -m 1000000;
$CMD -m 2000000;
