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

COMMON_OPTS=" stats.parser.py -f $STATSLOG -w $TESTTMPDIR -n $NOW --do-not-skip-to-end -l . "
CMD="$INTERPRETER $COMMON_OPTS"
$CMD -m 1000 --influx-drop-database --startover;
#test change of bucket duration, must fail (hence the !)
! $CMD -m 1000 --bucket-duration 30 && echo "Testing exit on change of bucket duration, SUCCESS"
#test change of lookback factor, must fail (hence the !)
! $CMD -m 1000 --lookback-factor 3 && echo "Testing exit on change of lookback factor, SUCCESS"

$CMD -m 300000;

for i in $(seq 1 5); do
	$CMD -m 70000 --simulate-send-failure;
done

for i in $(seq 1 5); do
	$CMD -m 70000;
done

$CMD -m 2000000;

# simulate a log rotation
mv $STATSLOG $STATSLOG.1
head -5500000 $STATSLOG_SOURCE | tail -500000 > $STATSLOG
$CMD -m 200000;
