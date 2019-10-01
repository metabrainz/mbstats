[![CircleCI](https://circleci.com/gh/metabrainz/mbstats/tree/master.svg?style=svg)](https://circleci.com/gh/metabrainz/mbstats/tree/master)


```
usage: __main__.py [-h] [-f FILE] [-c FILE] [-d DATACENTER] [-H HOSTNAME]
                   [-l LOG_DIR] [-n NAME] [-m MAX_LINES] [-w WORKDIR] [-y]
                   [-q] [--influx-host INFLUX_HOST]
                   [--influx-port INFLUX_PORT]
                   [--influx-username INFLUX_USERNAME]
                   [--influx-password INFLUX_PASSWORD]
                   [--influx-database INFLUX_DATABASE]
                   [--influx-timeout INFLUX_TIMEOUT]
                   [--influx-batch-size INFLUX_BATCH_SIZE] [-D]
                   [--influx-drop-database] [--locker {fcntl,portalocker}]
                   [--lookback-factor LOOKBACK_FACTOR] [--startover]
                   [--do-not-skip-to-end] [--bucket-duration BUCKET_DURATION]
                   [--log-conf LOG_CONF] [--dump-config]
                   [--log-handler LOG_HANDLER]
                   [--send-failure-fifo-size SEND_FAILURE_FIFO_SIZE]
                   [--simulate-send-failure]

Tail and parse a formatted nginx log file, sending results to InfluxDB.

optional arguments:
  -h, --help            show this help message and exit

required arguments:
  -f FILE, --file FILE  log file to process

common arguments:
  -c FILE, --config FILE
                        Specify json config file(s)
  -d DATACENTER, --datacenter DATACENTER
                        string to use as 'dc' tag
  -H HOSTNAME, --hostname HOSTNAME
                        string to use as 'host' tag
  -l LOG_DIR, --log-dir LOG_DIR
                        Where to store the stats.parser logfile. Default
                        location is workdir
  -n NAME, --name NAME  string to use as 'name' tag
  -m MAX_LINES, --max-lines MAX_LINES
                        maximum number of lines to process
  -w WORKDIR, --workdir WORKDIR
                        directory where offset/status are stored
  -y, --dry-run         Parse the log file but send stats to standard output
  -q, --quiet           Reduce verbosity / quiet mode

influxdb arguments:
  --influx-host INFLUX_HOST
                        influxdb host
  --influx-port INFLUX_PORT
                        influxdb port
  --influx-username INFLUX_USERNAME
                        influxdb username
  --influx-password INFLUX_PASSWORD
                        influxdb password
  --influx-database INFLUX_DATABASE
                        influxdb database
  --influx-timeout INFLUX_TIMEOUT
                        influxdb timeout
  --influx-batch-size INFLUX_BATCH_SIZE
                        number of points to send per batch

expert arguments:
  -D, --debug           Enable debug mode
  --influx-drop-database
                        drop existing InfluxDB database, use with care
  --locker {fcntl,portalocker}
                        type of lock to use
  --lookback-factor LOOKBACK_FACTOR
                        number of buckets to wait before sending any data
  --startover           ignore all status/offset, like a first run
  --do-not-skip-to-end  do not skip to end on first run
  --bucket-duration BUCKET_DURATION
                        duration for each bucket in seconds
  --log-conf LOG_CONF   Logging configuration file. None by default
  --dump-config         dump config as json to stdout
  --log-handler LOG_HANDLER
                        Log to (syslog, file, stdout)
  --send-failure-fifo-size SEND_FAILURE_FIFO_SIZE
                        Number of failed sends to backup
  --simulate-send-failure
                        Simulate send failure for testing purposes

    To use add following to http section of your nginx configuration:

      log_format stats
        '1|'
        '$msec|'
        '$host|'
        '$statproto|'
        '$loctag|'
        '$status|'
        '$bytes_sent|'
        '$gzip_ratio|'
        '$request_length|'
        '$request_time|'
        '$upstream_addr|'
        '$upstream_status|'
        '$upstream_response_time|'
        '$upstream_connect_time|'
        '$upstream_header_time';

      map $host $loctag {
        default '-';
      }

      map $https $statproto {
        default '-';
        on 's';
      }

    You can use $loctag to tag a specific location:
        set $loctag "ws";

    In addition of your usual access log, add something like:
        access_log /var/log/nginx/my.stats.log stats buffer=256k flush=10s

    Note: first field in stats format declaration is a format version, it should be set to 1.
```

## Dev

Installing dev requirements:

```
pip install -r requirements-dev.txt
```

Running source code checks (isort, pyflakes, pylint, ...):

```
make check
```

Running tests:

```
make test
```

or:

```
python setup.py test
```

Build docker image:

```
make dockerbuild
```
