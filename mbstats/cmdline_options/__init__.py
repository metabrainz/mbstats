#!/usr/bin/python3 -tt
# -*- coding: utf-8 -*-

#
# stats.parser.py
#
# Tails a log and applies mbstats parser, then reports metrics to InfluxDB
#
# Usage:
#
# $ stats.parser.py [options]
#
# Help:
#
# $ stats.parser.py -h
#
#
# Copyright 2016-2019, MetaBrainz Foundation
# Author: Laurent Monin
#
# stats.parser.py is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# stats.parser.py is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Logster. If not, see <http://www.gnu.org/licenses/>.
#
# Include bits of code from Etsy Logster
# https://github.com/etsy/logster
#
# Logster itself was forked from the ganglia-logtailer project
# (http://bitbucket.org/maplebed/ganglia-logtailer):
# Copyright Linden Research, Inc. 2008
# Released under the GPL v2 or later.
# For a full description of the license, please visit
# http://www.gnu.org/licenses/gpl.txt
#

import argparse
import json
import platform

from mbstats.utils import read_config


class ParseOptionsSysExit(Exception):

    def __init__(self, exit_code, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.code = exit_code


def parse_options():
    description = \
        """Tail and parse a formatted nginx log file, sending results to InfluxDB."""
    epilog = \
        """
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

    """

    default_options = {
        'config': [],
        'datacenter': '',
        'dry_run': False,
        'file': '',
        'hostname': platform.node(),
        'log_conf': None,
        'log_dir': '',
        'max_lines': 0,
        'name': '',
        'quiet': 0,
        'workdir': '.',

        'influx_batch_size': 500,
        'influx_database': 'mbstats',
        'influx_host': 'localhost',
        'influx_password': 'root',
        'influx_port': 8086,
        'influx_timeout': 40,
        'influx_username': 'root',

        'bucket_duration': 60,
        'debug': False,
        'do_not_skip_to_end': False,
        'influx_drop_database': False,
        'locker': 'fcntl',
        'lookback_factor': 2,
        'send_failure_fifo_size': 30,
        'simulate_send_failure': False,
        'startover': False,
        'log_handler': 'file',
    }
    conf_parser = argparse.ArgumentParser(add_help=False)
    conf_parser.add_argument("-c", "--config", help="Specify json config file(s)",
                             action='append', metavar="FILE")
    args, remaining_argv = conf_parser.parse_known_args()

    if args.config:
        for conf_path in args.config:
            default_options['config'].append(conf_path)
            config = read_config(conf_path)
            for k in config:
                if k not in default_options:
                    continue
                if k == 'config':
                    continue
                default_options[k] = config[k]

    parser = argparse.ArgumentParser(description=description, epilog=epilog,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     parents=[conf_parser], conflict_handler='resolve')
    parser.set_defaults(**default_options)

    required = parser.add_argument_group('required arguments')
    required.add_argument('-f', '--file',
                          help="log file to process")

    common = parser.add_argument_group('common arguments')
    common.add_argument('-c', '--config', action='append', metavar='FILE',
                        help="Specify json config file(s)")
    common.add_argument('-d', '--datacenter',
                        help="string to use as 'dc' tag")
    common.add_argument('-H', '--hostname',
                        help="string to use as 'host' tag")
    common.add_argument('-l', '--log-dir', action='store',
                        help='Where to store the stats.parser logfile.  Default location is workdir')
    common.add_argument('-n', '--name',
                        help="string to use as 'name' tag")
    common.add_argument('-m', '--max-lines', type=int,
                        help="maximum number of lines to process")
    common.add_argument('-w', '--workdir',
                        help="directory where offset/status are stored")
    common.add_argument('-y', '--dry-run', action='store_true',
                        help='Parse the log file but send stats to standard output')
    common.add_argument('-q', '--quiet', action='count',
                        help='Reduce verbosity / quiet mode')

    influx = parser.add_argument_group('influxdb arguments')
    influx.add_argument('--influx-host',
                        help="influxdb host")
    influx.add_argument('--influx-port', type=int,
                        help="influxdb port")
    influx.add_argument('--influx-username',
                        help="influxdb username")
    influx.add_argument('--influx-password',
                        help="influxdb password")
    influx.add_argument('--influx-database',
                        help="influxdb database")
    influx.add_argument('--influx-timeout', type=int,
                        help="influxdb timeout")
    influx.add_argument('--influx-batch-size', type=int,
                        help="number of points to send per batch")

    expert = parser.add_argument_group('expert arguments')
    expert.add_argument('-D', '--debug', action='store_true',
                        help="Enable debug mode")
    expert.add_argument('--influx-drop-database', action='store_true',
                        help="drop existing InfluxDB database, use with care")
    expert.add_argument('--locker', choices=('fcntl', 'portalocker'),
                        help="type of lock to use")
    expert.add_argument('--lookback-factor', type=int,
                        help="number of buckets to wait before sending any data")
    expert.add_argument('--startover', action='store_true',
                        help="ignore all status/offset, like a first run")
    expert.add_argument('--do-not-skip-to-end', action='store_true',
                        help="do not skip to end on first run")
    expert.add_argument('--bucket-duration', type=int,
                        help="duration for each bucket in seconds")
    expert.add_argument('--log-conf', action='store',
                        help='Logging configuration file. None by default')
    expert.add_argument('--dump-config', action='store_true',
                        help="dump config as json to stdout")
    expert.add_argument('--log-handler', action='store',
                        help="Log to (syslog, file, stdout)")
    expert.add_argument('--send-failure-fifo-size', type=int,
                        help="Number of failed sends to backup")
    expert.add_argument('--simulate-send-failure', action='store_true',
                        help="Simulate send failure for testing purposes")

    options = parser.parse_args(remaining_argv)
    if options.dump_config:
        print((json.dumps(vars(options), indent=4, sort_keys=True)))
        raise ParseOptionsSysExit(0)

    if not options.file:
        parser.print_usage()
        raise ParseOptionsSysExit(1)

    return options
