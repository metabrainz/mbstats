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


from collections import (
    defaultdict,
    deque,
)
from enum import (
    IntEnum,
    unique,
)
import itertools
import logging.config
import logging.handlers
import math
import os.path
import sys
from time import time
import traceback

from pygtail import Pygtail

from mbstats.backends import BackendDryRun
from mbstats.backends.influxdb import InfluxBackend
from mbstats.cmdline_options import (
    ParseOptionsSysExit,
    parse_options,
)
from mbstats.locker import (
    Locker,
    LockingError,
)
from mbstats.safefile import SafeFile
from mbstats.utils import (
    bucket2time,
    lineno,
    load_obj,
    msec2bucket,
    save_obj,
)


# https://github.com/metabrainz/openresty-gateways/blob/master/files/nginx/nginx.conf#L23
@unique
class PosField(IntEnum):
    version = 0
    msec = 1
    vhost = 2
    protocol = 3
    loctag = 4
    status = 5
    bytes_sent = 6
    gzip_ratio = 7
    request_length = 8
    request_time = 9
    upstream_addr = 10
    upstream_status = 11
    upstream_response_time = 12
    upstream_connect_time = 13
    upstream_header_time = 14


def factory():
    return lambda x: x


types = defaultdict(factory)
# pylint: disable=W0108
types['upstream_status'] = lambda x: int(x)
types['upstream_response_time'] = types['upstream_connect_time'] = types['upstream_header_time'] = lambda x: float(
    x)
# pylint: enable=W0108


# @profile


def parse_upstreams(row):
    # servers were contacted ", "
    # internal redirect " : "
    r = dict()
    splitted = [x.split(' : ') for x in row['upstream_addr'].split(", ")]
    r['servers_contacted'] = len(splitted)
    r['internal_redirects'] = len([x for x in splitted if len(x) > 1])
    upstream_addr = list(itertools.chain.from_iterable(splitted))

    upstream_status = list(itertools.chain.from_iterable([x.split(' : ') for x
                                                          in
                                                          row['upstream_status'].split(", ")]))
    upstream_response_time = \
        list(itertools.chain.from_iterable([x.split(' : ') for x
                                            in
                                            row['upstream_response_time'].split(", ")]))
    upstream_header_time = \
        list(itertools.chain.from_iterable([x.split(' : ') for x
                                            in
                                            row['upstream_header_time'].split(", ")]))
    upstream_connect_time = \
        list(itertools.chain.from_iterable([x.split(' : ') for x
                                            in
                                            row['upstream_connect_time'].split(", ")]))

    r['status'] = dict()
    r['response_time'] = defaultdict(float)
    r['connect_time'] = defaultdict(float)
    r['header_time'] = defaultdict(float)
    r['servers'] = []
    for item in zip(upstream_addr,
                    upstream_status,
                    upstream_response_time,
                    upstream_connect_time,
                    upstream_header_time
                    ):
        k = item[0]
        r['servers'].append(k)
        # not using defauldict() here intentionally, because it requires lamba/function
        # and it breaks with pickle
        if k not in r['status']:
            r['status'][k] = dict()

        # ensure status code is an integer (but it is stored as string), this
        # should raise ValueError if it can't be converted
        int(item[1])
        if item[1] in r['status'][k]:
            r['status'][k][item[1]] += 1
        else:
            r['status'][k][item[1]] = 1

        try:
            r['response_time'][k] += float(item[2])
        except ValueError:
            if item[2] not in ('-', ''):
                raise

        try:
            r['connect_time'][k] += float(item[3])
        except ValueError:
            if item[3] not in ('-', ''):
                raise

        try:
            r['header_time'][k] += float(item[4])
        except ValueError:
            if item[4] not in ('-', ''):
                raise

    return r


class ParseEnd(Exception):
    pass


class ParseSkip(Exception):
    pass


def parseline(line, last_msec=0, ignore_before=0, bucket_duration=60):
    items = line.split('|')
    msec = float(items[PosField.msec])
    if msec <= ignore_before:
        # skip unordered & old entries
        raise ParseSkip
    if msec > last_msec:
        last_msec = msec
    bucket = msec2bucket(msec, bucket_duration)

    row = {
        'vhost': items[PosField.vhost],
        'protocol': items[PosField.protocol],
        'loctag': items[PosField.loctag],
        'status': int(items[PosField.status]),
        'bytes_sent': int(items[PosField.bytes_sent]),
        'request_length': int(items[PosField.request_length]),
    }

    if items[PosField.gzip_ratio] != '-':
        row['gzip_ratio'] = float(items[PosField.gzip_ratio])
    if items[PosField.request_time] != '-':
        row['request_time'] = float(items[PosField.request_time])

    if items[PosField.upstream_addr] != '-':
        # Note : last element contains trailing new line character
        # from readline()
        upstreams = {
            'upstream_addr': items[PosField.upstream_addr],
            'upstream_status': items[PosField.upstream_status],
            'upstream_response_time': items[PosField.upstream_response_time],
            'upstream_connect_time': items[PosField.upstream_connect_time],
            'upstream_header_time': items[PosField.upstream_header_time].rstrip('\r\n'),
        }

        row['upstreams'] = parse_upstreams(upstreams)

    return row, last_msec, bucket


def parsefile(tailer, status, options, logger=None):
    parsed_lines = 0
    skipped_lines = 0
    first_run = False
    max_lines = options.max_lines
    bucket_duration = status['bucket_duration']
    lookback_factor = status['lookback_factor']
    mbs = mbsdict()
    # lines are logged when request ends, which means they can be unordered
    if not status['last_msec']:
        ignore_before = 0
    else:
        ignore_before = status['last_msec'] - bucket_duration * lookback_factor
    if logger:
        logger.debug(
            "max_lines=%d bucket_duration=%d lookback_factor=%d ignore_before=%f" %
            (max_lines, bucket_duration, lookback_factor, ignore_before))
    last_msec = 0
    last_bucket = 0
    if status['leftover'] is not None:
        if logger:
            logger.debug("Examining %d leftovers" % len(status['leftover']))
        storage = status['leftover']
        if logger and options.quiet < 2:
            for bucket in storage:
                logger.info("Previous leftover bucket: %s %d" %
                            (bucket2time(bucket, status['bucket_duration']), len(storage[bucket])))
    else:
        storage = defaultdict(deque)
        if logger:
            logger.info("First run")
        first_run = not options.do_not_skip_to_end
    bucket = 0
    if first_run:
        # code duplication here, intentional
        try:
            for line in tailer:
                parsed_lines += 1
                try:
                    items = line.split('|', 2)
                    msec = float(items[PosField.msec])
                    if msec > last_msec:
                        last_msec = msec
                    bucket = int(math.ceil(msec / bucket_duration))
                except ValueError as e:
                    logger.error(str(e), line)
                    raise
                if parsed_lines == max_lines:
                    raise ParseEnd
        except ParseEnd:
            pass
        # ensure we start on an entire bucket, so values are correct
        last_msec = (bucket + lookback_factor) * bucket_duration
        skipped_lines = parsed_lines
        if logger:
            logger.info("End of first run: bucket=%d last_msec=%f skipped=%d" %
                        (bucket, last_msec, skipped_lines))
    else:
        try:
            for line in tailer:
                parsed_lines += 1
                try:
                    row, last_msec, bucket = parseline(line,
                                                       ignore_before=ignore_before,
                                                       bucket_duration=bucket_duration,
                                                       last_msec=last_msec)
                    storage[bucket].append(row)
                    ready_to_process = bucket - lookback_factor

                    if storage[ready_to_process]:
                        if logger and options.quiet < 2:
                            logger.info("Processing bucket: %s %d" %
                                        (bucket2time(ready_to_process,
                                                     status['bucket_duration']),
                                         len(storage[ready_to_process])))
                        process_bucket(ready_to_process, storage, status, mbs)
                except ParseSkip:
                    skipped_lines += 1
                except Exception as e:
                    if logger:
                        logger.error("%s: %s" % (line, e))
                    raise
                if parsed_lines == max_lines:
                    raise ParseEnd
        except ParseEnd:
            pass
        if skipped_lines and logger and options.quiet < 2:
            logger.info("Skipped %d unordered lines" % skipped_lines)

    tailer._update_offset_file()
    last_bucket = bucket
    leftover = defaultdict(deque)
    for bucket in storage:
        if storage[bucket] and bucket >= last_bucket - lookback_factor:
            leftover[bucket] = storage[bucket]

    if logger:
        logger.debug("Leftovers %d" % len(leftover))
        if options.quiet < 2:
            for bucket in leftover:
                logger.info("Unprocessed bucket: %s %d" % (bucket2time(bucket,
                                                                       status['bucket_duration']),
                                                           len(leftover[bucket])))

    mbspostprocess(mbs)
    return (mbs, leftover, last_msec, parsed_lines, skipped_lines)


def mbsdict():
    return {
        'bytes_sent': defaultdict(int),
        'gzip_count': defaultdict(int),
        'gzip_count_percent': defaultdict(float),
        'gzip_ratio_mean': defaultdict(float),
        '_gzip_ratio_premean': defaultdict(float),
        'hits': defaultdict(int),
        'hits_with_upstream': defaultdict(int),
        'request_length_mean': defaultdict(float),
        '_request_length_premean': defaultdict(int),
        'request_time_mean': defaultdict(float),
        '_request_time_premean': defaultdict(float),
        'status': defaultdict(int),
        'upstreams_connect_time_mean': defaultdict(float),
        '_upstreams_connect_time_premean': defaultdict(float),
        'upstreams_header_time_mean': defaultdict(float),
        '_upstreams_header_time_premean': defaultdict(float),
        'upstreams_hits': defaultdict(int),
        '_upstreams_internal_redirects': defaultdict(int),
        'upstreams_internal_redirects_per_hit': defaultdict(int),
        'upstreams_response_time_mean': defaultdict(float),
        '_upstreams_response_time_premean': defaultdict(float),
        '_upstreams_servers_contacted': defaultdict(int),
        'upstreams_servers_contacted_per_hit': defaultdict(float),
        'upstreams_servers': defaultdict(int),
        'upstreams_status': defaultdict(int),
    }


def process_bucket(bucket, storage, status, mbs):
    while True:
        try:
            row = storage[bucket].pop()

            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
            mbs['hits'][tags] += 1
            mbs['bytes_sent'][tags] += row['bytes_sent']

            if 'gzip_ratio' in row:
                mbs['gzip_count'][tags] += 1
                mbs['_gzip_ratio_premean'][tags] += row['gzip_ratio']

            mbs['_request_length_premean'][tags] += row['request_length']
            mbs['_request_time_premean'][tags] += row['request_time']

            tags = (bucket, row['vhost'], row['protocol'],
                    row['loctag'], row['status'])
            mbs['status'][tags] += 1

            if 'upstreams' in row:
                ru = row['upstreams']

                tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
                mbs['hits_with_upstream'][tags] += 1
                mbs['_upstreams_servers_contacted'][tags] += ru['servers_contacted']
                mbs['_upstreams_internal_redirects'][tags] += ru['internal_redirects']
                mbs['upstreams_servers'][tags] += len(ru['servers'])
                for upstream in ru['servers']:
                    tags = (bucket, row['vhost'],
                            row['protocol'], row['loctag'], upstream)
                    mbs['upstreams_hits'][tags] += 1
                    mbs['_upstreams_response_time_premean'][tags] += ru['response_time'][upstream]
                    mbs['_upstreams_connect_time_premean'][tags] += ru['connect_time'][upstream]
                    mbs['_upstreams_header_time_premean'][tags] += ru['header_time'][upstream]
                    for status_ in ru['status'][upstream]:
                        tags = (bucket, row['vhost'], row['protocol'], row['loctag'],
                                upstream, status_)
                        mbs['upstreams_status'][tags] += 1
        except IndexError:
            break


def mbspostprocess(mbs):
    if mbs['gzip_count']:
        for k, v in list(mbs['_gzip_ratio_premean'].items()):
            mbs['gzip_ratio_mean'][k] = v / mbs['gzip_count'][k]

    if mbs['hits']:
        for k, v in list(mbs['_request_length_premean'].items()):
            mbs['request_length_mean'][k] = v / mbs['hits'][k]

        for k, v in list(mbs['_request_time_premean'].items()):
            mbs['request_time_mean'][k] = v / mbs['hits'][k]

        for k, v in list(mbs['hits'].items()):
            if mbs['gzip_count'] and v:
                mbs['gzip_count_percent'][k] = (mbs['gzip_count'][k] * 1.0) / v
            else:
                mbs['gzip_count_percent'][k] = 0.0

    if mbs['upstreams_hits']:
        for k, v in list(mbs['_upstreams_response_time_premean'].items()):
            mbs['upstreams_response_time_mean'][k] = v / \
                mbs['upstreams_hits'][k]

        for k, v in list(mbs['_upstreams_connect_time_premean'].items()):
            mbs['upstreams_connect_time_mean'][k] = v / \
                mbs['upstreams_hits'][k]

        for k, v in list(mbs['_upstreams_header_time_premean'].items()):
            mbs['upstreams_header_time_mean'][k] = v / mbs['upstreams_hits'][k]

        for k, v in list(mbs['_upstreams_servers_contacted'].items()):
            mbs['upstreams_servers_contacted_per_hit'][k] = float(
                v) / mbs['hits_with_upstream'][k]

        for k, v in list(mbs['_upstreams_internal_redirects'].items()):
            mbs['upstreams_internal_redirects_per_hit'][k] = float(
                v) / mbs['hits_with_upstream'][k]


def init_logger(options):

    logger = logging.getLogger('stats.parser')
    logger.handlers = []
    if options.log_handler == 'syslog':
        hdlr = logging.handlers.SysLogHandler(
            address='/dev/log', facility=logging.handlers.SysLogHandler.LOG_SYSLOG)
        formatter = logging.Formatter('%(name)s: %(message)s')
        hdlr.setFormatter(formatter)
    elif options.log_handler == 'stdout':
        hdlr = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s %(process)-5s %(levelname)-8s %(message)s')
        hdlr.setFormatter(formatter)
    else:
        log_dir = options.log_dir
        if not log_dir:
            log_dir = options.workdir
        # Logging infrastructure for use throughout the script.
        # Uses appending log file, rotated at 100 MB, keeping 5.
        if (not os.path.isdir(log_dir)):
            os.mkdir(log_dir)
        formatter = logging.Formatter(
            '%(asctime)s %(process)-5s %(levelname)-8s %(message)s')
        hdlr = logging.handlers.RotatingFileHandler(
            '%s/stats.parser.log' % log_dir, 'a', 100 * 1024 * 1024, 5)
        hdlr.setFormatter(formatter)

    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    if options.log_conf:
        logging.config.fileConfig(options.log_conf)

    if options.debug:
        logger.setLevel(logging.DEBUG)

    if not options.quiet:
        logger.info("Starting with options %r", vars(options))
    elif options.quiet == 1:
        logger.info("Starting")

    return logger


def init_status(files, options, logger):
    status = {}
    try:
        status = load_obj(files['status'].main, logger=logger)
    except IOError as e:
        logger.warning("Failed to load status from %s: %s" %
                       (files['status'].main, e))

    logger.debug("main status: %r" % len(status))

    default_status = {
        'last_msec': lambda: 0,
        'leftover': lambda: None,
        'bucket_duration': lambda: options.bucket_duration,
        'lookback_factor': lambda: options.lookback_factor,
        'saved_points': lambda: deque([], options.send_failure_fifo_size),
    }
    save = False
    for k in default_status:
        if k not in status:
            status[k] = default_status[k]()
            save = True
    if save:
        save_obj(status, files['status'].tmp, logger=logger)
    return status


def main():
    script_start_time = time()
    try:
        options = parse_options()
    except ParseOptionsSysExit as e:
        sys.exit(e.code)

    logger = init_logger(options)

    parsed_lines = 0
    skipped_lines = 0
    try:
        workdir = os.path.abspath(options.workdir)
        files = {
            'offset': SafeFile(workdir, options.file, suffix='.offset',
                               logger=logger),
            'status': SafeFile(workdir, options.file, suffix='.status',
                               logger=logger),
            'lock': SafeFile(workdir, options.file, suffix='.lock',
                             logger=logger),
        }

        # Check for lock file so we don't run multiple copies of the same parser
        # simultaneuosly. This will happen if the log parsing takes more time than
        # the cron period.
        try:
            lock = Locker(files['lock'].main, lock_type=options.locker, logger=logger)
        except LockingError as e:
            msg = "Locking error: %s" % e
            logger.warning(msg)
            sys.exit(1)

        backend = InfluxBackend(options, logger=logger)

        if options.startover:
            files['offset'].remove_main()
            files['status'].remove_main()

        files['offset'].copy_main_to_tmp()

        pygtail = Pygtail(options.file, offset_file=files['offset'].tmp)

        status = init_status(files, options, logger)

        if status['leftover'] is not None and len(status['leftover']) > 0:
            exit = False
            msg = 'Error:'
            if status['bucket_duration'] != options.bucket_duration:
                msg += (" Bucket duration mismatch %d vs %d (set via option)" %
                        (status['bucket_duration'], options.bucket_duration))
                exit = True
            if status['lookback_factor'] != options.lookback_factor:
                msg += (" Lookback factor mismatch %d vs %d (set via option)" %
                        (status['lookback_factor'], options.lookback_factor))
                exit = True
            if exit:
                msg += (" If you know what you are doing, remove status file %s" %
                        files['status'].main)
                logger.error(msg)
                sys.exit(1)

        mbs, leftover, last_msec, parsed_lines, skipped_lines = parsefile(pygtail, status, options, logger=logger)
        status['leftover'] = leftover
        status['last_msec'] = last_msec

        backend.add_points(mbs, status)
        if backend.points or status['saved_points']:
            tags = {
                'host': options.hostname,
                'name': options.name or options.file,
            }
            if options.datacenter:
                tags['dc'] = options.datacenter

            if status['saved_points']:
                to_resend = list()
                for savedpoints in status['saved_points']:
                    to_resend += savedpoints
                try:
                    logger.info("Trying to send %d saved points" %
                                len(to_resend))
                    if options.simulate_send_failure:
                        raise Exception('Simulating send failure (resend)')
                    if backend.send_points(tags, points=to_resend):
                        status['saved_points'].clear()
                except BackendDryRun as e:
                    logger.debug("Dry run: %s" % e)
                except Exception as e:
                    msg = "Send failed again: %s: %s" % (lineno(), e)
                    traceback.print_exc()
                    logger.error(msg)

            if backend.points:
                try:
                    if options.simulate_send_failure:
                        raise Exception('Simulating send failure')
                    if not backend.send_points(tags):
                        raise Exception('influx_send failed')
                except BackendDryRun as e:
                    logger.debug("Dry run: %s" % e)
                except Exception as e:
                    msg = "Send: Exception caught at %s: %s" % (lineno(), e)
                    traceback.print_exc()
                    logger.error(msg)
                    status['saved_points'].append(backend.points)
                    logger.info("Failed to send, saving points for later %d/%d" %
                                (len(status['saved_points']),
                                 options.send_failure_fifo_size))

        save_obj(status, files['status'].tmp, logger=logger)
    except KeyboardInterrupt:
        if options.quiet < 2:
            msg = "Exiting on keyboard interrupt"
            logger.info(msg)
        retcode = 1
    except SystemExit as e:
        retcode = e.code
    except Exception as e:
        msg = "Exception caught at %s: %s" % (lineno(), e)
        traceback.print_exc()
        logger.error(msg)
        retcode = 1
    else:
        files['offset'].rename_tmp_to_main()
        files['status'].rename_tmp_to_main()
        retcode = 0
    finally:
        files['offset'].remove_tmp()
        files['status'].remove_tmp()
        try:
            lock.unlock()
        except LockingError:
            pass

    if options.quiet < 2:
        # Log the execution time
        exec_time = round(time() - script_start_time, 1)
        if parsed_lines:
            mean_per_line = 1000000.0 * (exec_time / parsed_lines)
        else:
            mean_per_line = 0.0
        logger.info("duration=%ss parsed=%d skipped=%d mean_per_line=%0.3fµs" %
                    (exec_time, parsed_lines, skipped_lines, mean_per_line))

    sys.exit(retcode)


if __name__ == "__main__":
    main()
