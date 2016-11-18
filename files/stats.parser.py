#!/usr/bin/python2 -tt
# -*- coding: utf-8 -*-

###
###  stats.parser.py
###
###  Tails a log and applies mbstats parser, then reports metrics to InfluxDB
###
###  Usage:
###
###    $ stats.parser.py [options]
###
###  Help:
###
###    $ stats.parser.py -h
###
###
###  Copyright 2016, MetaBrainz Foundation
###  Author: Laurent Monin
###
###  stats.parser.py is free software: you can redistribute it and/or modify
###  it under the terms of the GNU General Public License as published by
###  the Free Software Foundation, either version 3 of the License, or
###  (at your option) any later version.
###
###  stats.parser.py is distributed in the hope that it will be useful,
###  but WITHOUT ANY WARRANTY; without even the implied warranty of
###  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
###  GNU General Public License for more details.
###
###  You should have received a copy of the GNU General Public License
###  along with Logster. If not, see <http://www.gnu.org/licenses/>.
###
###  Include bits of code from Etsy Logster
###  https://github.com/etsy/logster
###
###  Logster itself was forked from the ganglia-logtailer project
###  (http://bitbucket.org/maplebed/ganglia-logtailer):
###    Copyright Linden Research, Inc. 2008
###    Released under the GPL v2 or later.
###    For a full description of the license, please visit
###    http://www.gnu.org/licenses/gpl.txt
###


from collections import defaultdict
from influxdb import InfluxDBClient
from pygtail import Pygtail
import argparse
try:
    import cPickle as pickle
except:
    import pickle
import csv
import datetime
import gzip
import itertools
import json
import math
import os.path
import platform
import re
import shutil
import sys



# https://github.com/metabrainz/openresty-gateways/blob/master/files/nginx/nginx.conf#L23
fieldnames = [
    'version',
    'msec',
    'vhost',
    'protocol',
    'loctag',
    'status',
    'bytes_sent',
    'gzip_ratio',
    'request_length',
    'request_time',
    'upstream_addr',
    'upstream_status',
    'upstream_response_time',
    'upstream_connect_time',
    'upstream_header_time',
]

pos_version = 0
pos_msec = 1
pos_vhost = 2
pos_protocol = 3
pos_loctag = 4
pos_status = 5
pos_bytes_sent = 6
pos_gzip_ratio = 7
pos_request_length = 8
pos_request_time = 9
pos_upstream_addr = 10
pos_upstream_status = 11
pos_upstream_response_time = 12
pos_upstream_connect_time = 13
pos_upstream_header_time = 14



mbs_tags = {
    'hits': ('vhost', 'protocol', 'loctag'),
    'status': ('vhost', 'protocol', 'loctag', 'status'),
    'bytes_sent': ('vhost', 'protocol', 'loctag'),
    'gzip_count': ('vhost', 'protocol', 'loctag'),
    'gzip_percent': ('vhost', 'protocol', 'loctag'),
    'gzip_ratio_mean': ('vhost', 'protocol', 'loctag'),
    'request_length_mean': ('vhost', 'protocol', 'loctag'),
    'request_time_mean': ('vhost', 'protocol', 'loctag'),
    'upstreams_hits': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_status': ('vhost', 'protocol', 'loctag', 'upstream', 'status'),
    'upstreams_servers_contacted': ('vhost', 'protocol', 'loctag'),
    'upstreams_internal_redirects': ('vhost', 'protocol', 'loctag'),
    'upstreams_servers': ('vhost', 'protocol', 'loctag'),
    'upstreams_response_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_connect_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_header_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
}

def factory():
    return lambda x: x
types = defaultdict(factory)
types['upstream_status'] = lambda x: int(x)
types['upstream_response_time'] = types['upstream_connect_time'] = types['upstream_header_time'] = lambda x: float(x)

#@profile
def parse_upstreams(row):
    #servers were contacted ", "
    #internal redirect " : "
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
        if item[1] in r['status'][k]:
            r['status'][k][item[1]] += 1
        else:
            r['status'][k][item[1]] = 1
        r['response_time'][k] += float(item[2])
        r['connect_time'][k] += float(item[3])
        r['header_time'][k] += float(item[4])
    return r


def parsefile(tailer, status, options):
    max_lines = options.max_lines
    bucket_duration = status['bucket_duration']
    lookback_factor = status['lookback_factor']
    mbs = mbsdict()
    # lines are logged when request ends, which means they can be unordered
    if status['last_msec']:
        ignore_before = status['last_msec'] - bucket_duration * lookback_factor
    else:
        ignore_before = 0
    last_msec = 0
    skipped = 0
    last_bucket = 0
    storage = defaultdict(list)
    if status['leftover'] is not None:
        previous_leftover = status['leftover']
        for bucket in previous_leftover:
            storage[bucket] = previous_leftover[bucket]
            print ("Previous leftover bucket: %s %d" % (bucket2time(bucket, status), len(previous_leftover[bucket])))

    for line in tailer:
        try:
            items = line.rstrip('\r\n').split('|')
            msec = float(items[pos_msec])
            if msec <= ignore_before:
                # skip unordered & old entries
                skipped += 1
                continue
            if msec > last_msec:
                last_msec = msec
            bucket = int(math.ceil(float(msec)/bucket_duration))

            row = {
                'vhost': items[pos_vhost],
                'protocol': items[pos_protocol],
                'loctag': items[pos_loctag],
                'status': int(items[pos_status]),
                'bytes_sent': int(items[pos_bytes_sent]),
                'request_length': int(items[pos_request_length]),
            }

            if items[pos_gzip_ratio] != '-':
                row['gzip_ratio'] = float(items[pos_gzip_ratio])
            if items[pos_request_time] != '-':
                row['request_time'] = float(items[pos_request_time])

            if items[pos_upstream_addr] != '-':
                row['upstreams'] = parse_upstreams({
                'upstream_addr': items[pos_upstream_addr],
                'upstream_status': items[pos_upstream_status],
                'upstream_response_time': items[pos_upstream_response_time],
                'upstream_connect_time': items[pos_upstream_connect_time],
                'upstream_header_time': items[pos_upstream_header_time],
                })

            storage[bucket].append(row)
            ready_to_process = bucket - lookback_factor

            if ready_to_process in storage:
                process_bucket(ready_to_process, storage, status, mbs)
        except ValueError as e:
            print(e, line)
            print(row)
            raise
        max_lines -= 1
        if max_lines == 0:
            break
    if skipped:
        print("Skipped %d unordered lines" % skipped)
    tailer._update_offset_file()
    last_bucket = bucket
    for bucket in storage:
         print ("Unprocessed bucket: %s %d" % (bucket2time(bucket, status), len(storage[bucket])))
         if bucket < last_bucket - lookback_factor:
            print ("Removing old bucket: %s %d" % (bucket2time(bucket, status), len(storage[bucket])))
            del storage[bucket]
    mbspostprocess(mbs)
    return (mbs, storage, last_msec)


def mbsdict():
    return {
        'bytes_sent': defaultdict(int),
        'gzip_count': defaultdict(int),
        'gzip_percent': defaultdict(float),
        'gzip_ratio_mean': defaultdict(float),
        '_gzip_ratio_premean': defaultdict(float),
        'hits': defaultdict(int),
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
        'upstreams_internal_redirects': defaultdict(int),
        'upstreams_response_time_mean': defaultdict(float),
        '_upstreams_response_time_premean': defaultdict(float),
        'upstreams_servers_contacted': defaultdict(int),
        'upstreams_servers': defaultdict(int),
        'upstreams_status': defaultdict(int),
    }


def process_bucket(bucket, storage, status, mbs):
    print ("Processing bucket: %s %d" % (bucket2time(bucket, status), len(storage[bucket])))

    for row in storage[bucket]:
#    mbs['hits']:
#        tags: vhost, protocol, loc
#        value: count
#
        tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
        mbs['hits'][tags] += 1

#    mbs['bytes_sent']:
#        tags: vhost, protocol, loc
#        value: sum
#
#            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
        mbs['bytes_sent'][tags] += row['bytes_sent']


#    mbs['gzip_count']:
#        tags: vhost, protocol, loc
#        value: count
#            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
#           mbs['gzip_count'][tags] += ('gzip_ratio' in r)

#    mbs['gzip_ratio']:
#        tags: vhost, protocol, loc
#        value: sum of gzip ratio / number of gzipped requests
        if 'gzip_ratio' in row:
            mbs['gzip_count'][tags] += 1
#                tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
            mbs['_gzip_ratio_premean'][tags] += row['gzip_ratio']

#
#    mbs['request_length']:
#        tags: vhost, protocol, loc
#        value: sum of request_length / hits
#
#            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
        mbs['_request_length_premean'][tags] += row['request_length']

#    mbs['request_time']:
#        tags: vhost, protocol, loc
#        value: sum of request_time / hits
#
#            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
        mbs['_request_time_premean'][tags] += row['request_time']

#    mbs['status']:
#        tags: vhost, protocol, loc,  status
#        value: count
#
        tags = (bucket, row['vhost'], row['protocol'], row['loctag'], row['status'])
        mbs['status'][tags] += 1

##### upstreams

        if 'upstreams' in row:

#    mbs['upstreams_hits']:
#        tags: vhost, protocol, loc, upstream
#        value: count
#
            for upstream in row['upstreams']['servers']:
                tags = (bucket, row['vhost'], row['protocol'], row['loctag'], upstream)
                mbs['upstreams_hits'][tags] += 1

#    mbs['upstreams_response_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of response_time / hits

           # for upstream in row['upstreams']['servers']:
          #      tags = (bucket, row['vhost'], row['protocol'], row['loctag'], upstream)
                mbs['_upstreams_response_time_premean'][tags] += row['upstreams']['response_time'][upstream]

#
#    mbs['upstreams_connect_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of connect_time / hits
#
          #  for upstream in row['upstreams']['servers']:
           #     tags = (bucket, row['vhost'], row['protocol'], row['loctag'], upstream)
                mbs['_upstreams_connect_time_premean'][tags] += row['upstreams']['connect_time'][upstream]


#    mbs['upstreams_header_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of header_time / hits
          #  for upstream in row['upstreams']['servers']:
           #     tags = (bucket, row['vhost'], row['protocol'], row['loctag'], upstream)
                mbs['_upstreams_header_time_premean'][tags] += row['upstreams']['header_time'][upstream]


#    mbs['upstreams_status']:
#        tags: vhost, protocol, loc, upstream, status
#        value: count
            #for upstream in row['upstreams']['servers']:
                for status in row['upstreams']['status'][upstream]:
                    tags = (bucket, row['vhost'], row['protocol'], row['loctag'],
                            upstream, status)
                    mbs['upstreams_status'][tags] += 1

#
#    mbs['upstreams_servers_contacted']:
#        tags: vhost, protocol, loc
#        value: sum of servers_contacted
#
            tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
            mbs['upstreams_servers_contacted'][tags] += row['upstreams']['servers_contacted']


#    mbs['upstream_internal_redirects']:
#        tags: vhost, protocol, loc
#        value: sum of internal_redirects
#
#                tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
            mbs['upstreams_internal_redirects'][tags] += row['upstreams']['internal_redirects']

#    mbs['upstreams_servers_count']:
#        tags: vhost, protocol, loc
#        value: sum of len of servers
#
#                tags = (bucket, row['vhost'], row['protocol'], row['loctag'])
            mbs['upstreams_servers'][tags] += len(row['upstreams']['servers'])


# bucket processed, remove it
    del storage[bucket]


def mbspostprocess(mbs):
###### calculations of means

# gzip_ratio_mean
    if mbs['gzip_count']:
        for k, v in mbs['_gzip_ratio_premean'].items():
            mbs['gzip_ratio_mean'][k] = v / mbs['gzip_count'][k]

    if mbs['hits']:
        # mbs['request_length_mean']
        for k, v in mbs['_request_length_premean'].items():
            mbs['request_length_mean'][k] = v / mbs['hits'][k]

        # mbs['request_time_mean']
        for k, v in mbs['_request_time_premean'].items():
            mbs['request_time_mean'][k] = v / mbs['hits'][k]

        for k, v in mbs['hits'].items():
            if mbs['gzip_count'] and v:
                mbs['gzip_percent'][k] = (mbs['gzip_count'][k] * 1.0) / v
            else:
                mbs['gzip_percent'][k] = 0.0

    if mbs['upstreams_hits']:
        # mbs['upstreams_response_time_mean']
        for k, v in mbs['_upstreams_response_time_premean'].items():
            mbs['upstreams_response_time_mean'][k] = v / mbs['upstreams_hits'][k]

        # mbs['upstreams_connect_time_mean']
        for k, v in mbs['_upstreams_connect_time_premean'].items():
            mbs['upstreams_connect_time_mean'][k] = v / mbs['upstreams_hits'][k]

        # mbs['upstreams_header_time_mean']
        for k, v in mbs['_upstreams_header_time_premean'].items():
            mbs['upstreams_header_time_mean'][k] = v / mbs['upstreams_hits'][k]


def save_obj(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(filepath):
    with open(filepath, 'rb') as f:
        return pickle.load(f)


def bucket2time(bucket, status):
    d = datetime.datetime.utcfromtimestamp(
            bucket*status['bucket_duration']
    )
    return d.isoformat() + 'Z'

def mbs2influx(mbs, status):
    extra_tags = dict()
    points = []
    for measurement, tagnames in mbs_tags.items():
        if not measurement in mbs:
            continue
        for tags, value in mbs[measurement].items():
            influxtags = dict(zip(tagnames, tags[1:]))
            for k, v in influxtags.items():
                if k == 'protocol':
                    if v == 's':
                        influxtags[k] = 'https'
                    else:
                        influxtags[k] = 'http'
                influxtags[k] = str(v)
            fields = { 'value': value}
            points.append({
                "measurement": measurement,
                "tags": influxtags,
                "time": bucket2time(tags[0], status),
                "fields": fields
            })
    return points


def influxdb_client(options):
    database = options.influx_database
    client = InfluxDBClient(host=options.influx_host,
                            port=options.influx_port,
                            username=options.influx_username,
                            password=options.influx_password,
                            database=database,
                            timeout=options.influx_timeout)
    if options.influx_drop_database:
        client.drop_database(database)
    client.create_database(database)
    return client

def influxdb_send(client, points, tags, batch_size=500):
    npoints = len(points)
    if npoints:
        print("Sending %d points" % npoints)
        print(points[0])
        return client.write_points(points, tags=tags, time_precision='m', batch_size=batch_size)
    return True



description= \
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
parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)

required = parser.add_argument_group('required arguments')
required.add_argument('-f', '--file', help="log file to process")

common = parser.add_argument_group('common arguments')
common.add_argument('-m', '--max-lines', default=0, type=int,
                    help="maximum number of lines to process")
common.add_argument('-w', '--workdir', default='.',
                   help="directory where offset/status are stored")
common.add_argument('-H', '--hostname', default=platform.node(),
                   help="string to use as 'host' tag")
common.add_argument('-n', '--name', default='',
                   help="string to use as 'name' tag")
common.add_argument('-d', '--datacenter', default='',
                   help="string to use as 'dc' tag")

influx = parser.add_argument_group('influxdb arguments')
influx.add_argument('--influx-host', default='localhost',
                   help="influxdb host")
influx.add_argument('--influx-port', default=8086, type=int,
                   help="influxdb port")
influx.add_argument('--influx-username', default='root',
                   help="influxdb username")
influx.add_argument('--influx-password', default='root',
                   help="influxdb password")
influx.add_argument('--influx-database', default='mbstats',
                   help="influxdb database")
influx.add_argument('--influx-timeout', default=40, type=int,
                   help="influxdb timeout")

expert = parser.add_argument_group('expert arguments')
expert.add_argument('--influx-drop-database', action='store_true', default=False,
                   help="drop existing InfluxDB database, use with care")
expert.add_argument('--locker', choices=('fcntl', 'portalocker'),
                    default='fcntl',
                    help="type of lock to use")
expert.add_argument('--lookback-factor', type=int, default=2,
                   help="number of buckets to wait before sending any data")
expert.add_argument('--startover', action='store_true', default=False,
                   help="ignore all status/offset, like a first run")
expert.add_argument('--ignore-first-run', action='store_true', default=False,
                   help="do not skip to end on first run")
expert.add_argument('--bucket-duration', type=int, default=60,
                   help="duration for each bucket in seconds")


options = parser.parse_args()
print(options)
filename = options.file
if not filename:
    parser.print_usage()
    sys.exit(1)

if options.locker == 'portalocker':
    import portalocker
    lock_exception_klass = portalocker.LockException
else:
    import fcntl
    lock_exception_klass = IOError

class LockingError(Exception):
    """ Exception raised for errors creating or destroying lockfiles. """
    pass


def start_locking(lockfile_name):
    """ Acquire a lock via a provided lockfile filename. """
    if os.path.exists(lockfile_name):
        raise LockingError("Lock file (%s) already exists." % lockfile_name)

    f = open(lockfile_name, 'w')

    try:
        if options.locker == 'portalocker':
            portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
        else:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write("%s" % os.getpid())
    except lock_exception_klass:
        # Would be better to also check the pid in the lock file and remove the
        # lock file if that pid no longer exists in the process table.
        raise LockingError("Cannot acquire logster lock (%s)" % lockfile_name)

    #logger.debug("Locking successful")
    return f


def end_locking(lockfile_fd, lockfile_name):
    """ Release a lock via a provided file descriptor. """
    try:
        if options.locker == 'portalocker':
            portalocker.unlock(lockfile_fd) # uses fcntl.LOCK_UN on posix (in contrast with the flock()ing below)
        else:
            if platform.system() == "SunOS": # GH issue #17
                fcntl.flock(lockfile_fd, fcntl.LOCK_UN)
            else:
                fcntl.flock(lockfile_fd, fcntl.LOCK_UN | fcntl.LOCK_NB)
    except lock_exception_klass:
        raise LockingError("Cannot release logster lock (%s)" % lockfile_name)

    try:
        lockfile_fd.close()
        os.unlink(lockfile_name)
    except OSError as e:
        raise LockingError("Cannot unlink %s" % lockfile_name)

    #logger.debug("Unlocking successful")
    return

class SafeFile(object):
    def __init__(self, workdir, identifier, suffix = ''):
        self.identifier = identifier
        self.suffix = suffix
        self.sane_filename = re.sub(r'\W', '_', self.identifier + self.suffix)
        self.workdir = workdir
        self.main = os.path.join(self.workdir, self.sane_filename)
        self.tmp = "%s.%d.tmp" % (self.main, os.getpid())
        self.old = "%s.old" % (self.main)
        self.lock = "%s.lock" % (self.main)

    def suffixed(self, suffix):
        return SafeFile(self.workdir, self.identifier, suffix='.' + suffix)

    def main2old(self):
        try:
            os.unlink(self.old)
            shutil.copy2(self.main, self.old)
        except:
            pass

    def tmp2main(self):
        self.main2old()
        os.rename(self.tmp, self.main)

    def tmpclean(self):
        try:
            os.remove(self.tmp)
            print("Removed %s" % self.tmp)
        except:
            pass

    def main2tmp(self):
        if os.path.isfile(self.main):
            shutil.copy2(self.main, self.tmp)

    def remove_main(self):
        self.main2old()
        self.tmpclean()
        try:
            os.remove(self.main)
        except:
            pass




workdir = os.path.abspath(options.workdir)
safefile = SafeFile(workdir, filename)
files = {
    'offset':   safefile.suffixed('offset'),
    'status':   safefile.suffixed('status'),
    'lock':     safefile.suffixed('lock')
}

if options.startover:
    files['offset'].remove_main()
    files['status'].remove_main()
    files['lock'].remove_main()

# Check for lock file so we don't run multiple copies of the same parser
# simultaneuosly. This will happen if the log parsing takes more time than
# the cron period.
try:
    lockfile = start_locking(files['lock'].main)
except LockingError as e:
    #logger.warning(str(e))
    print("Locking error: ", str(e))
    sys.exit(1)


def cleanup():
    files['offset'].tmpclean()
    files['status'].tmpclean()
    end_locking(lockfile, files['lock'].main)

def finalize():
    try:
        files['offset'].tmp2main()
        files['status'].tmp2main()
    except:
        cleanup()
        raise

res = False
try:
    influxdb = influxdb_client(options)

    files['offset'].main2tmp()

    pygtail = Pygtail(filename, offset_file=files['offset'].tmp)

    try:
        status = load_obj(files['status'].main)
    except IOError:
        status = {}

    if not 'last_msec' in status:
        status['last_msec'] = 0
        status['leftover'] = None
        status['bucket_duration'] = options.bucket_duration
        status['lookback_factor'] = options.lookback_factor

    if (status['leftover'] is not None and len(status['leftover']) > 0):
        exit = False
        if status['bucket_duration'] != options.bucket_duration:
            print("Error: bucket duration mismatch %d vs %d (set via option)" %
                  (status['bucket_duration'], options.bucket_duration))
            exit = True
        if status['lookback_factor'] != options.lookback_factor:
            print("Error: lookback factor mismatch %d vs %d (set via option)" %
                  (status['lookback_factor'], options.lookback_factor))
            exit = True
        if exit:
            print("Exiting. If you know what you are doing, remove status file %s" % files['status'].main)
            sys.exit(1)

    mbs, leftover, last_msec = parsefile(pygtail, status, options)
    status['leftover'] = leftover
    status['last_msec'] = last_msec

    points = mbs2influx(mbs, status)
    if points:
        tags = {
            'host': options.hostname,
            'name': options.name or filename,
        }
        if options.datacenter:
            tags['dc'] = options.datacenter
        influxdb_send(influxdb, points, tags)

    save_obj(status, files['status'].tmp)
except:
    cleanup()
    raise
else:
    finalize()
finally:
    try:
        end_locking(lockfile, files['lock'].main)
    except Exception as e:
        pass
