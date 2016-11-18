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


def parsefile(pygtail, status, maxlines = 1000, bucket_secs=60,
              lookbackfactor=2):
    # lines are logged when request ends, which means they can be unordered
    if status['last_msec']:
        ignore_before = status['last_msec'] - bucket_secs * lookbackfactor
    else:
        ignore_before = 0
    max_msec = 0
    skipped = 0
    storage = defaultdict(list)
    for line in pygtail:
        try:
            items = line.rstrip('\r\n').split('|')
            msec = float(items[pos_msec])
            if msec <= ignore_before:
                # skip unordered & old entries
                skipped += 1
                continue
            if msec > max_msec:
                max_msec = msec
            bucket = int(math.ceil(float(msec)/bucket_secs))

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
        except ValueError as e:
            print(e, line)
            print(row)
            raise
        maxlines -= 1
        if maxlines == 0:
            break
    if skipped:
        print("Skipped %d unordered lines" % skipped)
    pygtail._update_offset_file()
    return (storage, max_msec)

#@profile
def parse_storage(storage, status):
    for k in storage:
        print("storage", bucket2time(k, status), len(storage[k]))

    if status['leftover'] is not None:
        previous_leftover = status['leftover']
        for k in previous_leftover:
            print ("Previous leftover bucket: %s %d" % (bucket2time(k, status), len(previous_leftover[k])))
            if k in storage:
                storage[k][0:0] = previous_leftover[k]
            else:
                storage[k] = previous_leftover[k]
    for k in storage:
        print("storage+leftover", bucket2time(k, status), len(storage[k]))

    leftover = defaultdict(list)
    n_buckets = len(storage)
    print("Storage n_buckets: %d" % n_buckets)
    if not n_buckets:
        return (dict(), leftover)
    if n_buckets <= status['lookbackfactor']:
        return (dict(), storage)

    n = status['lookbackfactor']
    while n > 0:
        last_bucket = max(storage)
        leftover[last_bucket] = list(storage[last_bucket])
        del storage[last_bucket]
        n -= 1

    for k in leftover:
        print ("Leftover bucket: %s %d" % (bucket2time(k, status), len(leftover[k])))

    print("Storage %s -> %s" % (bucket2time(min(storage), status),
                                bucket2time(max(storage), status)))
    return (calculate_full_buckets(storage, status), leftover)

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

#@profile
def calculate_full_buckets(storage, status):
    mbs = mbsdict()

    for k in storage:
        print("bucket=%d len=%d" % (k, len(storage[k])))
        for r in storage[k]:
#    mbs['hits']:
#        tags: vhost, protocol, loc
#        value: count
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['hits'][tags] += 1

#    mbs['bytes_sent']:
#        tags: vhost, protocol, loc
#        value: sum
#
#            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['bytes_sent'][tags] += r['bytes_sent']


#    mbs['gzip_count']:
#        tags: vhost, protocol, loc
#        value: count
#            tags = (k, r['vhost'], r['protocol'], r['loctag'])
#           mbs['gzip_count'][tags] += ('gzip_ratio' in r)

#    mbs['gzip_ratio']:
#        tags: vhost, protocol, loc
#        value: sum of gzip ratio / number of gzipped requests
            if 'gzip_ratio' in r:
                mbs['gzip_count'][tags] += 1
#                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['_gzip_ratio_premean'][tags] += r['gzip_ratio']

#
#    mbs['request_length']:
#        tags: vhost, protocol, loc
#        value: sum of request_length / hits
#
#            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['_request_length_premean'][tags] += r['request_length']

#    mbs['request_time']:
#        tags: vhost, protocol, loc
#        value: sum of request_time / hits
#
#            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['_request_time_premean'][tags] += r['request_time']

#    mbs['status']:
#        tags: vhost, protocol, loc,  status
#        value: count
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'], r['status'])
            mbs['status'][tags] += 1

##### upstreams

            if 'upstreams' in r:

#    mbs['upstreams_hits']:
#        tags: vhost, protocol, loc, upstream
#        value: count
#
                for upstream in r['upstreams']['servers']:
                    tags = (k, r['vhost'], r['protocol'], r['loctag'], upstream)
                    mbs['upstreams_hits'][tags] += 1

#    mbs['upstreams_response_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of response_time / hits

               # for upstream in r['upstreams']['servers']:
              #      tags = (k, r['vhost'], r['protocol'], r['loctag'], upstream)
                    mbs['_upstreams_response_time_premean'][tags] += r['upstreams']['response_time'][upstream]

#
#    mbs['upstreams_connect_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of connect_time / hits
#
              #  for upstream in r['upstreams']['servers']:
               #     tags = (k, r['vhost'], r['protocol'], r['loctag'], upstream)
                    mbs['_upstreams_connect_time_premean'][tags] += r['upstreams']['connect_time'][upstream]


#    mbs['upstreams_header_time']:
#        tags: vhost, protocol, loc, upstream
#        value: sum of header_time / hits
              #  for upstream in r['upstreams']['servers']:
               #     tags = (k, r['vhost'], r['protocol'], r['loctag'], upstream)
                    mbs['_upstreams_header_time_premean'][tags] += r['upstreams']['header_time'][upstream]


#    mbs['upstreams_status']:
#        tags: vhost, protocol, loc, upstream, status
#        value: count
                #for upstream in r['upstreams']['servers']:
                    for status in r['upstreams']['status'][upstream]:
                        tags = (k, r['vhost'], r['protocol'], r['loctag'],
                                upstream, status)
                        mbs['upstreams_status'][tags] += 1

#
#    mbs['upstreams_servers_contacted']:
#        tags: vhost, protocol, loc
#        value: sum of servers_contacted
#
                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['upstreams_servers_contacted'][tags] += r['upstreams']['servers_contacted']


#    mbs['upstream_internal_redirects']:
#        tags: vhost, protocol, loc
#        value: sum of internal_redirects
#
#                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['upstreams_internal_redirects'][tags] += r['upstreams']['internal_redirects']

#    mbs['upstreams_servers_count']:
#        tags: vhost, protocol, loc
#        value: sum of len of servers
#
#                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['upstreams_servers'][tags] += len(r['upstreams']['servers'])

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

    return mbs


def save_obj(obj, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(filepath):
    with open(filepath, 'rb') as f:
        return pickle.load(f)


def bucket2time(bucket, status):
    d = datetime.datetime.utcfromtimestamp(
            bucket*status['bucket_secs']
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

# TODO: influxdb params
def influxdb_client(host='localhost',
                    port=8086,
                    username='root',
                    password='root',
                    database='mbstats',
                    timeout=40,
                    deletedatabase=False):
    client = InfluxDBClient(host=host,
                            port=port,
                            username=username,
                            password=password,
                            database=database,
                            timeout=timeout)
    if deletedatabase:
        client.drop_database(database)
    client.create_database(database)
    return client

def influxdb_send(client, points, tags, batch_size=200):
    npoints = len(points)
    if npoints:
        print("Sending %d points" % npoints)
        print(points[0])
        return client.write_points(points, tags=tags, time_precision='m', batch_size=batch_size)
    return True

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-s', '--start', type=int)
parser.add_argument('-m', '--maxlines', default=0, type=int)
parser.add_argument('-w', '--workdir', default='.')
parser.add_argument('-H', '--hostname', default=platform.node())
parser.add_argument('-l', '--logname', default='')
parser.add_argument('-d', '--datacenter', default='')
parser.add_argument('--deletedatabase', action='store_true', default=False)
parser.add_argument('--locker', choices=('fcntl', 'portalocker'), default='fcntl')
parser.add_argument('-b', '--lookbackfactor', type=int, default=2)
parser.add_argument('--startover', action='store_true', default=False)


args = parser.parse_args()
print(args)
if args.locker == 'portalocker':
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
        if args.locker == 'portalocker':
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
        if args.locker == 'portalocker':
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



filename = args.file

workdir = os.path.abspath(args.workdir)
safefile = SafeFile(workdir, filename)
files = {
    'offset':   safefile.suffixed('offset'),
    'leftover': safefile.suffixed('leftover'),
    'lock':     safefile.suffixed('lock')
}

if args.startover:
    files['offset'].remove_main()
    files['leftover'].remove_main()
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
    files['leftover'].tmpclean()
    end_locking(lockfile, files['lock'].main)

def finalize():
    try:
        files['offset'].tmp2main()
        files['leftover'].tmp2main()
    except:
        cleanup()
        raise

bucket_secs = 60
lookbackfactor = 2
res = False
try:
    influxdb = influxdb_client(deletedatabase=args.deletedatabase)

    files['offset'].main2tmp()

    pygtail = Pygtail(filename, offset_file=files['offset'].tmp)

    try:
        status = load_obj(files['leftover'].main)
    except IOError:
        status = {
            'last_msec': 0,
            'leftover' : None
        }

    status['bucket_secs'] = bucket_secs
    status['lookbackfactor'] = lookbackfactor
    storage, max_msec = parsefile(pygtail, status, maxlines=args.maxlines)
    mbs, leftover = parse_storage(storage, status)
    status['leftover'] = leftover
    status['last_msec'] = max_msec

    points = mbs2influx(mbs, status)
    if points:
        tags = {
            'host': args.hostname,
            'logname': args.logname or filename,
        }
        if args.datacenter:
            tags['dc'] = args.datacenter
        influxdb_send(influxdb, points, tags)

    save_obj(status, files['leftover'].tmp)
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
