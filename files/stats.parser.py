from collections import defaultdict
from influxdb import InfluxDBClient
from pygtail import Pygtail
import argparse
#import cPickle as pickle
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
#    chains = {}

    def split_upstream(s):
        return [x.split(' : ') for x in s.split(", ")]

    splitted = split_upstream(row['upstream_addr'])
    r['servers_contacted'] = len(splitted)
    r['internal_redirects'] = len([x for x in splitted if len(x) > 1])
    upstream_addr = list(itertools.chain.from_iterable(splitted))

    upstream_status = list(itertools.chain.from_iterable(split_upstream(row['upstream_status'])))
    upstream_response_time = \
        list(itertools.chain.from_iterable(split_upstream(row['upstream_response_time'])))
    upstream_header_time = \
        list(itertools.chain.from_iterable(split_upstream(row['upstream_header_time'])))
    upstream_connect_time = \
        list(itertools.chain.from_iterable(split_upstream(row['upstream_connect_time'])))
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
              unordereddelay=2):
    # lines are logged when request ends, which means they can be unordered
    if status['last_msec']:
        ignore_before = status['last_msec'] - bucket_secs * unordereddelay
    else:
        ignore_before = 0
    max_msec = 0
    storage = defaultdict(list)
    for line in pygtail:
        try:
            items = line.rstrip('\r\n').split('|')
            row = dict()
            row['msec'] = float(items[pos_msec])
            if row['msec'] <= ignore_before:
                # skip unordered & old entries
                continue
            if row['msec'] > max_msec:
                max_msec = row['msec']
            bucket = int(math.ceil(float(row['msec'])/bucket_secs))

            row['vhost'] = items[pos_vhost]
            row['protocol'] = items[pos_protocol]
            row['loctag'] = items[pos_loctag]

            row['status'] = int(items[pos_status])
            row['bytes_sent'] = int(items[pos_bytes_sent])
            row['request_length'] = int(items[pos_request_length])

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
    n_buckets = len(storage.keys())
    print("Storage n_buckets: %d" % n_buckets)
    if not n_buckets:
        return (dict(), leftover)
    if n_buckets <= status['unordereddelay']:
        return (dict(), storage)

    n = status['unordereddelay']
    while n > 0:
        last_bucket = max(storage.keys())
        leftover[last_bucket] = list(storage[last_bucket])
        del storage[last_bucket]
        n -= 1

    for k in leftover:
        print ("Leftover bucket: %s %d" % (bucket2time(k, status), len(leftover[k])))

    print("Storage %s -> %s" % (bucket2time(min(storage.keys()), status),
                                bucket2time(max(storage.keys()), status)))
    return (calculate_full_buckets(storage, status), leftover)

#@profile
def calculate_full_buckets(storage, status):
    mbs = dict()
    mbs['hits'] = defaultdict(int)
    mbs['status'] = defaultdict(int)
    mbs['bytes_sent'] = defaultdict(int)
    mbs['gzip_count'] = defaultdict(int)
    mbs['_gzip_ratio_premean'] = defaultdict(float)
    mbs['_request_length_premean'] = defaultdict(int)
    mbs['_request_time_premean'] = defaultdict(float)
    mbs['upstreams_hits'] = defaultdict(int)
    mbs['upstreams_status'] = defaultdict(int)
    mbs['upstreams_servers_contacted'] = defaultdict(int)
    mbs['upstreams_internal_redirects'] = defaultdict(int)
    mbs['upstreams_servers'] = defaultdict(int)
    mbs['_upstreams_response_time_premean'] = defaultdict(float)
    mbs['_upstreams_connect_time_premean'] = defaultdict(float)
    mbs['_upstreams_header_time_premean'] = defaultdict(float)

    mbs['gzip_percent'] = defaultdict(float)
    mbs['gzip_ratio_mean'] = defaultdict(float)
    mbs['request_length_mean'] = defaultdict(float)
    mbs['request_time_mean'] = defaultdict(float)
    mbs['upstreams_response_time_mean'] = defaultdict(float)
    mbs['upstreams_connect_time_mean'] = defaultdict(float)
    mbs['upstreams_header_time_mean'] = defaultdict(float)

    for k in sorted(storage.keys()):
        print("bucket=%d len=%d" % (k, len(storage[k])))
        for r in storage[k]:
#    mbs['hits']:
#        tags: vhost, protocol, loc
#        value: count
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['hits'][tags] += 1

#    mbs['status']:
#        tags: vhost, protocol, loc,  status
#        value: count
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'], r['status'])
            mbs['status'][tags] += 1

#    mbs['bytes_sent']:
#        tags: vhost, protocol, loc
#        value: sum
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['bytes_sent'][tags] += r['bytes_sent']


#    mbs['gzip_count']:
#        tags: vhost, protocol, loc
#        value: count
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['gzip_count'][tags] += ('gzip_ratio' in r)

#    mbs['gzip_ratio']:
#        tags: vhost, protocol, loc
#        value: sum of gzip ratio / number of gzipped requests
            if 'gzip_ratio' in r:
                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['_gzip_ratio_premean'][tags] += r['gzip_ratio']

#
#    mbs['request_length']:
#        tags: vhost, protocol, loc
#        value: sum of request_length / hits
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['_request_length_premean'][tags] += r['request_length']

#    mbs['request_time']:
#        tags: vhost, protocol, loc
#        value: sum of request_time / hits
#
            tags = (k, r['vhost'], r['protocol'], r['loctag'])
            mbs['_request_time_premean'][tags] += r['request_time']

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
                tags = (k, r['vhost'], r['protocol'], r['loctag'])
                mbs['upstreams_internal_redirects'][tags] += r['upstreams']['internal_redirects']


#    mbs['upstreams_servers_count']:
#        tags: vhost, protocol, loc
#        value: sum of len of servers
#
                tags = (k, r['vhost'], r['protocol'], r['loctag'])
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

###### calculations of percentage

        if mbs['hits']:
            for k, v in mbs['hits'].items():
                if mbs['gzip_count'] and v:
                    mbs['gzip_percent'][k] = (mbs['gzip_count'][k] * 1.0) / v
                else:
                    mbs['gzip_percent'][k] = 0.0
    return mbs

def save_obj(obj, filepath):
    with gzip.GzipFile(filepath, 'wb') as f:
        pickle.dump(obj, f)

def load_obj(filepath):
    with gzip.GzipFile(filepath, 'rb') as f:
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
parser.add_argument('-w', '--writestatusdir', default='.')
parser.add_argument('-H', '--hostname', default=platform.node())
parser.add_argument('-l', '--logname', default='')
parser.add_argument('-d', '--datacenter', default='')
parser.add_argument('--deletedatabase', action='store_true', default=False)
parser.add_argument('--locker', choices=('fcntl', 'portalocker'), default='fcntl')
parser.add_argument('-u', '--unordereddelay', type=int, default=2)

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

filename = args.file

statusdir = os.path.abspath(args.writestatusdir)
sane_filename = re.sub(r'\W', '_', filename)
basepath = os.path.join(statusdir, sane_filename)
filepath_offset = basepath + '.offset'
filepath_status =  basepath + '.leftover'
pid = os.getpid()
fmt = "%s.%d.tmp"
filepath_offset_tmp = fmt % (filepath_offset, pid)
filepath_status_tmp =  fmt % (filepath_status, pid)
filepath_offset_old = filepath_offset + '.old'
filepath_status_old =  filepath_status + '.old'

lock_file = basepath + '.lock'

# Check for lock file so we don't run multiple copies of the same parser
# simultaneuosly. This will happen if the log parsing takes more time than
# the cron period.
try:
    lockfile = start_locking(lock_file)
except LockingError as e:
    #logger.warning(str(e))
    print("Locking error: ", str(e))
    sys.exit(1)


def cleanup():
    try:
        os.remove(filepath_offset_tmp)
        print("Removed %s" % filepath_offset_tmp)
    except:
        pass
    try:
        os.remove(filepath_status_tmp)
        print("Removed %s" % filepath_status_tmp)
    except:
        pass
    end_locking(lockfile, lock_file)

def finalize():
    try:
        try:
            os.unlink(filepath_offset_old)
            shutil.copy2(filepath_offset, filepath_offset_old)
        except:
            pass
        try:
            os.unlink(filepath_status_old)
            shutil.copy2(filepath_status, filepath_status_old)
        except:
            pass
        os.rename(filepath_offset_tmp, filepath_offset)
        os.rename(filepath_status_tmp, filepath_status)
    except:
        cleanup()
        raise

bucket_secs = 60
unordereddelay = 2
res = False
try:
    influxdb = influxdb_client(deletedatabase=args.deletedatabase)

    if os.path.isfile(filepath_offset):
        shutil.copy2(filepath_offset, filepath_offset_tmp)
    pygtail = Pygtail(filename, offset_file=filepath_offset_tmp)

    try:
        status = load_obj(filepath_status)
    except IOError:
        status = {
            'last_msec': 0,
            'leftover' : None
        }

    status['bucket_secs'] = bucket_secs
    status['unordereddelay'] = unordereddelay
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

    save_obj(status, filepath_status_tmp)
except:
    cleanup()
    raise
else:
    finalize()

try:
    end_locking(lockfile, lock_file)
except Exception as e:
    pass
