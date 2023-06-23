# -*- coding: utf-8 -*-

#
# mbstats
#
# Tails a log and applies mbstats parser, then reports metrics to InfluxDB
#
# Usage:
#
# $ mbstats [options]
#
# Help:
#
# $ mbstats -h
#
#
# Copyright 2016-2023, MetaBrainz Foundation
# Author: Laurent Monin
#
# mbstats is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# mbstats is distributed in the hope that it will be useful,
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

import time

from mbstats.backends import (
    Backend,
    BackendDryRun,
)
from mbstats.utils import bucket2time, timestamp_RFC3339


try:
    from influxdb import InfluxDBClient
    has_influxdb = True
except ImportError:
    has_influxdb = False


MBS_TAGS = {
    'hits': ('vhost', 'protocol', 'loctag'),
    'hits_with_upstream': ('vhost', 'protocol', 'loctag'),
    'status': ('vhost', 'protocol', 'loctag', 'status'),
    'bytes_sent': ('vhost', 'protocol', 'loctag'),
    'gzip_count': ('vhost', 'protocol', 'loctag'),
    'gzip_count_percent': ('vhost', 'protocol', 'loctag'),
    'gzip_ratio_mean': ('vhost', 'protocol', 'loctag'),
    'request_length_mean': ('vhost', 'protocol', 'loctag'),
    'request_time_mean': ('vhost', 'protocol', 'loctag'),
    'upstreams_hits': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_status': ('vhost', 'protocol', 'loctag', 'upstream', 'status'),
    'upstreams_servers_contacted_per_hit': ('vhost', 'protocol', 'loctag'),
    'upstreams_internal_redirects_per_hit': ('vhost', 'protocol', 'loctag'),
    'upstreams_servers': ('vhost', 'protocol', 'loctag'),
    'upstreams_response_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_connect_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
    'upstreams_header_time_mean': ('vhost', 'protocol', 'loctag', 'upstream'),
}


def _process_request_length_mean_value(value):
    # workaround for int vs float type issue
    val = value
    if isinstance(val, str):
        newval = ''
        for c in val:
            if c.isdigit():
                newval += c
        val = newval
    if not isinstance(val, int):
        return int(val)
    return value


PROCESS_MEASUREMENT_VALUE = {
    'request_length_mean': _process_request_length_mean_value,
}


class InfluxBackend(Backend):
    def __init__(self, options, logger=None):
        if not has_influxdb:
            options.dry_run = True
        super().__init__(options, logger=logger)

    def initialize(self):
        options = self.options
        if options.dry_run:
            return
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
        self.client = client

    def send_points(self, tags=None, points=None, batch_size=None):
        options = self.options
        logger = self.logger
        if batch_size is None:
            batch_size = options.influx_batch_size
        if points is None:
            points = self.points
        if points:
            if logger:
                if options.quiet < 2:
                    logger.info("Sending %d points" % len(points))
            if not self.client:
                raise BackendDryRun(points)
            return self.client.write_points(points, tags=tags, time_precision='m',
                                            batch_size=batch_size)
        return True

    @staticmethod
    def point_dict(measurement, fields, tags=None, time=None):
        return {
            "measurement": measurement,
            "tags": tags or {},
            "time": time or timestamp_RFC3339(time.time()),
            "fields": fields,
        }

    def _add_points(self, mbs, status):
        for measurement, tagnames in list(MBS_TAGS.items()):
            if measurement not in mbs:
                continue
            for tags, value in list(mbs[measurement].items()):
                influxtags = dict(list(zip(tagnames, tags[1:])))
                for k, v in list(influxtags.items()):
                    if k == 'protocol':
                        if v == 's':
                            influxtags[k] = 'https'
                        else:
                            influxtags[k] = 'http'
                    influxtags[k] = str(v)
                if measurement in PROCESS_MEASUREMENT_VALUE:
                    value = PROCESS_MEASUREMENT_VALUE[measurement](value)
                yield self.point_dict(
                    measurement,
                    {'value': value},
                    tags=influxtags,
                    time= bucket2time(tags[0], status['bucket_duration']),
                )

    def add_points(self, mbs, status):
        self.points = list(self._add_points(mbs, status))
