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

from mbstats.backends import (
    Backend,
    BackendDryRun,
)
from mbstats.utils import bucket2time


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

    def send_points(self, tags, points=None):
        options = self.options
        logger = self.logger
        if points is None:
            points = self.points
        if points:
            if logger:
                if options.quiet < 2:
                    logger.info("Sending %d points" % len(points))
            if not self.client:
                raise BackendDryRun(points)
            for point in points:
                if point['measurement'] == 'request_length_mean':
                    # workaround for int vs float type issue
                    val = point['fields']['value']
                    if isinstance(val, str):
                        newval = ''
                        for c in val:
                            if c.isdigit():
                                newval += c
                        val = newval
                    if not isinstance(val, int):
                        point['fields']['value'] = int(val)
            return self.client.write_points(points, tags=tags, time_precision='m',
                                            batch_size=options.influx_batch_size)
        return True

    def add_points(self, mbs, status):
        self.points = []
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
                fields = {'value': value}
                self.points.append({
                    "measurement": measurement,
                    "tags": influxtags,
                    "time": bucket2time(tags[0], status['bucket_duration']),
                    "fields": fields
                })
