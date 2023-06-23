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
import datetime
import inspect
import json
import math
import pickle


# This provides a lineno() function to make it easy to grab the line
# number that we're on (for logging)
# Danny Yoo (dyoo@hkn.eecs.berkeley.edu)
# taken from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/145297


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


def save_obj(obj, filepath, logger=None):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        if logger is not None:
            logger.debug("save_obj(): saved to %r" % filepath)


def load_obj(filepath, logger=None):
    with open(filepath, 'rb') as f:
        if logger is not None:
            logger.debug("load_obj(): loading from %r" % filepath)
        return pickle.load(f)


def timestamp_RFC3339(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).isoformat() + 'Z'


def bucket2time(bucket, bucket_duration):
    return timestamp_RFC3339(bucket * bucket_duration)


def msec2bucket(msec, bucket_duration):
    return int(math.ceil(float(msec) / float(bucket_duration)))


def _read_config(conf_path):
    with open(conf_path, 'r') as f:
        return json.load(f)


def read_config(conf_path, defaults_options):
    config = _read_config(conf_path)
    for k in config:
        if k not in defaults_options:
            continue
        if k == 'config':
            continue
        if isinstance(defaults_options[k], bool):
            if not isinstance(config[k], bool):
                if isinstance(config[k], int):
                    config[k] = config[k] != 0
                else:
                    config[k] = config[k].lower() != 'false'
        elif isinstance(defaults_options[k], int):
            if not isinstance(config[k], int):
                try:
                    config[k] = int(config[k])
                except ValueError:
                    continue
        defaults_options[k] = config[k]
