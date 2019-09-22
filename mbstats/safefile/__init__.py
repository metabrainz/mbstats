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
import os.path
import re
import shutil
from uuid import uuid1


class SafeFile(object):
    def __init__(self, workdir, identifier, suffix='', logger=None):
        self.identifier = identifier
        self.suffix = suffix
        self.sane_filename = re.sub(r'\W', '_', self.identifier + self.suffix)
        self.workdir = workdir
        self.main = os.path.join(self.workdir, self.sane_filename)
        self.tmp = "%s.%s.tmp" % (self.main, uuid1().hex)
        self.old = "%s.old" % (self.main)
        self.lock = "%s.lock" % (self.main)
        self.logger = logger

    def suffixed(self, suffix):
        return SafeFile(self.workdir, self.identifier, suffix='.' + suffix)

    def main2old(self):
        try:
            if os.path.isfile(self.old):
                os.unlink(self.old)
            shutil.copy2(self.main, self.old)
            if self.logger:
                self.logger.debug("main2old(): Copied %r to %r" % (self.main, self.old))
        except Exception as e:
            if self.logger:
                self.logger.warning("main2old() failed: %r -> %r %s" % (self.main, self.old, e))
            pass

    def tmp2main(self):
        try:
            self.main2old()
            os.rename(self.tmp, self.main)
            if self.logger:
                self.logger.debug("tmp2main(): Renamed %r to %r" % (self.tmp, self.main))
        except Exception as e:
            if self.logger:
                self.logger.error("tmp2main(): failed: %r -> %r %s" % (self.tmp, self.main, e))
            raise

    def tmpclean(self):
        if os.path.isfile(self.tmp):
            try:
                os.remove(self.tmp)
                if self.logger:
                    self.logger.debug("tmpclean(): Removed %r" % self.tmp)
            except Exception as e:
                if self.logger:
                    self.logger.error("tmpclean(): failed: %r %s" % (self.tmp, e))
                pass

    def main2tmp(self):
        if os.path.isfile(self.main):
            try:
                shutil.copy2(self.main, self.tmp)
                if self.logger:
                    self.logger.debug("main2tmp(): Copied %r to %r" % (self.main, self.tmp))
            except Exception as e:
                if self.logger:
                    self.logger.error("main2tmp(): failed: %r -> %r %s" % (self.main, self.tmp, e))
                raise

    def remove_main(self):
        self.main2old()
        self.tmpclean()
        try:
            os.remove(self.main)
            if self.logger:
                self.logger.debug("remove_main(): Removed %r" % (self.main))
        except Exception as e:
            if self.logger:
                self.logger.error("remove_main(): failed: %r %s" % (self.main, e))
            pass