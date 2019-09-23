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

import fcntl
import os.path
import platform


try:
    import portalocker
    has_portalocker = True
except ImportError:
    has_portalocker = False


class LockingError(Exception):
    """ Exception raised for errors creating or destroying lockfiles. """


class Locker:

    def __init__(self, lockfile_path, lock_type='fcntl', logger=None):
        self.lockfile_path = lockfile_path
        self.lock_type = lock_type
        self.logger = logger
        self.logfile_fd = None
        self.lock()

    def lock(self):
        """ Acquire a lock via a provided lockfile filename. """
        if os.path.exists(self.lockfile_path):
            raise LockingError("Lock file (%s) already exists." % self.lockfile_path)

        try:
            self.lockfile_fd = open(self.lockfile_path, 'w')
        except Exception as e:
            raise LockingError("Lock file (%s) creation failed: %s" %
                               (self.lockfile_path, e))

        if has_portalocker and self.lock_type == 'portalocker':
            try:
                portalocker.lock(self.lockfile_fd, portalocker.LOCK_EX | portalocker.LOCK_NB)
                self.lockfile_fd.write("%s" % os.getpid())
            except portalocker.LockException as e:
                raise LockingError("Cannot acquire lock on (%s): %s" %
                                   (self.lockfile_path, e))
        else:
            try:
                fcntl.flock(self.lockfile_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.lockfile_fd.write("%s" % os.getpid())
            except IOError as e:
                raise LockingError("Cannot acquire lock on (%s): %s" %
                                   (self.lockfile_path, e))

        if self.logger is not None:
            self.logger.debug("Locking successful (%s): %s" % (self.lock_type,
                                                               self.lockfile_path))

    def unlock(self):
        """ Release a lock via a provided file descriptor. """

        if self.lockfile_fd is None:
            if self.logger is not None:
                self.logger.debug("Nothing to unlock")
            return

        if has_portalocker and self.lock_type == 'portalocker':
            try:
                # uses fcntl.LOCK_UN on posix (in contrast with the flock()ing below)
                portalocker.unlock(self.lockfile_fd)
            except portalocker.LockException as e:
                raise LockingError("Cannot release lock on (%s): %s" %
                                   (self.lockfile_path, e))
        else:
            try:
                if platform.system() == "SunOS":  # GH issue #17
                    fcntl.flock(self.lockfile_fd, fcntl.LOCK_UN)
                else:
                    fcntl.flock(self.lockfile_fd, fcntl.LOCK_UN | fcntl.LOCK_NB)
            except IOError as e:
                raise LockingError("Cannot release lock on (%s): %s" %
                                   (self.lockfile_path, e))

        try:
            self.lockfile_fd.close()
            os.unlink(self.lockfile_path)
            self.lockfile_fd = None
        except OSError as e:
            raise LockingError("Cannot unlink %s: %s" % (self.lockfile_path, e))

        if self.logger is not None:
            self.logger.debug("Unlocking successful")
