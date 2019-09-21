import os.path
import sys
import unittest
import tempfile


from mbstats.app import Locker, LockingError

try:
    import portalocker
    has_portalocker = True
except ImportError:
    has_portalocker = False


class TestLocker(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.lock_path = os.path.join(self.test_dir.name, 'testlock')

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()

    def test_lock_fcntl(self):
        locker = Locker(self.lock_path, lock_type='fcntl')
        locker.unlock()

    def test_lock_fcntl_fail(self):
        locker = Locker(self.lock_path, lock_type='fcntl')
        with self.assertRaises(LockingError):
            locker2 = Locker(self.lock_path, lock_type='fcntl')
        locker.unlock()

    @unittest.skipIf(not has_portalocker, 'No portalocker module')
    def test_lock_portalocker(self):
        locker = Locker(self.lock_path, lock_type='portalocker')
        locker.unlock()

    @unittest.skipIf(not has_portalocker, 'No portalocker module')
    def test_lock_portalocker_fail(self):
        locker = Locker(self.lock_path, lock_type='portalocker')
        with self.assertRaises(LockingError):
            locker2 = Locker(self.lock_path, lock_type='portalocker')
        locker.unlock()


if __name__ == '__main__':
    unittest.main()
