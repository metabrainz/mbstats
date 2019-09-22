import sys
import tempfile
import unittest

from mbstats.app import main


class TestParser(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()

    def test_A(self):
        f = None
        with self.assertRaises(SystemExit):
            import contextlib, io

            f = io.StringIO()
            with contextlib.redirect_stdout(f):
                sys.argv = ['testing', '--dump-config', '--dry-run']
                main()
        if f:
            output = f.getvalue()
            self.assertIn('"dry_run": true', output)


if __name__ == '__main__':
    unittest.main()
