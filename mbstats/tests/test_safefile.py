import tempfile
import unittest

from mbstats.safefile import SafeFile


class TestSafeFile(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.safefile = SafeFile(self.test_dir.name, 'test')

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()

    def test_safe_file(self):
        attrs = ('old', 'tmp', 'main', 'lock')
        payloads = {}

        def read_data(attr):
            try:
                datafile = getattr(self.safefile, attr)
                with open(datafile, 'rb') as f:
                    return f.read()
            except FileNotFoundError:
                return b''

        def write_data(attr):
            payloads[attr] = bytes(attr, 'UTF-8')
            datafile = getattr(self.safefile, attr)
            with open(datafile, 'wb') as f:
                f.write(payloads[attr])

        for attr in attrs:
            write_data(attr)
            read_payload = read_data(attr)
            self.assertEqual(payloads[attr], read_payload, attr)

        self.safefile.rename_tmp_to_main()
        expect = {
            'old': b'main',
            'tmp': b'',
            'main': b'tmp',
            'lock': b'lock',
        }
        for attr in attrs:
            read_payload = read_data(attr)
            self.assertEqual(expect[attr], read_payload, attr)

        self.safefile.copy_main_to_tmp()
        expect = {
            'old': b'main',
            'tmp': b'tmp',
            'main': b'tmp',
            'lock': b'lock',
        }
        for attr in attrs:
            read_payload = read_data(attr)
            self.assertEqual(expect[attr], read_payload, attr)

        self.safefile.remove_tmp()
        expect = {
            'old': b'main',
            'tmp': b'',
            'main': b'tmp',
            'lock': b'lock',
        }
        for attr in attrs:
            read_payload = read_data(attr)
            self.assertEqual(expect[attr], read_payload, attr)

        self.safefile.remove_main()
        expect = {
            'old': b'tmp',
            'tmp': b'',
            'main': b'',
            'lock': b'lock',
        }
        for attr in attrs:
            read_payload = read_data(attr)
            self.assertEqual(expect[attr], read_payload, attr)


if __name__ == '__main__':
    unittest.main()
