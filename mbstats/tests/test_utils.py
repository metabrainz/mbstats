import json
from json.decoder import JSONDecodeError
import os.path
import tempfile
import unittest

from mbstats.utils import (
    _read_config,
    bucket2time,
    lineno,
    load_obj,
    msec2bucket,
    read_config,
    save_obj,
)


class TestUtils(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()

    def test_lineno(self):
        self.assertEqual(lineno(), 28)  #  if this line moves, change the number

    def test_save_load_obj(self):
        obj = {'test': 666}
        filepath = os.path.join(self.test_dir.name, 'testfile')
        save_obj(obj, filepath)
        read_obj = load_obj(filepath)
        self.assertEqual(obj, read_obj)

    def test_read_json_config(self):
        conf_file = os.path.join(self.test_dir.name, 'config')
        payload = '{a: 1}'
        with open(conf_file, 'w') as f:
            f.write(payload)

        with self.assertRaises(JSONDecodeError):
            config = _read_config(conf_file)

        payload = '{"a": 1}'
        with open(conf_file, 'w') as f:
            f.write(payload)
        config = json.dumps(_read_config(conf_file))
        self.assertEqual(payload, config)

    def test_read_config1(self):
        defaults_options = {
            'intval': 2,
            'strval': "abc",
            'boolval': True,
        }
        conf_file = os.path.join(self.test_dir.name, 'config')
        payload = '{"intval": 1, "strval": "def", "boolval": false}'
        with open(conf_file, 'w') as f:
            f.write(payload)

        read_config(conf_file, defaults_options)
        self.assertEqual(defaults_options['intval'], 1)
        self.assertEqual(defaults_options['strval'], 'def')
        self.assertEqual(defaults_options['boolval'], False)

    def test_read_config2(self):
        defaults_options = {
            'intval': 2,
            'boolval': True,
        }
        conf_file = os.path.join(self.test_dir.name, 'config')
        payload = '{"intval": "1", "boolval": "FaLsE"}'
        with open(conf_file, 'w') as f:
            f.write(payload)

        read_config(conf_file, defaults_options)
        self.assertEqual(defaults_options['intval'], 1)
        self.assertEqual(defaults_options['boolval'], False)

    def test_read_config3(self):
        defaults_options = {
            'boolval': False,
        }
        conf_file = os.path.join(self.test_dir.name, 'config')
        payload = '{"boolval": "TRUE"}'
        with open(conf_file, 'w') as f:
            f.write(payload)

        read_config(conf_file, defaults_options)
        self.assertEqual(defaults_options['boolval'], True)

    def test_bucket2time(self):
        msec = 1568962553.325

        def do_test(msec, bucket_duration, expected_t, expected_bucket):
            bucket = msec2bucket(msec, bucket_duration)
            t = bucket2time(bucket, bucket_duration)
            self.assertEqual(t, expected_t)
            self.assertEqual(bucket, expected_bucket)

        do_test(msec, 3600, '2019-09-20T07:00:00Z', 435823)
        do_test(msec, 7200, '2019-09-20T08:00:00Z', 217912)


if __name__ == '__main__':
    unittest.main()
