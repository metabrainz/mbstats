import contextlib
import gzip
import io
import os.path
import sys
import tempfile
import unittest

from mbstats.app import (
    ParseSkip,
    PosField,
    main,
    parse_upstreams,
    parseline,
)


LINES_TO_PARSE = 10


class TestParser(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.logfile = os.path.join(self.test_dir.name, 'nginx.log')
        this_dir = os.path.dirname(os.path.abspath(__file__))
        self.logfile_gz = os.path.join(this_dir, 'data', 'test1.log.gz')

        count = 0
        with open(self.logfile, 'w') as out:
            with io.TextIOWrapper(io.BufferedReader(gzip.open(self.logfile_gz))) as f:
                for line in f:
                    out.write(line)
                    count += 1
                    if count >= LINES_TO_PARSE:
                        break

        self.log_numlines = count
        #print("Log has %d lines" % self.log_numlines)
        self.sample_line = '1|1568962563.374|musicbrainz.org|s|ws|200|2799|2.5|289|0.026|10.2.2.31:65412|200|0.024|0.000|0.024'

    def get_sample_line(self, n, replace_with='xxx', line=None):
        if line is None:
            line = self.sample_line
        parts = line.split('|')
        parts[n] = replace_with
        return '|'.join(parts)

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()
        # pass

    def test_A(self):
        output = self.call_main(['testing', '--dump-config', '--dry-run',
                                 '--log-handler=stdout'])
        self.assertIn('"dry_run": true', output)

    def call_main(self, args, expected_code=0):
        sys.argv = args

        output = None
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                with self.assertRaises(SystemExit) as se:
                    main()
                output = buf.getvalue()
                self.assertEqual(se.exception.code, expected_code, output)
        return output

    def test_B(self):
        output = ''
        common_args = [
            'testing',
            '-f', self.logfile,
            '-w', self.test_dir.name,
            '--do-not-skip-to-end',
            '-l',
            '.',
            '-n',
            'test',
            '--dry-run',
            '--locker=portalocker',
            '--debug',
            '--log-handler=stdout',
            '--bucket-duration',
            '1',
        ]

        remain = self.log_numlines
        num = 1
        output = self.call_main(common_args + ['-m', str(num), '--startover'])
        self.assertIn(' parsed=%d ' % num, output)
        remain -= num

        num = 1
        output = self.call_main(common_args + [
            '-m',
            str(num),
            '--bucket-duration',
            '30',
        ], expected_code=1)
        self.assertIn(' Error: Bucket duration mismatch 1 vs 30', output)
        # Lines weren't parsed, so keep remain's value

        num = int(self.log_numlines / 2)
        output = self.call_main(common_args + [
            '-m',
            str(num),
        ])
        self.assertIn('Sending 68 points', output)
        remain -= num

        num = remain
        output = self.call_main(common_args + [
            '-m',
            str(num),
        ])
        self.assertIn('Sending 68 points', output)
        remain -= num

        # All lines should have been parsed
        self.assertEqual(remain, 0)

    def test_parse_upstreams(self):

        upstreams = {
            'upstream_addr': '10.2.2.31:65412, 10.2.2.32:65412',
            'upstream_status': '200, 200',
            'upstream_response_time': '0.024, 0.024',
            'upstream_connect_time': '0.000, 0.000',
            'upstream_header_time': '0.024, 0.024'
        }

        result = parse_upstreams(upstreams)
        self.assertEqual(result['servers_contacted'], 2)
        self.assertEqual(result['internal_redirects'], 0)
        servers = ['10.2.2.31:65412', '10.2.2.32:65412']
        self.assertEqual(result['servers'], servers)
        for server in servers:
            self.assertEqual(result['status'][server]['200'], 1)
            self.assertEqual(result['response_time'][server], 0.024)
            self.assertEqual(result['connect_time'][server], 0.0)
            self.assertEqual(result['header_time'][server], 0.024)

        with self.assertRaises(ValueError):
            upstreams = {
                'upstream_addr': '10.2.2.31:65412',
                'upstream_status': '200',
                'upstream_response_time': 'x0.024',
                'upstream_connect_time': '0.000',
                'upstream_header_time': '0.024'
            }
            result = parse_upstreams(upstreams)

        with self.assertRaises(ValueError):
            upstreams = {
                'upstream_addr': '10.2.2.31:65412',
                'upstream_status': '200',
                'upstream_response_time': '0.024',
                'upstream_connect_time': 'x0.000',
                'upstream_header_time': '0.024'
            }
            result = parse_upstreams(upstreams)

        with self.assertRaises(ValueError):
            upstreams = {
                'upstream_addr': '10.2.2.31:65412',
                'upstream_status': '200',
                'upstream_response_time': '0.024',
                'upstream_connect_time': '0.000',
                'upstream_header_time': 'x0.024'
            }
            result = parse_upstreams(upstreams)

        with self.assertRaises(ValueError):
            upstreams = {
                'upstream_addr': '10.2.2.31:65412',
                'upstream_status': '20x',
                'upstream_response_time': '0.024',
                'upstream_connect_time': '0.000',
                'upstream_header_time': '0.024'
            }
            result = parse_upstreams(upstreams)

    def test_parseline(self):
        line = '1|1568962563.374|musicbrainz.org|s|ws|200|2799|2.5|289|0.026|10.2.2.31:65412|200|0.024|0.000|0.024'
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

        expected = {
            'vhost': 'musicbrainz.org',
            'protocol': 's',
            'loctag': 'ws',
            'status': 200,
            'bytes_sent': 2799,
            'request_length': 289,
            'gzip_ratio': 2.5,
            'request_time': 0.026,
            'upstreams': {
                'servers_contacted': 1,
                'internal_redirects': 0,
                'status': {
                    '10.2.2.31:65412': {
                        '200': 1
                    }
                },
                'response_time': {
                    '10.2.2.31:65412': 0.024
                },
                'connect_time': {
                    '10.2.2.31:65412': 0.0
                },
                'header_time': {
                    '10.2.2.31:65412': 0.024
                },
                'servers': ['10.2.2.31:65412']
            }
        }
        self.assertEqual(row, expected)
        self.assertEqual(last_msec, 1568962563.374)
        self.assertEqual(bucket, 1568962564)

    def test_parseline_version_invalid(self):
        line = self.get_sample_line(PosField.version)
        with self.assertRaisesRegex(ParseSkip, "^invalid log version: xxx$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_msec_valid(self):
        row, last_msec, bucket = parseline(self.sample_line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(last_msec, 1568962563.374)

    def test_parseline_msec_invalid(self):
        line = self.get_sample_line(PosField.msec)
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_vhost(self):
        line = self.get_sample_line(PosField.vhost, replace_with='xxx')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['vhost'], 'xxx')

    def test_parseline_protocol(self):
        line = self.get_sample_line(PosField.protocol, replace_with='xxx')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['protocol'], 'xxx')

    def test_parseline_loctag(self):
        line = self.get_sample_line(PosField.loctag, replace_with='xxx')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['loctag'], 'xxx')

    def test_parseline_status_invalid(self):
        line = self.get_sample_line(PosField.status, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^invalid literal for int\(\) with base 10: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_status_valid(self):
        line = self.get_sample_line(PosField.status, replace_with='444')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['status'], 444)

    def test_parseline_bytes_sent_invalid(self):
        line = self.get_sample_line(PosField.bytes_sent, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^invalid literal for int\(\) with base 10: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_bytes_sent_valid(self):
        line = self.get_sample_line(PosField.bytes_sent, replace_with='12345')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['bytes_sent'], 12345)

    def test_parseline_gzip_ratio_invalid(self):
        line = self.get_sample_line(PosField.gzip_ratio, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_gzip_ratio_valid(self):
        line = self.get_sample_line(PosField.gzip_ratio, replace_with='12.34')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['gzip_ratio'], 12.34)

    def test_parseline_request_length_invalid(self):
        line = self.get_sample_line(PosField.request_length, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^invalid literal for int\(\) with base 10: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_request_length_valid(self):
        line = self.get_sample_line(PosField.request_length, replace_with='12345')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['request_length'], 12345)

    def test_parseline_request_time_invalid(self):
        line = self.get_sample_line(PosField.request_time, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_request_time_valid(self):
        line = self.get_sample_line(PosField.request_time, replace_with='12.34')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['request_time'], 12.34)

    def test_parseline_upstream_status_invalid(self):
        line = self.get_sample_line(PosField.upstream_status, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^invalid literal for int\(\) with base 10: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_upstream_status_valid(self):
        line = self.get_sample_line(PosField.upstream_status, replace_with='444')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['upstreams']['status']['10.2.2.31:65412'], {'444': 1})

    def test_parseline_upstream_addr_invalid(self):
        line = self.get_sample_line(PosField.upstream_addr, replace_with='-')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertNotIn('upstreams', row)

    def test_parseline_upstream_addr_valid_simple(self):
        line = self.get_sample_line(PosField.upstream_addr, replace_with='6.6.6.6')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertIn('upstreams', row)
        self.assertIn('6.6.6.6', row['upstreams']['status'])
        self.assertIn('6.6.6.6', row['upstreams']['response_time'])
        self.assertIn('6.6.6.6', row['upstreams']['connect_time'])
        self.assertIn('6.6.6.6', row['upstreams']['header_time'])
        self.assertEqual(row['upstreams']['servers_contacted'], 1)
        self.assertEqual(row['upstreams']['internal_redirects'], 0)
        self.assertIn('6.6.6.6', row['upstreams']['servers'])

    def test_parseline_upstream_addr_valid_one_redirect(self):
        line = self.get_sample_line(PosField.upstream_addr, replace_with='6.6.6.6 : 7.7.7.7')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertIn('upstreams', row)
        self.assertIn('6.6.6.6', row['upstreams']['status'])
        self.assertIn('6.6.6.6', row['upstreams']['response_time'])
        self.assertIn('6.6.6.6', row['upstreams']['connect_time'])
        self.assertIn('6.6.6.6', row['upstreams']['header_time'])
        self.assertEqual(row['upstreams']['servers_contacted'], 1)
        self.assertEqual(row['upstreams']['internal_redirects'], 1)
        self.assertIn('6.6.6.6', row['upstreams']['servers'])

    def test_parseline_upstream_addr_valid_multiple(self):
        line = "1|1611817838.597|musicbrainz.org|-|ws|200|6110|-|516|0.726|10.2.2.36:65412 : 195.201.47.148:80|200 : 200|0.012 : 0.712|0.000 : 0.000|0.012 : 0.712"
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertIn('upstreams', row)
        for addr in ('10.2.2.36:65412', '195.201.47.148:80'):
            self.assertIn(addr, row['upstreams']['response_time'])
            self.assertIn(addr, row['upstreams']['connect_time'])
            self.assertIn(addr, row['upstreams']['header_time'])
            self.assertIn(addr, row['upstreams']['servers'])
            self.assertIn(addr, row['upstreams']['status'])
            self.assertEqual(row['upstreams']['status'][addr], {'200': 1})
        self.assertEqual(row['upstreams']['servers_contacted'], 1)
        self.assertEqual(row['upstreams']['internal_redirects'], 1)

    def test_parseline_upstream_response_time_invalid(self):
        line = self.get_sample_line(PosField.upstream_response_time, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_upstream_response_time_valid(self):
        line = self.get_sample_line(PosField.upstream_response_time, replace_with='12.34')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['upstreams']['response_time']['10.2.2.31:65412'], 12.34)

    def test_parseline_upstream_connect_time_invalid(self):
        line = self.get_sample_line(PosField.upstream_connect_time, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_upstream_connect_time_valid(self):
        line = self.get_sample_line(PosField.upstream_connect_time, replace_with='12.34')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['upstreams']['connect_time']['10.2.2.31:65412'], 12.34)

    def test_parseline_upstream_header_time_invalid(self):
        line = self.get_sample_line(PosField.upstream_header_time, replace_with='xxx')
        with self.assertRaisesRegex(ParseSkip, "^could not convert string to float: 'xxx'$"):
            row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)

    def test_parseline_upstream_header_time_valid(self):
        line = self.get_sample_line(PosField.upstream_header_time, replace_with='12.34')
        row, last_msec, bucket = parseline(line, ignore_before=0, bucket_duration=1, last_msec=0)
        self.assertEqual(row['upstreams']['header_time']['10.2.2.31:65412'], 12.34)

if __name__ == '__main__':
    unittest.main()
