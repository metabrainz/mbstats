import contextlib
import gzip
import io
import os.path
import sys
import tempfile
import unittest

from mbstats.app import (
    main,
    parse_upstreams,
)


LINES_TO_PARSE = 10


class TestParser(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.logfile = os.path.join(self.test_dir.name, 'nginx.log')
        this_dir =  os.path.dirname(os.path.abspath(__file__))
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

    def tearDown(self):
        # Close the file, the directory will be removed after the test
        self.test_dir.cleanup()
        #pass

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
                self.assertEqual(se.exception.code, expected_code)
                output = buf.getvalue()
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
            'test'
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
        #Lines weren't parsed, so keep remain's value

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

#test change of bucket duration, must fail (hence the !)
#! $CMD -m 1000 --bucket-duration 30 && echo "Testing exit on change of bucket duration, SUCCESS"
#test change of lookback factor, must fail (hence the !)
#! $CMD -m 1000 --lookback-factor 3 && echo "Testing exit on change of lookback factor, SUCCESS"

#$CMD -m 300000;

#for i in $(seq 1 5); do
#	$CMD -m 70000 --simulate-send-failure;
#done

#for i in $(seq 1 5); do
#	$CMD -m 70000;
#done

#$CMD -m 2000000;

# simulate a log rotation
#mv $STATSLOG $STATSLOG.1
#head -5500000 $STATSLOG_SOURCE | tail -500000 > $STATSLOG
#$CMD -m 200000;

if __name__ == '__main__':
    unittest.main()
