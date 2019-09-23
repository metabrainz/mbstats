import contextlib
import gzip
import io
import os.path
import sys
import tempfile
import unittest

from mbstats.app import main


class TestParser(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.TemporaryDirectory()
        self.logfile = os.path.join(self.test_dir.name, 'nginx.log')
        this_dir =  os.path.dirname(os.path.abspath(__file__))
        self.logfile_gz = os.path.join(this_dir, 'data', 'stats.log.gz')

        max_count = 70000
        count = 0
        with open(self.logfile, 'w') as out:
            with io.TextIOWrapper(io.BufferedReader(gzip.open(self.logfile_gz))) as f:
                for line in f:
                    out.write(line)
                    count += 1
                    if count >= max_count:
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

    def call_main(self, args):
        sys.argv = args

        output = None
        with io.StringIO() as buf:
            with contextlib.redirect_stdout(buf):
                with self.assertRaises(SystemExit):
                    main()
                output = buf.getvalue()
        #print(output)
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
        ]

        output += self.call_main(common_args + ['-m', '1000', '--startover'])

        output += self.call_main(common_args + [
            '-m',
            '1000',
            '--bucket-duration',
            '30',
        ])

        output += self.call_main(common_args + [
            '-m',
            str(int(self.log_numlines / 2)),
        ])
        output += self.call_main(common_args + [
            '-m',
            str(int(self.log_numlines / 2)),
        ])
        #print(output)
        self.assertIn('--dry-run', output)


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
