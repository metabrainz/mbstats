"""Integration test for InfluxDB backend against a real InfluxDB 1.x instance.

Requires: docker run -d --name influxdb-test -p 8086:8086 influxdb:1.8
"""

import time
import urllib.request

import pytest

INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
TEST_DB = "mbstats_integration_test"


def influxdb_available():
    try:
        urllib.request.urlopen(
            f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/ping", timeout=2
        )
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not influxdb_available(), reason="InfluxDB not available on localhost:8086"
)


@pytest.fixture
def influx_options():
    """Fake options object mimicking argparse namespace."""

    class Opts:
        influx_host = INFLUXDB_HOST
        influx_port = INFLUXDB_PORT
        influx_username = "root"
        influx_password = "root"
        influx_database = TEST_DB
        influx_timeout = 10
        influx_batch_size = 100
        influx_drop_database = True
        dry_run = False
        quiet = 0

    return Opts()


def query_influx(q, db=None):
    """Helper to query InfluxDB directly via HTTP."""
    import urllib.parse

    params = {"q": q}
    if db:
        params["db"] = db
    url = (
        f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/query?{urllib.parse.urlencode(params)}"
    )
    import json

    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


class TestInfluxBackendIntegration:
    """Test the InfluxBackend with a real InfluxDB instance."""

    def test_full_write_cycle(self, influx_options):
        """Test create db, write points, verify data, drop db."""
        from mbstats.backends.influxdb import InfluxBackend

        # Initialize backend (creates/drops database)
        backend = InfluxBackend(influx_options)

        # Verify database was created
        result = query_influx("SHOW DATABASES")
        databases = [row[0] for row in result["results"][0]["series"][0]["values"]]
        assert TEST_DB in databases

        # Create test points (same format as mbstats produces)
        points = [
            {
                "measurement": "hits",
                "tags": {"vhost": "example.com", "protocol": "https", "loctag": "-"},
                "time": "2024-01-01T00:00:00Z",
                "fields": {"value": 42},
            },
            {
                "measurement": "bytes_sent",
                "tags": {"vhost": "example.com", "protocol": "https", "loctag": "-"},
                "time": "2024-01-01T00:00:00Z",
                "fields": {"value": 123456},
            },
            {
                "measurement": "status",
                "tags": {
                    "vhost": "example.com",
                    "protocol": "https",
                    "loctag": "-",
                    "status": "200",
                },
                "time": "2024-01-01T00:00:00Z",
                "fields": {"value": 100},
            },
        ]

        # Write points with tags (same as production usage)
        tags = {"host": "testhost", "name": "testname"}
        result = backend.send_points(tags=tags, points=points)
        assert result is True

        # Give InfluxDB a moment to index
        time.sleep(0.5)

        # Verify data was written
        result = query_influx("SELECT * FROM hits", db=TEST_DB)
        series = result["results"][0]["series"][0]
        assert series["name"] == "hits"
        # Check tags are present
        assert "host" in series["columns"]
        assert "vhost" in series["columns"]
        # Check value
        value_idx = series["columns"].index("value")
        assert series["values"][0][value_idx] == 42

    def test_batch_write(self, influx_options):
        """Test writing with batch_size smaller than total points."""
        from mbstats.backends.influxdb import InfluxBackend

        influx_options.influx_batch_size = 5
        backend = InfluxBackend(influx_options)

        # Create 12 points to test batching (batch_size=5 -> 3 batches)
        points = [
            {
                "measurement": "test_batch",
                "tags": {"idx": str(i)},
                "time": f"2024-01-01T00:{i:02d}:00Z",
                "fields": {"value": i},
            }
            for i in range(12)
        ]

        tags = {"host": "testhost"}
        result = backend.send_points(tags=tags, points=points)
        assert result is True

        time.sleep(0.5)

        result = query_influx("SELECT count(value) FROM test_batch", db=TEST_DB)
        count = result["results"][0]["series"][0]["values"][0][1]
        assert count == 12

    def test_point_dict(self, influx_options):
        """Test point_dict helper produces correct format."""
        from mbstats.backends.influxdb import InfluxBackend

        backend = InfluxBackend(influx_options)
        point = backend.point_dict(
            "mbstats",
            {"duration_seconds": 1.5, "parsed_lines": 1000},
            tags={"host": "myhost"},
            time_rfc3339="2024-06-01T12:00:00Z",
        )
        assert point["measurement"] == "mbstats"
        assert point["fields"]["parsed_lines"] == 1000
        assert point["tags"]["host"] == "myhost"
        assert point["time"] == "2024-06-01T12:00:00Z"

        # Write it and verify
        result = backend.send_points(points=[point])
        assert result is True

    def test_special_characters_in_tags(self, influx_options):
        """Test that special characters in tag values are handled."""
        from mbstats.backends.influxdb import InfluxBackend

        backend = InfluxBackend(influx_options)

        points = [
            {
                "measurement": "test_special",
                "tags": {"vhost": "my host.com", "path": "a=b,c d"},
                "time": "2024-01-01T00:00:00Z",
                "fields": {"value": 1},
            },
        ]

        result = backend.send_points(points=points)
        assert result is True

        time.sleep(0.5)

        result = query_influx("SELECT * FROM test_special", db=TEST_DB)
        series = result["results"][0]["series"][0]
        vhost_idx = series["columns"].index("vhost")
        assert series["values"][0][vhost_idx] == "my host.com"


class TestVendoredClientComparison:
    """Compare vendored client output against the original influxdb library."""

    def test_identical_writes(self, influx_options):
        """Write same points with both clients, compare database contents."""
        from influxdb import InfluxDBClient as OriginalClient
        from mbstats.influxdb1x import InfluxDBClient as VendoredClient

        db_original = "mbstats_cmp_original"
        db_vendored = "mbstats_cmp_vendored"

        # Setup both clients
        orig = OriginalClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username="root",
            password="root",
            database=db_original,
        )
        orig.drop_database(db_original)
        orig.create_database(db_original)

        vend = VendoredClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username="root",
            password="root",
            database=db_vendored,
        )
        vend.drop_database(db_vendored)
        vend.create_database(db_vendored)

        # Test points matching real mbstats output
        points = [
            {
                "measurement": "hits",
                "tags": {
                    "vhost": "musicbrainz.org",
                    "protocol": "https",
                    "loctag": "ws",
                },
                "time": "2024-03-15T10:00:00Z",
                "fields": {"value": 1523},
            },
            {
                "measurement": "bytes_sent",
                "tags": {
                    "vhost": "musicbrainz.org",
                    "protocol": "https",
                    "loctag": "ws",
                },
                "time": "2024-03-15T10:00:00Z",
                "fields": {"value": 4567890},
            },
            {
                "measurement": "request_time_mean",
                "tags": {
                    "vhost": "musicbrainz.org",
                    "protocol": "https",
                    "loctag": "ws",
                },
                "time": "2024-03-15T10:00:00Z",
                "fields": {"value": 0.0342},
            },
            {
                "measurement": "upstreams_status",
                "tags": {
                    "vhost": "musicbrainz.org",
                    "protocol": "https",
                    "loctag": "ws",
                    "upstream": "10.0.0.1:5000",
                    "status": "200",
                },
                "time": "2024-03-15T10:00:00Z",
                "fields": {"value": 1400},
            },
            {
                "measurement": "status",
                "tags": {
                    "vhost": "musicbrainz.org",
                    "protocol": "https",
                    "loctag": "ws",
                    "status": "304",
                },
                "time": "2024-03-15T10:00:00Z",
                "fields": {"value": 89},
            },
        ]
        shared_tags = {
            "host": "prod-web-01",
            "name": "/var/log/nginx/stats.log",
            "dc": "hetzner",
        }

        # Write with both
        orig.write_points(points, tags=shared_tags, time_precision="m")
        vend.write_points(points, tags=shared_tags, time_precision="m")

        time.sleep(0.5)

        # Query both and compare
        measurements = [
            "hits",
            "bytes_sent",
            "request_time_mean",
            "upstreams_status",
            "status",
        ]
        for m in measurements:
            r_orig = query_influx(f"SELECT * FROM {m}", db=db_original)
            r_vend = query_influx(f"SELECT * FROM {m}", db=db_vendored)

            orig_series = r_orig["results"][0]["series"][0]
            vend_series = r_vend["results"][0]["series"][0]

            # Same columns (order may differ, so compare as sets)
            assert set(orig_series["columns"]) == set(vend_series["columns"]), (
                f"Column mismatch for {m}: {orig_series['columns']} vs {vend_series['columns']}"
            )

            # Same data - normalize by sorting columns
            orig_cols = orig_series["columns"]
            vend_cols = vend_series["columns"]

            for orig_row in orig_series["values"]:
                orig_dict = dict(zip(orig_cols, orig_row))
                vend_row = vend_series["values"][0]
                vend_dict = dict(zip(vend_cols, vend_row))
                assert orig_dict == vend_dict, (
                    f"Data mismatch for {m}: {orig_dict} vs {vend_dict}"
                )

        # Cleanup
        orig.drop_database(db_original)
        vend.drop_database(db_vendored)

    def test_batch_comparison(self, influx_options):
        """Verify batched writes produce identical results."""
        from influxdb import InfluxDBClient as OriginalClient
        from mbstats.influxdb1x import InfluxDBClient as VendoredClient

        db_original = "mbstats_batch_orig"
        db_vendored = "mbstats_batch_vend"

        orig = OriginalClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username="root",
            password="root",
            database=db_original,
        )
        orig.drop_database(db_original)
        orig.create_database(db_original)

        vend = VendoredClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username="root",
            password="root",
            database=db_vendored,
        )
        vend.drop_database(db_vendored)
        vend.create_database(db_vendored)

        points = [
            {
                "measurement": "test_batch",
                "tags": {"idx": str(i)},
                "time": f"2024-01-01T{i:02d}:00:00Z",
                "fields": {"value": i * 100},
            }
            for i in range(20)
        ]

        orig.write_points(points, time_precision="m", batch_size=7)
        vend.write_points(points, time_precision="m", batch_size=7)

        time.sleep(0.5)

        r_orig = query_influx("SELECT count(value) FROM test_batch", db=db_original)
        r_vend = query_influx("SELECT count(value) FROM test_batch", db=db_vendored)

        orig_count = r_orig["results"][0]["series"][0]["values"][0][1]
        vend_count = r_vend["results"][0]["series"][0]["values"][0][1]
        assert orig_count == vend_count == 20

        orig.drop_database(db_original)
        vend.drop_database(db_vendored)
