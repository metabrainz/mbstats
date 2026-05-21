"""Minimal InfluxDB 1.x client.

Vendored from influxdb-python (MIT License) with simplifications:
- No UDP, gzip, proxy, msgpack, or SSL support
- No pandas/dataframe support
- Uses urllib3 directly instead of requests
- Retries with exponential backoff on connection errors
"""

from datetime import datetime, timezone
from itertools import chain, islice
import json
from numbers import Integral
import random
import time

import urllib3

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class InfluxDBClientError(Exception):
    def __init__(self, content, code=None):
        if isinstance(content, bytes):
            content = content.decode("UTF-8", "replace")
        message = f"{code}: {content}" if code is not None else content
        super().__init__(message)
        self.content = content
        self.code = code


class InfluxDBServerError(Exception):
    pass


# --- Line Protocol ---


def _escape_tag(tag):
    s = str(tag)
    return (
        s.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace("\n", "\\n")
    )


def _escape_tag_value(value):
    ret = _escape_tag(value)
    if ret.endswith("\\"):
        ret += " "
    return ret


def _escape_field_value(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, Integral):
        return f"{value}i"
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    return str(value)


def _convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp

    if isinstance(timestamp, str):
        ts = timestamp
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
    elif isinstance(timestamp, datetime):
        dt = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
    else:
        raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")

    ns = int((dt - EPOCH).total_seconds() * 1_000_000_000)

    if precision is None or precision == "n":
        return ns
    if precision == "u":
        return ns // 1_000
    if precision == "ms":
        return ns // 1_000_000
    if precision == "s":
        return ns // 1_000_000_000
    if precision == "m":
        return ns // 60_000_000_000
    if precision == "h":
        return ns // 3_600_000_000_000
    raise ValueError(f"Invalid precision: {precision}")


def make_line(measurement, tags=None, fields=None, time=None, precision=None):
    tags = tags or {}
    fields = fields or {}

    line = _escape_tag(measurement)

    tag_parts = []
    for key in sorted(tags.keys()):
        k = _escape_tag(key)
        v = _escape_tag_value(tags[key])
        if k and v:
            tag_parts.append(f"{k}={v}")
    if tag_parts:
        line += "," + ",".join(tag_parts)

    field_parts = []
    for key in sorted(fields.keys()):
        k = _escape_tag(key)
        v = _escape_field_value(fields[key])
        if k and v:
            field_parts.append(f"{k}={v}")
    if field_parts:
        line += " " + ",".join(field_parts)

    if time is not None:
        line += f" {int(_convert_timestamp(time, precision))}"

    return line


def make_lines(data, precision=None):
    lines = []
    static_tags = data.get("tags")
    for point in data["points"]:
        if static_tags:
            tags = dict(static_tags)
            tags.update(point.get("tags") or {})
        else:
            tags = point.get("tags") or {}
        line = make_line(
            point.get("measurement", data.get("measurement")),
            tags=tags,
            fields=point.get("fields"),
            precision=precision,
            time=point.get("time"),
        )
        lines.append(line)
    return "\n".join(lines) + "\n"


# --- HTTP Client ---


def _quote_ident(identifier):
    escaped = identifier.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


class InfluxDBClient:
    def __init__(
        self,
        host="localhost",
        port=8086,
        username="root",
        password="root",
        database=None,
        timeout=None,
        retries=3,
    ):
        self._database = database
        self._retries = retries
        self._baseurl = f"http://{host}:{int(port)}"
        headers = urllib3.make_headers(basic_auth=f"{username}:{password}")
        self._http = urllib3.PoolManager(
            num_pools=1,
            maxsize=10,
            timeout=urllib3.Timeout(total=timeout),
            headers=headers,
        )

    def close(self):
        self._http.clear()

    def _request(
        self, method, path, fields=None, body=None, headers=None, expected_code=200
    ):
        url = f"{self._baseurl}/{path}"

        retry = True
        _try = 0
        while retry:
            try:
                if body is not None:
                    response = self._http.request(
                        method, url, body=body, headers=headers, fields=fields
                    )
                else:
                    response = self._http.request(
                        method, url, fields=fields, headers=headers
                    )
                break
            except (urllib3.exceptions.HTTPError, TimeoutError, OSError):
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
                if not retry:
                    raise
                time.sleep((2**_try) * random.random() / 100.0)

        if 500 <= response.status < 600:
            raise InfluxDBServerError(response.data.decode("utf-8", "replace"))
        if response.status != expected_code:
            raise InfluxDBClientError(
                response.data.decode("utf-8", "replace"), response.status
            )
        return response

    def query(self, query, method="GET", database=None):
        params = {"q": query}
        if database or self._database:
            params["db"] = database or self._database
        resp = self._request(method, "query", fields=params)
        return json.loads(resp.data.decode("utf-8"))

    def write_points(
        self, points, time_precision=None, database=None, tags=None, batch_size=None
    ):
        if batch_size and batch_size > 0:
            for batch in self._batches(points, batch_size):
                self._write_points(list(batch), time_precision, database, tags)
        else:
            self._write_points(points, time_precision, database, tags)
        return True

    def _write_points(self, points, time_precision, database, tags):
        data = {"points": points}
        if tags:
            data["tags"] = tags

        body = make_lines(data, precision=time_precision).encode("utf-8")

        fields = {"db": database or self._database}
        if time_precision:
            fields["precision"] = time_precision

        # urllib3 doesn't mix body and fields in the URL, so build URL manually
        from urllib.parse import urlencode

        path = "write?" + urlencode(fields)
        self._request(
            "POST",
            path,
            body=body,
            headers={"Content-Type": "application/octet-stream"},
            expected_code=204,
        )

    @staticmethod
    def _batches(iterable, size):
        iterator = iter(iterable)
        while True:
            try:
                head = (next(iterator),)
            except StopIteration:
                return
            yield chain(head, islice(iterator, size - 1))

    def create_database(self, dbname):
        self.query(f"CREATE DATABASE {_quote_ident(dbname)}", method="POST")

    def drop_database(self, dbname):
        self.query(f"DROP DATABASE {_quote_ident(dbname)}", method="POST")
