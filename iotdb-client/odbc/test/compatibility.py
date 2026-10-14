# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Public-ABI regressions for catalog browsing, Unicode, and REST result types."""

import ctypes as c
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


def wide(value):
    return (c.c_ushort * (len(value.encode("utf-16-le")) // 2 + 1)).from_buffer_copy(
        value.encode("utf-16-le") + b"\0\0"
    )


def main():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            sql = payload["sql"]
            requests.append(sql)
            names, types, rows = ["Version"], ["TEXT"], [["2.0.11-SNAPSHOT"]]
            if "information_schema.tables" in sql:
                names, types = ["database", "table_name", "comment"], ["STRING"] * 3
                rows = [
                    ["db_one", "samples", None],
                    ["db_one", "samplex", None],
                    ["db_two", "中文", None],
                ]
            elif "information_schema.columns" in sql:
                names = [
                    "database",
                    "table_name",
                    "column_name",
                    "datatype",
                    "category",
                    "comment",
                ]
                types = ["STRING"] * 6
                rows = [
                    ["db_one", "samples", "time", "TIMESTAMP", "TIME", None],
                    ["db_one", "samples", "flag", "BOOLEAN", "FIELD", None],
                    ["db_one", "samples", "day", "DATE", "FIELD", None],
                ]
            elif sql == "SHOW DATABASES":
                names, types, rows = ["Database"], ["STRING"], [["db_one"], ["db_two"]]
            elif sql.startswith("SELECT time"):
                names, types = ["中文", "day", "bytes", "empty"], [
                    "STRING",
                    "DATE",
                    "BLOB",
                    "STRING",
                ]
                rows = [["中文😀", "2024-02-29", "0x0001ff", ""]]
            body = {"column_names": names, "data_types": types, "values": rows}
            if "/rest/v2/" in self.path:
                body["expressions"] = []
                body["timestamps"] = []
                body["values"] = [list(column) for column in zip(*rows)]
            response = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    lib = (c.WinDLL if os.name == "nt" else c.CDLL)(os.path.abspath(sys.argv[1]))
    h, short, ushort, integer, length = (
        c.c_void_p,
        c.c_short,
        c.c_ushort,
        c.c_int,
        c.c_ssize_t,
    )

    def api(name, *arguments):
        fn = getattr(lib, name)
        fn.argtypes, fn.restype = arguments, short
        return fn

    alloc = api("SQLAllocHandle", short, h, c.POINTER(h))
    free = api("SQLFreeHandle", short, h)
    connect = api("SQLDriverConnectW", h, h, h, short, h, short, h, ushort)
    direct = api("SQLExecDirectW", h, h, integer)
    tables = api("SQLTablesW", h, h, short, h, short, h, short, h, short)
    columns = api("SQLColumnsW", h, h, short, h, short, h, short, h, short)
    fetch = api("SQLFetch", h)
    get = api("SQLGetData", h, ushort, short, h, length, c.POINTER(length))
    count = api("SQLNumResultCols", h, c.POINTER(short))
    describe = api(
        "SQLDescribeColW",
        h,
        ushort,
        h,
        short,
        c.POINTER(short),
        c.POINTER(short),
        h,
        h,
        h,
    )
    close = api("SQLCloseCursor", h)
    env, dbc, stmt = h(), h(), h()

    def read_rows(expected_columns):
        n = short()
        assert count(stmt, c.byref(n)) == 0 and n.value == expected_columns
        result = []
        while True:
            rc = fetch(stmt)
            if rc == 100:
                break
            assert rc == 0, rc
            row = []
            for col in range(1, n.value + 1):
                value, size = c.create_string_buffer(4096), length()
                assert get(stmt, col, 1, value, len(value), c.byref(size)) == 0
                row.append(None if size.value == -1 else value.value.decode())
            result.append(row)
        assert close(stmt) == 0
        return result

    try:
        assert alloc(1, None, c.byref(env)) == 0
        assert alloc(2, env, c.byref(dbc)) == 0
        connection = wide(
            f"SERVER=127.0.0.1;PORT={server.server_port};RESTFUL=1;DATABASE=db_one"
        )
        assert connect(dbc, None, connection, -3, None, 0, None, 0) == 0
        assert alloc(3, dbc, c.byref(stmt)) == 0
        # SQL_NTS catalog enumeration, standard null padding, and fixed schemas.
        assert tables(stmt, wide("%"), -3, wide(""), -3, wide(""), -3, None, 0) == 0
        rows = read_rows(5)
        assert [row[0] for row in rows] == ["db_one", "db_two"]
        assert all(row[1:] == [None] * 4 for row in rows)
        assert tables(stmt, wide(""), -3, wide("%"), -3, wide(""), -3, None, 0) == 0
        assert read_rows(5) == []
        assert (
            tables(stmt, wide(""), -3, wide(""), -3, wide(""), -3, wide("%"), -3) == 0
        )
        assert read_rows(5) == [[None, None, None, "TABLE", None]]
        assert tables(stmt, None, 0, None, 0, None, 0, wide("'VIEW'"), -3) == 0
        assert read_rows(5) == []
        assert (
            tables(stmt, wide(r"db\_one"), -3, None, 0, wide("sample_"), -3, None, 0)
            == 0
        )
        assert len(read_rows(5)) == 2
        assert tables(stmt, None, 0, None, 0, wide("中_"), -3, None, 0) == 0
        assert read_rows(5)[0][2] == "中文"
        before = len(requests)
        assert tables(stmt, wide("x' OR 1=1 --"), -3, None, 0, None, 0, None, 0) == 0
        assert read_rows(5) == []
        assert all("OR 1=1" not in sql for sql in requests[before:])
        assert columns(stmt, None, 0, None, 0, None, 0, None, 0) == 0
        rows = read_rows(18)
        assert len(rows) == 3 and [row[16] for row in rows] == ["1", "2", "3"]
        assert rows[1][4] == "-7" and rows[2][4] == "91"
        assert rows[0][10] == "0" and rows[0][17] == "NO"
        assert columns(stmt, None, 0, None, 0, None, 0, wide("day"), -3) == 0
        assert read_rows(18)[0][16] == "3"
        assert columns(stmt, None, 0, None, 0, wide("missing"), -3, None, 0) == 0
        assert read_rows(18) == []
        sql = "SELECT time, '中文😀' FROM samples"
        assert direct(stmt, wide(sql), -3) == 0 and requests[-1] == sql
        name, name_length, data_type = (ushort * 3)(), short(), short()
        assert (
            describe(
                stmt,
                1,
                name,
                3,
                c.byref(name_length),
                c.byref(data_type),
                None,
                None,
                None,
            )
            == 0
        )
        assert name_length.value == 2 and list(name) == [0x4E2D, 0x6587, 0]
        assert data_type.value == -9  # SQL_WVARCHAR makes ADO request UTF-16 data.
        assert fetch(stmt) == 0

        class Date(c.Structure):
            _fields_ = [("year", short), ("month", ushort), ("day", ushort)]

        day, size = Date(), length()
        assert get(stmt, 2, 91, c.byref(day), c.sizeof(day), c.byref(size)) == 0
        assert (day.year, day.month, day.day) == (2024, 2, 29)
        blob = (c.c_ubyte * 3)()
        assert get(stmt, 3, -2, blob, 3, c.byref(size)) == 0 and list(blob) == [
            0,
            1,
            255,
        ]
        empty = c.create_string_buffer(1)
        assert get(stmt, 4, 1, empty, 1, c.byref(size)) == 0 and size.value == 0
        assert get(stmt, 4, 1, empty, 1, c.byref(size)) == 100
        assert fetch(stmt) == 100
        print("ODBC catalog, Unicode and REST compatibility checks passed")
    finally:
        for kind, value in ((3, stmt), (2, dbc), (1, env)):
            if value:
                free(kind, value)
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
