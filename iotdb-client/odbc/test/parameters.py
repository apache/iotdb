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
"""Check parameter values at the REST boundary using the public ODBC ABI."""

import ctypes as c
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import sys
import threading


class Timestamp(c.Structure):
    _fields_ = [
        (name, c.c_short)
        for name in ("year", "month", "day", "hour", "minute", "second")
    ] + [("fraction", c.c_uint)]


def main():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.path, payload["sql"]))
            response = b'{"code":200,"expressions":null,"column_names":[],"values":[]}'
            if payload["sql"].startswith("SHOW DEVICES"):
                response = json.dumps(
                    {
                        "code": 200,
                        "expressions": None,
                        "column_names": [
                            "Device",
                            "Database",
                            "IsAligned",
                            "Template",
                            "TTL(ms)",
                        ],
                        "values": [["root.db.d"], ["root.db"], [False], [""], [None]],
                    }
                ).encode()
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
    handle, small, integer, length = c.c_void_p, c.c_short, c.c_int, c.c_ssize_t

    def api(name, *args):
        function = getattr(lib, name)
        function.argtypes = args
        function.restype = small
        return function

    alloc = api("SQLAllocHandle", small, handle, c.POINTER(handle))
    free = api("SQLFreeHandle", small, handle)
    connect = api(
        "SQLDriverConnect",
        handle,
        handle,
        c.c_char_p,
        small,
        handle,
        small,
        handle,
        c.c_ushort,
    )
    prepare = api("SQLPrepare", handle, c.c_char_p, integer)
    bind = api(
        "SQLBindParameter",
        handle,
        c.c_ushort,
        small,
        small,
        small,
        c.c_size_t,
        small,
        handle,
        length,
        c.POINTER(length),
    )
    execute = api("SQLExecute", handle)
    param_data = api("SQLParamData", handle, c.POINTER(handle))
    put = api("SQLPutData", handle, handle, length)
    direct = api("SQLExecDirect", handle, c.c_char_p, integer)
    get_attr = api("SQLGetStmtAttr", handle, integer, handle, integer, handle)
    set_attr = api("SQLSetStmtAttr", handle, integer, handle, integer)
    free_stmt = api("SQLFreeStmt", handle, c.c_ushort)
    set_desc = api("SQLSetDescField", handle, small, small, handle, integer)
    get_desc = api("SQLGetDescField", handle, small, small, handle, integer, handle)
    get_type_info = api("SQLGetTypeInfo", handle, small)
    get_desc_rec = api(
        "SQLGetDescRec",
        handle,
        small,
        handle,
        small,
        handle,
        handle,
        handle,
        handle,
        handle,
        handle,
        handle,
    )
    copy_desc = api("SQLCopyDesc", handle, handle)
    tables = api(
        "SQLTables", handle, handle, small, handle, small, handle, small, handle, small
    )
    env, dbc, stmt = handle(), handle(), handle()
    try:
        assert alloc(1, None, c.byref(env)) == 0
        assert alloc(2, env, c.byref(dbc)) == 0
        options = (
            f"SERVER=127.0.0.1;PORT={server.server_port};"
            "RESTFUL=1;ISTABLEMODEL=0;UID=root;PWD=root;"
        ).encode()
        assert connect(dbc, None, options, -3, None, 0, None, 0) == 0
        assert alloc(3, dbc, c.byref(stmt)) == 0

        def check(
            value,
            ctype,
            sqltype,
            expected,
            streamed=False,
            indicator=None,
            direct_execution=False,
        ):
            assert prepare(stmt, b"insert into root.d(time, v) values(1, ?)", -3) == 0
            ind = (
                length(-2 if streamed else indicator)
                if streamed or indicator is not None
                else None
            )
            assert (
                bind(
                    stmt,
                    1,
                    1,
                    ctype,
                    sqltype,
                    64,
                    0,
                    c.byref(value),
                    c.sizeof(value),
                    c.byref(ind) if ind is not None else None,
                )
                == 0
            )
            result = (
                direct(stmt, b"insert into root.d(time, v) values(1, ?)", -3)
                if direct_execution
                else execute(stmt)
            )
            if streamed:
                assert result == 99
                assert prepare(stmt, b"select ?, ?, ?", -3) == -1
                assert direct(stmt, b"select 1", -3) == -1
                assert execute(stmt) == -1
                assert set_attr(stmt, 22, handle(2), 0) == -1
                assert (
                    bind(
                        stmt,
                        1,
                        1,
                        ctype,
                        sqltype,
                        64,
                        0,
                        c.byref(value),
                        c.sizeof(value),
                        c.byref(ind),
                    )
                    == -1
                )
                token = handle()
                assert param_data(stmt, c.byref(token)) == 99
                assert token.value == c.addressof(value)
                assert (
                    put(
                        stmt,
                        c.byref(value),
                        indicator if indicator is not None else c.sizeof(value),
                    )
                    == 0
                )
                assert param_data(stmt, c.byref(token)) == 0
            else:
                assert result == 0
            assert requests[-1] == (
                "/rest/v2/nonQuery",
                f"insert into root.d(time, v) values(1, {expected})",
            ), requests[-1]

        check(c.c_double(1.2345678901234567), 8, 8, "1.2345678901234567")

        # Tail bytes make an incorrect wider read deterministic.
        class SmallBuffer(c.Structure):
            _fields_ = [("value", c.c_short), ("tail", c.c_short)]

        class TinyBuffer(c.Structure):
            _fields_ = [("value", c.c_byte), ("tail", c.c_byte * 3)]

        check(SmallBuffer(42, 1234), 99, 5, "42")
        check(TinyBuffer(42, (c.c_byte * 3)(1, 2, 3)), 99, -6, "42")
        check(c.create_string_buffer(b"12.3456"), 99, 3, "'12.3456'")
        check((c.c_ushort * 3)(0x4E2D, 0x6587, 0), 99, -9, "'中文'")
        check(
            Timestamp(2026, 9, 22, 1, 2, 3, 123456789),
            93,
            93,
            "'2026-09-22 01:02:03.123456789'",
        )
        check(c.create_string_buffer(b"it's text"), 99, 12, "'it''s text'")
        check(c.c_int(42), 4, 4, "42")
        check(c.c_int(42), 4, 4, "42", streamed=True)
        check(c.c_int(42), 4, 4, "42", direct_execution=True)
        check(c.c_int(42), 4, 4, "42", streamed=True, direct_execution=True)
        check((c.c_ubyte * 3)(0, 255, 39), -2, -3, "X'00FF27'", streamed=True)
        check(
            (c.c_ushort * 3)(0x4E2D, 0x6587, 0),
            -8,
            -9,
            "'中文'",
            streamed=True,
            indicator=-3,
        )
        # Explicit descriptor length and NULL-indicator pointers are independent.
        assert prepare(stmt, b"insert into root.d(time, v) values(1, ?)", -3) == 0
        value = c.create_string_buffer(b"abcdef")
        size, indicator, apd = length(3), length(0), handle()
        assert (
            bind(stmt, 1, 1, 1, 12, 64, 0, value, c.sizeof(value), c.byref(size)) == 0
        )
        assert get_attr(stmt, 10011, c.byref(apd), 0, None) == 0
        assert set_desc(apd, 1, 1009, c.byref(indicator), 0) == 0
        assert execute(stmt) == 0
        assert requests[-1][1].endswith("(1, 'abc')"), requests[-1]
        assert direct(stmt, b"/* comment */ SELECT 1", -3) == 0
        assert requests[-1][0] == "/rest/v2/query"

        # Validate every marker before requesting streamed data from the caller.
        assert free_stmt(stmt, 3) == 0  # SQL_RESET_PARAMS
        value, indicator = c.c_int(42), length(-2)
        assert bind(stmt, 1, 1, 4, 4, 0, 0, c.byref(value), 0, c.byref(indicator)) == 0
        sql = b"insert into root.d(time, v) values(?, ?)"
        before = len(requests)
        assert prepare(stmt, sql, -3) == 0
        assert execute(stmt) == -1
        assert direct(stmt, sql, -3) == -1
        token = handle()
        assert param_data(stmt, c.byref(token)) == -1
        assert len(requests) == before

        # Arrays apply to parameterized SQL, not subsequent literal statements.
        assert free_stmt(stmt, 3) == 0
        values = (c.c_int * 3)(11, 22, 33)
        assert set_attr(stmt, 22, handle(3), 0) == 0  # SQL_ATTR_PARAMSET_SIZE
        assert bind(stmt, 1, 1, 4, 4, 0, 0, values, c.sizeof(c.c_int), None) == 0
        sql = b"insert into root.d(time, v) values(1, ?)"
        for direct_execution in (False, True):
            before = len(requests)
            if direct_execution:
                assert direct(stmt, sql, -3) == 0
            else:
                assert prepare(stmt, sql, -3) == 0
                assert execute(stmt) == 0
            assert [request[1] for request in requests[before:]] == [
                f"insert into root.d(time, v) values(1, {value})" for value in values
            ]
            literal = b"create database root.repro"
            before = len(requests)
            if direct_execution:
                assert direct(stmt, literal, -3) == 0
            else:
                assert prepare(stmt, literal, -3) == 0
                assert execute(stmt) == 0
            assert [request[1] for request in requests[before:]] == [literal.decode()]
        # Read the IRD without SQLNumResultCols (which also refreshes it).
        assert set_attr(stmt, 22, handle(1), 0) == 0
        assert get_type_info(stmt, 0) == 0
        ird, count = handle(), small()
        assert get_attr(stmt, 10012, c.byref(ird), 0, None) == 0
        descriptor_copy = handle()
        assert alloc(4, dbc, c.byref(descriptor_copy)) == 0
        try:
            assert copy_desc(ird, descriptor_copy) == 0
            assert get_desc(descriptor_copy, 0, 1001, c.byref(count), 0, None) == 0
            assert count.value == 19, count.value
        finally:
            assert free(4, descriptor_copy) == 0
        assert direct(stmt, b"select 1", -3) == 0
        assert get_type_info(stmt, 0) == 0
        name = c.create_string_buffer(64)
        assert (
            get_desc_rec(
                ird, 19, name, len(name), None, None, None, None, None, None, None
            )
            == 0
        )
        assert name.value == b"INTERVAL_PRECISION", name.value
        assert get_desc(ird, 0, 1001, c.byref(count), 0, None) == 0
        assert count.value == 19, count.value
        name = c.create_string_buffer(64)
        assert get_desc(ird, 1, 1011, name, len(name), None) == 0
        assert name.value == b"TYPE_NAME", name.value
        assert tables(stmt, None, 0, None, 0, None, 0, None, 0) == 0
        assert get_desc(ird, 0, 1001, c.byref(count), 0, None) == 0
        assert count.value == 5, count.value
        assert get_desc(ird, 1, 1011, name, len(name), None) == 0
        assert name.value == b"TABLE_CAT", name.value
        print("ODBC parameter serialization checks passed")
    finally:
        for kind, value in ((3, stmt), (2, dbc), (1, env)):
            if value:
                free(kind, value)
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
