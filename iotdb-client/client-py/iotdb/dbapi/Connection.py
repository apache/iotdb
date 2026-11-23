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

import weakref

from iotdb.Session import Session

from .Cursor import Cursor
from .Exceptions import ConnectionError, InterfaceError, NotSupportedError


class Connection(object):
    def __init__(
        self,
        host,
        port,
        username=Session.DEFAULT_USER,
        password=Session.DEFAULT_PASSWORD,
        fetch_size=Session.DEFAULT_FETCH_SIZE,
        zone_id=Session.DEFAULT_ZONE_ID,
        enable_rpc_compression=False,
        sqlalchemy_mode=False,
        use_ssl=False,
        ca_certs=None,
        connection_timeout_in_ms=None,
        client_cert=None,
        client_key=None,
        sql_dialect="table",
        database=None,
    ):
        if sql_dialect not in ("tree", "table"):
            raise InterfaceError("sql_dialect must be either 'tree' or 'table'")

        try:
            self.__session = Session(
                host,
                port,
                username,
                password,
                fetch_size,
                zone_id,
                use_ssl=self.__to_bool(use_ssl),
                ca_certs=ca_certs,
                connection_timeout_in_ms=self.__to_optional_int(
                    connection_timeout_in_ms
                ),
                client_cert=client_cert,
                client_key=client_key,
            )
            self.__session.sql_dialect = sql_dialect
            self.__session.database = database
            self.__session.open(self.__to_bool(enable_rpc_compression))
        except Exception as error:
            raise ConnectionError(str(error)) from error

        self.__is_close = False
        self.__sqlalchemy_mode = bool(sqlalchemy_mode)
        # Track live cursors for connection cleanup without preventing their garbage collection.
        self.__cursors = weakref.WeakSet()

    def close(self):
        if self.__is_close:
            return

        first_error = None
        for cursor in list(self.__cursors):
            try:
                cursor._close_from_connection()
            except Exception as error:
                if first_error is None:
                    first_error = error
        try:
            self.__session.close()
        except Exception as error:
            if first_error is None:
                first_error = ConnectionError(str(error))
        finally:
            self.__is_close = True

        if first_error is not None:
            raise first_error

    def cursor(self):
        self.__ensure_open()
        cursor = Cursor(self, self.__session, self.__sqlalchemy_mode)
        self.__cursors.add(cursor)
        return cursor

    def commit(self):
        self.__ensure_open()

    def rollback(self):
        self.__ensure_open()
        raise NotSupportedError("IoTDB does not support transactions or rollback")

    @property
    def is_close(self):
        return self.__is_close

    def __enter__(self):
        self.__ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __ensure_open(self):
        if self.__is_close:
            raise InterfaceError("connection is closed")

    @staticmethod
    def __to_bool(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "yes", "y")
        return bool(value)

    @staticmethod
    def __to_optional_int(value):
        if value is None or value == "":
            return None
        return int(value)
