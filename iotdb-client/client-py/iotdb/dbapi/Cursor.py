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

import warnings
from numbers import Integral

from iotdb.Session import Session
from iotdb.utils.exception import IoTDBConnectionException
from iotdb.utils.exception import StatementExecutionException

from .Exceptions import (
    DataError,
    DatabaseError,
    Error,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .Parameters import format_operation


class Cursor(object):
    def __init__(self, connection, session: Session, sqlalchemy_mode=False, fetch_size=None):
        self.__connection = connection
        self.__session = session
        self.__sqlalchemy_mode = bool(sqlalchemy_mode)
        self.__fetch_size = fetch_size
        self.__arraysize = 1
        self.__is_close = False
        self.__data_set = None
        self.__description = None
        self.__has_result_set = False
        self.__rowcount = -1
        self.__operation = None
        self.__time_indexes = []
        self.__time_names = []

    @property
    def description(self):
        self.__ensure_open()
        return self.__description

    @property
    def arraysize(self):
        self.__ensure_open()
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, value):
        self.__ensure_open()
        self.__arraysize = self.__validate_fetch_size(value)

    @property
    def rowcount(self):
        self.__ensure_open()
        return self.__rowcount

    def execute(self, operation, parameters=None):
        self.__ensure_open()
        try:
            self.__release_result_checked()
        except Error:
            self.__reset_result()
            raise
        self.__reset_result()

        try:
            # PEP 249 retains the operation object for possible reuse.
            if self.__sqlalchemy_mode:
                operation, self.__time_indexes, self.__time_names = (
                    self.__strip_sqlalchemy_time_markers(operation)
                )
            self.__operation = operation
            sql = format_operation(operation, parameters)
            if self.__fetch_size is None:
                data_set = self.__session.execute_statement(sql)
            else:
                data_set = self.__session.execute_statement(
                    sql, timeout=0, fetch_size=self.__fetch_size
                )
            if data_set is None:
                return None

            self.__data_set = data_set
            column_names = list(data_set.get_column_names())
            column_types = list(data_set.get_column_types())
            description = [
                (name, data_type.value, None, None, None, None, None)
                for name, data_type in zip(column_names, column_types)
            ]
            if self.__sqlalchemy_mode:
                if self.__time_indexes:
                    for index, name in zip(self.__time_indexes, self.__time_names):
                        description.insert(
                            index, (name, None, None, None, None, None, None)
                        )
                else:
                    description.insert(0, ("Time", None, None, None, None, None, None))
                description = [
                    (name, None, None, None, None, None, None)
                    for name, *_ in description
                ]
            self.__description = tuple(description)
            self.__has_result_set = True
            return None
        except Error:
            self.__discard_result_after_error()
            raise
        except Exception as error:
            self.__discard_result_after_error()
            raise self.__translate_error(error) from error

    def executemany(self, operation, seq_of_parameters):
        self.__ensure_open()
        self.__release_result_checked()
        self.__reset_result()
        if seq_of_parameters is None:
            raise ProgrammingError("executemany() requires a sequence of parameters")

        try:
            iterator = iter(seq_of_parameters)
        except TypeError as error:
            raise ProgrammingError(
                "executemany() parameters must be an iterable"
            ) from error

        for parameters in iterator:
            self.execute(operation, parameters)
            if self.__has_result_set:
                self.__release_result_checked()
                self.__reset_result()
                raise NotSupportedError(
                    "executemany() does not support operations that return rows"
                )
        return None

    def fetchone(self):
        self.__ensure_fetchable()
        if self.__data_set is None:
            return None
        try:
            row = self.__data_set.next_tuple()
        except Exception as error:
            raise self.__translate_error(error) from error
        if row is None:
            self.__release_result_checked()
            return None
        if self.__sqlalchemy_mode:
            timestamp, *values = row
            if self.__time_indexes:
                for index in self.__time_indexes:
                    values.insert(index, timestamp)
                row = tuple(values)
        return row

    def fetchmany(self, size=None):
        self.__ensure_fetchable()
        fetch_size = (
            self.__arraysize if size is None else self.__validate_fetch_size(size)
        )
        rows = []
        for _ in range(fetch_size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        self.__ensure_fetchable()
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                return rows
            rows.append(row)

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    __next__ = next

    def close(self):
        if self.__is_close:
            return
        try:
            self.__release_result_checked()
        finally:
            self.__is_close = True
            self.__reset_result()

    def _close_from_connection(self):
        self.close()

    def setinputsizes(self, sizes):
        self.__ensure_open()

    def setoutputsize(self, size, column=None):
        self.__ensure_open()

    def __iter__(self):
        self.__ensure_open()
        warnings.warn("DB-API extension cursor.__iter__() used", stacklevel=2)
        return self

    def __enter__(self):
        self.__ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __ensure_open(self):
        if self.__is_close:
            raise InterfaceError("cursor is closed")
        if self.__connection.is_close:
            raise InterfaceError("connection is closed")

    def __ensure_fetchable(self):
        self.__ensure_open()
        if not self.__has_result_set:
            raise ProgrammingError(
                "the previous operation did not produce a result set"
            )

    def __release_result(self):
        if self.__data_set is None:
            return
        data_set = self.__data_set
        self.__data_set = None
        data_set.close_operation_handle()

    def __release_result_checked(self):
        try:
            self.__release_result()
        except Error:
            raise
        except Exception as error:
            raise self.__translate_error(error) from error

    def __reset_result(self):
        self.__data_set = None
        self.__description = None
        self.__has_result_set = False
        self.__rowcount = -1
        self.__operation = None

    def __discard_result_after_error(self):
        try:
            self.__release_result()
        except Exception:
            pass
        self.__reset_result()

    @staticmethod
    def __strip_sqlalchemy_time_markers(operation):
        if not isinstance(operation, str):
            return operation, [], []
        time_indexes = []
        time_names = []
        sql_lines = []
        for line in operation.split("\n"):
            if "FROM Time Index" in line:
                time_indexes = [
                    int(index)
                    for index in line.replace("FROM Time Index", "").split()
                ]
            elif "FROM Time Name" in line:
                time_names = line.replace("FROM Time Name", "").split()
            else:
                sql_lines.append(line)
        return "\n".join(sql_lines), time_indexes, time_names

    @staticmethod
    def __validate_fetch_size(value):
        if not isinstance(value, Integral):
            raise ProgrammingError("fetch size must be a non-negative integer")
        value = int(value)
        if value < 0:
            raise ProgrammingError("fetch size must be a non-negative integer")
        return value

    @staticmethod
    def __translate_error(error):
        if isinstance(error, Error):
            return error
        if isinstance(error, IoTDBConnectionException):
            return OperationalError(str(error))
        if isinstance(error, StatementExecutionException):
            status = getattr(error, "status", None)
            code = getattr(status, "code", None)
            if code in (205, 300, 707):
                return NotSupportedError(str(error))
            # Schema codes also include storage/availability failures; do not
            # classify the entire schema range as programming errors.
            if code in (
                303,
                500,
                501,
                503,
                505,
                506,
                508,
                509,
                511,
                512,
                513,
                514,
                515,
                516,
                524,
                525,
                527,
                528,
                529,
                530,
                550,
                551,
                552,
                554,
                560,
                616,
                617,
                700,
                701,
                704,
            ):
                return ProgrammingError(str(error))
            if code in (607, 612, 614, 615, 620):
                return DataError(str(error))
            if code in (305, 517, 521, 523, 705, 706, 711):
                return InternalError(str(error))
            if code in (
                502,
                518,
                520,
                526,
                535,
                536,
                600,
                602,
                606,
                611,
                709,
                712,
                713,
                715,
                717,
                719,
                720,
                721,
            ):
                return OperationalError(str(error))
            return DatabaseError(str(error))
        return InterfaceError(str(error))
