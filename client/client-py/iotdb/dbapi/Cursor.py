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
#

import logging
import warnings

from iotdb.Session import Session

from .Exceptions import ProgrammingError

logger = logging.getLogger("IoTDB")


class Cursor(object):
    def __init__(self, connection, session: Session, sqlalchemy_mode):
        self.__connection = connection
        self.__session = session
        self.__sqlalchemy_mode = sqlalchemy_mode
        self.__arraysize = 1
        self.__is_close = False
        self.__result = None
        self.__rows = None
        self.__rowcount = -1

    @property
    def description(self):
        """
        This read-only attribute is a sequence of 7-item sequences.
        """
        if self.__is_close or not self.__result["col_names"]:
            return

        description = []

        col_names = self.__result["col_names"]
        col_types = self.__result["col_types"]

        for i in range(len(col_names)):
            description.append(
                (
                    col_names[i],
                    None if self.__sqlalchemy_mode is True else col_types[i].value,
                    None,
                    None,
                    None,
                    None,
                    col_names[i] == "Time",
                )
            )
        return tuple(description)

    @property
    def arraysize(self):
        """
        This read/write attribute specifies the number of rows to fetch at a time with .fetchmany().
        """
        return self.__arraysize

    @arraysize.setter
    def arraysize(self, value):
        """
        Set the arraysize.
        :param value: arraysize
        """
        try:
            self.__arraysize = int(value)
        except TypeError:
            self.__arraysize = 1

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        .execute*() produced (for DQL statements like ``SELECT``) or affected
        (for DML statements like ``DELETE`` or ``INSERT`` return 0 if successful, -1 if unsuccessful).
        """
        if self.__is_close or self.__result is None or "row_count" not in self.__result:
            return -1
        return self.__result.get("row_count", -1)

    def execute(self, operation, parameters=None):
        """
        Prepare and execute a database operation (query or command).
        :param operation: a database operation
        :param parameters: parameters of the operation
        """
        if self.__connection.is_close:
            raise ProgrammingError("Connection closed!")

        if self.__is_close:
            raise ProgrammingError("Cursor closed!")

        if parameters is None:
            sql = operation
        else:
            sql = operation % parameters

        time_index = []
        time_names = []
        if self.__sqlalchemy_mode:
            sql_seqs = []
            seqs = sql.split("\n")
            for seq in seqs:
                if seq.find("FROM Time Index") >= 0:
                    time_index = [
                        int(index)
                        for index in seq.replace("FROM Time Index", "").split()
                    ]
                elif seq.find("FROM Time Name") >= 0:
                    time_names = [
                        name for name in seq.replace("FROM Time Name", "").split()
                    ]
                else:
                    sql_seqs.append(seq)
            sql = "\n".join(sql_seqs)

        try:
            data_set = self.__session.execute_statement(sql)
            col_names = None
            col_types = None
            rows = []

            if data_set:
                data = data_set.todf()

                if self.__sqlalchemy_mode and time_index:
                    time_column = data.columns[0]
                    time_column_value = data.Time
                    del data[time_column]
                    for i in range(len(time_index)):
                        data.insert(time_index[i], time_names[i], time_column_value)

                col_names = data.columns.tolist()
                col_types = data_set.get_column_types()
                rows = data.values.tolist()
                data_set.close_operation_handle()

            self.__result = {
                "col_names": col_names,
                "col_types": col_types,
                "rows": rows,
                "row_count": len(rows),
            }
        except Exception:
            logger.error("failed to execute statement:{}".format(sql))
            self.__result = {
                "col_names": None,
                "col_types": None,
                "rows": [],
                "row_count": -1,
            }
        self.__rows = iter(self.__result["rows"])

    def executemany(self, operation, seq_of_parameters=None):
        """
        Prepare a database operation (query or command) and then execute it
        against all parameter sequences or mappings found in the sequence
        ``seq_of_parameters``
        :param operation: a database operation
        :param seq_of_parameters: pyformat style parameter list of the operation
        """
        if self.__connection.is_close:
            raise ProgrammingError("Connection closed!")

        if self.__is_close:
            raise ProgrammingError("Cursor closed!")

        rows = []
        if seq_of_parameters is None:
            self.execute(operation)
            rows.extend(self.__result["rows"])
        else:
            for parameters in seq_of_parameters:
                self.execute(operation, parameters)
                rows.extend(self.__result["rows"])

        self.__result["rows"] = rows
        self.__rows = iter(self.__result["rows"])

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or None when no more data is available.
        Alias for ``next()``.
        """
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchmany(self, count=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        if count is None:
            count = self.__arraysize
        if count == 0:
            return self.fetchall()
        result = []
        for i in range(count):
            try:
                result.append(self.next())
            except StopIteration:
                pass
        return result

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        result = []
        iterate = True
        while iterate:
            try:
                result.append(self.next())
            except StopIteration:
                iterate = False
        return result

    def next(self):
        """
        Return the next row of a query result set, respecting if cursor was
        closed.
        """
        if self.__result is None:
            raise ProgrammingError(
                "No result available. execute() or executemany() must be called first."
            )
        elif not self.__is_close:
            return next(self.__rows)
        else:
            raise ProgrammingError("Cursor closed!")

    __next__ = next

    def close(self):
        """
        Close the cursor now.
        """
        self.__is_close = True
        self.__result = None

    def setinputsizes(self, sizes):
        """
        Not supported method.
        """
        pass

    def setoutputsize(self, size, column=None):
        """
        Not supported method.
        """
        pass

    def __iter__(self):
        """
        Support iterator interface:
        http://legacy.python.org/dev/peps/pep-0249/#iter
        This iterator is shared. Advancing this iterator will advance other
        iterators created from this cursor.
        """
        warnings.warn("DB-API extension cursor.__iter__() used")
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
