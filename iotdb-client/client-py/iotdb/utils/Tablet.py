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

import struct
from enum import unique, IntEnum
from typing import List, Union

from iotdb.tsfile.utils.date_utils import parse_date_to_int
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import (
    TSDataType,
    TS_TABLET_LENGTH_PREFIXED,
)
from iotdb.utils.object_column import encode_object_cell


@unique
class ColumnType(IntEnum):
    TAG = 0
    FIELD = 1
    ATTRIBUTE = 2

    def n_copy(self, n):
        result = []
        for i in range(n):
            result.append(self)
        return result


class Tablet(object):
    def __init__(
        self,
        insert_target_name: str,
        column_names: List[str],
        data_types: List[TSDataType],
        values: List[List],
        timestamps: List[int],
        column_types: List[ColumnType] = None,
    ):
        """
        creating a tablet for insertion
          for example using tree-model, considering device: root.sg1.d1
            timestamps,     m1,    m2,     m3
                     1,  125.3,  True,  text1
                     2,  111.6, False,  text2
                     3,  688.6,  True,  text3
          for example using table-model, considering table: table1
            timestamps,    id1,  attr1,    m1
                     1,  id:1,  attr:1,   1.0
                     2,  id:1,  attr:1,   2.0
                     3,  id:2,  attr:2,   3.0
        Notice: The tablet will be sorted at the initialization by timestamps
        :param insert_target_name: Str, DeviceId if using tree model or TableName when using table model.
        :param column_names: Str List, names of columns
        :param data_types: TSDataType List, specify value types for columns
        :param values: 2-D List, the values of each row should be the outer list element
        :param timestamps: int List, contains the timestamps
        :param column_types: ColumnType List, marking the type of each column, can be none for tree-view interfaces.
        """
        if len(timestamps) != len(values):
            raise RuntimeError(
                "Input error! len(timestamps) does not equal to len(values)!"
            )

        if not Tablet.check_sorted(timestamps):
            sorted_zipped = sorted(zip(timestamps, values))
            result = zip(*sorted_zipped)
            self.__timestamps, self.__values = [list(x) for x in result]
        else:
            self.__values = values
            self.__timestamps = timestamps

        self.__insert_target_name = insert_target_name
        self.__measurements = column_names
        self.__data_types = data_types
        self.__row_number = len(timestamps)
        self.__column_number = len(column_names)
        if column_types is None:
            self.__column_types = ColumnType.n_copy(
                ColumnType.FIELD, self.__column_number
            )
        else:
            self.__column_types = column_types

    def add_value_object(
        self,
        row_index: int,
        column_index: int,
        is_eof: bool,
        offset: int,
        content: bytes,
    ):
        """
        Write one OBJECT column cell, same semantics as Java/C++ Tablet.addValue(
        rowIndex, columnIndex, isEOF, offset, content).
        """
        if row_index < 0 or row_index >= self.__row_number:
            raise IndexError("row_index out of range")
        if column_index < 0 or column_index >= self.__column_number:
            raise IndexError("column_index out of range")
        if self.__data_types[column_index] != TSDataType.OBJECT:
            raise TypeError(
                "add_value_object requires TSDataType.OBJECT column, got %s"
                % self.__data_types[column_index]
            )
        self.__values[row_index][column_index] = encode_object_cell(
            is_eof, offset, content
        )

    def add_value_object_by_name(
        self,
        column_name: str,
        row_index: int,
        is_eof: bool,
        offset: int,
        content: bytes,
    ):
        if column_name not in self.__measurements:
            raise KeyError("column %r not found" % column_name)
        column_index = self.__measurements.index(column_name)
        self.add_value_object(row_index, column_index, is_eof, offset, content)

    @staticmethod
    def check_sorted(timestamps):
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i - 1]:
                return False
        return True

    def get_measurements(self):
        return self.__measurements

    def get_data_types(self):
        return self.__data_types

    def get_column_categories(self):
        return self.__column_types

    def get_row_number(self):
        return self.__row_number

    def get_insert_target_name(self):
        return self.__insert_target_name

    def get_timestamps(self):
        return self.__timestamps

    def get_values(self):
        return self.__values

    def get_binary_timestamps(self):
        format_str_list = [">"]
        values_tobe_packed = []
        for timestamp in self.__timestamps:
            format_str_list.append("q")
            values_tobe_packed.append(timestamp)

        format_str = "".join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def get_binary_values(self):
        format_str_list = [">"]
        values_tobe_packed = []
        bitmaps: List[Union[BitMap, None]] = []
        has_none = False
        for i in range(self.__column_number):
            bitmap = None
            bitmaps.append(bitmap)
            data_type = self.__data_types[i]
            # BOOLEAN
            if data_type == TSDataType.BOOLEAN:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("?")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(self.__values[j][i])
                    else:
                        values_tobe_packed.append(False)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # INT32
            elif data_type == TSDataType.INT32:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("i")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(self.__values[j][i])
                    else:
                        values_tobe_packed.append(0)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # INT64 or TIMESTAMP
            elif data_type in (TSDataType.INT64, TSDataType.TIMESTAMP):
                format_str_list.append(str(self.__row_number))
                format_str_list.append("q")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(self.__values[j][i])
                    else:
                        values_tobe_packed.append(0)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # FLOAT
            elif data_type == TSDataType.FLOAT:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("f")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(self.__values[j][i])
                    else:
                        values_tobe_packed.append(0)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # DOUBLE
            elif data_type == TSDataType.DOUBLE:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("d")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(self.__values[j][i])
                    else:
                        values_tobe_packed.append(0)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # TEXT, STRING, BLOB, OBJECT (OBJECT cells are already encoded bytes)
            elif data_type in TS_TABLET_LENGTH_PREFIXED:
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        if data_type == TSDataType.OBJECT:
                            value_bytes = self.__values[j][i]
                            if not isinstance(value_bytes, (bytes, bytearray)):
                                raise TypeError(
                                    "OBJECT column must be bytes (use add_value_object)"
                                )
                            value_bytes = bytes(value_bytes)
                        elif isinstance(self.__values[j][i], str):
                            value_bytes = bytes(self.__values[j][i], "utf-8")
                        else:
                            value_bytes = self.__values[j][i]
                        format_str_list.append("i")
                        format_str_list.append(str(len(value_bytes)))
                        format_str_list.append("s")
                        values_tobe_packed.append(len(value_bytes))
                        values_tobe_packed.append(value_bytes)
                    else:
                        value_bytes = bytes("", "utf-8")
                        format_str_list.append("i")
                        format_str_list.append(str(len(value_bytes)))
                        format_str_list.append("s")
                        values_tobe_packed.append(len(value_bytes))
                        values_tobe_packed.append(value_bytes)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            # DATE
            elif data_type == TSDataType.DATE:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("i")
                for j in range(self.__row_number):
                    if self.__values[j][i] is not None:
                        values_tobe_packed.append(
                            parse_date_to_int(self.__values[j][i])
                        )
                    else:
                        values_tobe_packed.append(0)
                        self.__mark_none_value(bitmaps, i, j)
                        has_none = True
            else:
                raise RuntimeError("Unsupported data type:" + str(self.__data_types[i]))

        if has_none:
            for i in range(self.__column_number):
                format_str_list.append("?")
                if bitmaps[i] is None:
                    values_tobe_packed.append(False)
                else:
                    values_tobe_packed.append(True)
                    format_str_list.append(str(self.__row_number // 8 + 1))
                    format_str_list.append("c")
                    for j in range(self.__row_number // 8 + 1):
                        values_tobe_packed.append(bytes([bitmaps[i].bits[j]]))
        format_str = "".join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def __mark_none_value(self, bitmaps, column, row):
        if bitmaps[column] is None:
            bitmaps[column] = BitMap(self.__row_number)
        bitmaps[column].mark(row)
