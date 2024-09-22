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

import numpy as np
from numpy import ndarray
from typing import List

from iotdb.tsfile.utils.DateUtils import parse_date_to_int
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.BitMap import BitMap
from iotdb.utils.Tablet import ColumnType


class NumpyTablet(object):
    def __init__(
        self,
        insert_target_name: str,
        column_names: List[str],
        data_types: List[TSDataType],
        values: List[ndarray],
        timestamps: ndarray,
        bitmaps: List[BitMap] = None,
        column_types: List[ColumnType] = None,
    ):
        """
        creating a numpy tablet for insertion
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
        :param values: ndarray List, one ndarray contains the value of one column
        :param timestamps: ndarray, contains the timestamps
        :param bitmaps: BitMap list, one bitmap records the position of none value in a column
        :param column_types: ColumnType List, marking the type of each column, can be none for tree-view interfaces.
        """
        if len(values) > 0 and len(values[0]) != len(timestamps):
            raise RuntimeError(
                "Input error! len(timestamps) does not equal to len(values[0])!"
            )
        if len(values) != len(data_types):
            raise RuntimeError(
                "Input error! len(values) does not equal to len(data_types)!"
            )

        if not self.check_sorted(timestamps):
            index = timestamps.argsort()
            timestamps = timestamps[index]
            for i in range(len(values)):
                values[i] = values[i][index]

        if timestamps.dtype != TSDataType.INT64.np_dtype():
            timestamps = timestamps.astype(TSDataType.INT64.np_dtype())
        for i in range(len(values)):
            if values[i].dtype != data_types[i].np_dtype():
                values[i] = values[i].astype(data_types[i].np_dtype())

        self.__values = values
        self.__timestamps = timestamps
        self.__insert_target_name = insert_target_name
        self.__measurements = column_names
        self.__data_types = data_types
        self.__row_number = len(timestamps)
        self.__column_number = len(column_names)
        self.bitmaps = bitmaps
        if column_types is None:
            self.__column_types = ColumnType.n_copy(
                ColumnType.MEASUREMENT, self.__column_number
            )
        else:
            self.__column_types = column_types

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
        return self.__timestamps.tobytes()

    def get_binary_values(self):
        bs_len = 0
        bs_list = []
        for data_type, value in zip(self.__data_types, self.__values):
            # BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
            if (
                data_type == 0
                or data_type == 1
                or data_type == 2
                or data_type == 3
                or data_type == 4
                or data_type == 8
            ):
                bs = value.tobytes()
            # TEXT, STRING, BLOB
            elif data_type == 5 or data_type == 11 or data_type == 10:
                format_str_list = [">"]
                values_tobe_packed = []
                for str_list in value:
                    # For TEXT, it's same as the original solution
                    if isinstance(str_list, str):
                        value_bytes = bytes(str_list, "utf-8")
                    else:
                        value_bytes = str_list
                    format_str_list.append("i")
                    format_str_list.append(str(len(value_bytes)))
                    format_str_list.append("s")
                    values_tobe_packed.append(len(value_bytes))
                    values_tobe_packed.append(value_bytes)
                format_str = "".join(format_str_list)
                bs = struct.pack(format_str, *values_tobe_packed)
            # DATE
            elif data_type == 9:
                bs = (
                    np.vectorize(parse_date_to_int)(value)
                    .astype(np.dtype(">i4"))
                    .tobytes()
                )
            else:
                raise RuntimeError("Unsupported data type:" + str(data_type))
            bs_list.append(bs)
            bs_len += len(bs)

        if self.bitmaps is not None:
            format_str_list = [">"]
            values_tobe_packed = []
            for i in range(self.__column_number):
                format_str_list.append("?")
                if self.bitmaps[i] is None or self.bitmaps[i].is_all_unmarked():
                    values_tobe_packed.append(False)
                else:
                    values_tobe_packed.append(True)
                    format_str_list.append(str(self.__row_number // 8 + 1))
                    format_str_list.append("c")
                    for j in range(self.__row_number // 8 + 1):
                        values_tobe_packed.append(bytes([self.bitmaps[i].bits[j]]))
            format_str = "".join(format_str_list)
            bs = struct.pack(format_str, *values_tobe_packed)
            bs_list.append(bs)
            bs_len += len(bs)
        ret = memoryview(bytearray(bs_len))
        offset = 0
        for bs in bs_list:
            _l = len(bs)
            ret[offset : offset + _l] = bs
            offset += _l
        return bytes(ret)

    def mark_none_value(self, column, row):
        if self.bitmaps is None:
            self.bitmaps = []
            for i in range(self.__column_number):
                self.bitmaps.append(None)
        if self.bitmaps[column] is None:
            self.bitmaps[column] = BitMap(self.__row_number)
        self.bitmaps[column].mark(row)
