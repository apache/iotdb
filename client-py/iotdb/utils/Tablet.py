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

from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.BitMap import BitMap


class Tablet(object):
    def __init__(
        self, device_id, measurements, data_types, values, timestamps, use_new=False
    ):
        """
        creating a tablet for insertion
          for example, considering device: root.sg1.d1
            timestamps,     m1,    m2,     m3
                     1,  125.3,  True,  text1
                     2,  111.6, False,  text2
                     3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet will be sorted at the initialization by timestamps

        :param device_id: String, IoTDB time series path to device layer (without sensor).
        :param measurements: List, sensors.
        :param data_types: TSDataType List, specify value types for sensors.
        :param values: 2-D List, the values of each row should be the outer list element.
        :param timestamps: List.
        """
        if not use_new and len(timestamps) != len(values):
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

        self.__device_id = device_id
        self.__measurements = measurements
        self.__data_types = data_types
        self.__row_number = len(timestamps)
        self.__column_number = len(measurements)
        self.__use_new = use_new

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

    def get_row_number(self):
        return self.__row_number

    def get_device_id(self):
        return self.__device_id

    def get_binary_timestamps(self):
        if not self.__use_new:
            format_str_list = [">"]
            values_tobe_packed = []
            for timestamp in self.__timestamps:
                format_str_list.append("q")
                values_tobe_packed.append(timestamp)

            format_str = "".join(format_str_list)
            return struct.pack(format_str, *values_tobe_packed)
        else:
            return self.__timestamps.tobytes()

    def get_binary_values(self):
        if not self.__use_new:
            format_str_list = [">"]
            values_tobe_packed = []
            bitmaps = []
            has_none = False
            for i in range(self.__column_number):
                bitmap = None
                bitmaps.insert(i, bitmap)
                if self.__data_types[i] == TSDataType.BOOLEAN:
                    format_str_list.append(str(self.__row_number))
                    format_str_list.append("?")
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            values_tobe_packed.append(self.__values[j][i])
                        else:
                            values_tobe_packed.append(False)
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                elif self.__data_types[i] == TSDataType.INT32:
                    format_str_list.append(str(self.__row_number))
                    format_str_list.append("i")
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            values_tobe_packed.append(self.__values[j][i])
                        else:
                            values_tobe_packed.append(0)
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                elif self.__data_types[i] == TSDataType.INT64:
                    format_str_list.append(str(self.__row_number))
                    format_str_list.append("q")
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            values_tobe_packed.append(self.__values[j][i])
                        else:
                            values_tobe_packed.append(0)
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                elif self.__data_types[i] == TSDataType.FLOAT:
                    format_str_list.append(str(self.__row_number))
                    format_str_list.append("f")
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            values_tobe_packed.append(self.__values[j][i])
                        else:
                            values_tobe_packed.append(0)
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                elif self.__data_types[i] == TSDataType.DOUBLE:
                    format_str_list.append(str(self.__row_number))
                    format_str_list.append("d")
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            values_tobe_packed.append(self.__values[j][i])
                        else:
                            values_tobe_packed.append(0)
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                elif self.__data_types[i] == TSDataType.TEXT:
                    for j in range(self.__row_number):
                        if self.__values[j][i] is not None:
                            value_bytes = bytes(self.__values[j][i], "utf-8")
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
                            self.__mark_none_value(bitmaps, bitmap, i, j)
                            has_none = True

                else:
                    raise RuntimeError(
                        "Unsupported data type:" + str(self.__data_types[i])
                    )

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
        else:
            bs_len = 0
            bs_list = []
            for i, value in enumerate(self.__values):
                if self.__data_types[i] == TSDataType.TEXT:
                    format_str_list = [">"]
                    values_tobe_packed = []
                    for str_list in value:
                        # Fot TEXT, it's same as the original solution
                        value_bytes = bytes(str_list, "utf-8")
                        format_str_list.append("i")
                        format_str_list.append(str(len(value_bytes)))
                        format_str_list.append("s")
                        values_tobe_packed.append(len(value_bytes))
                        values_tobe_packed.append(value_bytes)
                    format_str = "".join(format_str_list)
                    bs = struct.pack(format_str, *values_tobe_packed)
                else:
                    bs = value.tobytes()
                bs_list.append(bs)
                bs_len += len(bs)
            ret = memoryview(bytearray(bs_len))
            offset = 0
            for bs in bs_list:
                _l = len(bs)
                ret[offset : offset + _l] = bs
                offset += _l
            return ret

    def __mark_none_value(self, bitmaps, bitmap, column, row):
        if bitmap is None:
            bitmap = BitMap(self.__row_number)
            bitmaps.insert(column, bitmap)
        bitmap.mark(row)
