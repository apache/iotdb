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

from IoTDBConstants import *
import struct


class Tablet(object):

    def __init__(self, device_id, measurements, data_types, values, timestamps):
        """
        creating a tablet for insertion
          for example, considering device: root.sg1.d1
            timestamps,     m1,    m2,     m3
                     1,  125.3,  True,  text1
                     2,  111.6, False,  text2
                     3,  688.6,  True,  text3
        Notice: The tablet should not have empty cell
                The tablet will be sorted at the initialization by timestamps

        :param device_id: String, IoTDB time series path to device layer (without sensor).
        :param measurements: List, sensors.
        :param data_types: TSDataType List, specify value types for sensors.
        :param values: 2-D List, the values of each row should be the outer list element.
        :param timestamps: List.
        """
        if len(timestamps) != len(values):
            print("Input error! len(timestamps) does not equal to len(values)!")
            # could raise an error here.

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
        format_str_list = [">"]
        values_tobe_packed = []
        for timestamp in self.__timestamps:
            format_str_list.append("q")
            values_tobe_packed.append(timestamp)

        format_str = ''.join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

    def get_binary_values(self):
        format_str_list = [">"]
        values_tobe_packed = []
        for i in range(self.__column_number):
            if self.__data_types[i] == TSDataType.BOOLEAN:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("?")
                for j in range(self.__row_number):
                    values_tobe_packed.append(self.__values[j][i])
            elif self.__data_types[i] == TSDataType.INT32:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("i")
                for j in range(self.__row_number):
                    values_tobe_packed.append(self.__values[j][i])
            elif self.__data_types[i] == TSDataType.INT64:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("q")
                for j in range(self.__row_number):
                    values_tobe_packed.append(self.__values[j][i])
            elif self.__data_types[i] == TSDataType.FLOAT:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("f")
                for j in range(self.__row_number):
                    values_tobe_packed.append(self.__values[j][i])
            elif self.__data_types[i] == TSDataType.DOUBLE:
                format_str_list.append(str(self.__row_number))
                format_str_list.append("d")
                for j in range(self.__row_number):
                    values_tobe_packed.append(self.__values[j][i])
            elif self.__data_types[i] == TSDataType.TEXT:
                for j in range(self.__row_number):
                    value_bytes = bytes(self.__values[j][i], 'utf-8')
                    format_str_list.append("i")
                    format_str_list.append(str(len(value_bytes)))
                    format_str_list.append("s")
                    values_tobe_packed.append(len(value_bytes))
                    values_tobe_packed.append(value_bytes)
            else:
                print("Unsupported data type:" + str(self.__data_types[i]))
                # could raise an error here.
                return

        format_str = ''.join(format_str_list)
        return struct.pack(format_str, *values_tobe_packed)

