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

from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor


class ReadWriteUtils:
    BOOLEAN_LEN = 1
    SHORT_LEN = 2
    INT_LEN = 4
    LONG_LEN = 8
    DOUBLE_LEN = 8
    FLOAT_LEN = 4
    BIT_LEN = 0.125
    NO_BYTE_TO_READ = -1
    magicStringBytes = []
    RETURN_ERROR = "Intend to read %d bytes but %d are actually returned"
    URN_ERROR = "Intend to read %d bytes but %d are actually returned"

    @classmethod
    def write(cls, *args, **kwargs):
        value, format_str_list, values_tobe_packed = args
        if isinstance(value, bool):
            cls.write_bool(value, format_str_list, values_tobe_packed)
        elif isinstance(value, str):
            cls.write_str(value, format_str_list, values_tobe_packed)
        elif isinstance(value, int):
            cls.write_int(value, format_str_list, values_tobe_packed)
        elif isinstance(value, TSDataType):
            cls.write_byte(value.value, format_str_list, values_tobe_packed)
        elif isinstance(value, TSEncoding):
            cls.write_byte(value.value, format_str_list, values_tobe_packed)
        elif isinstance(value, Compressor):
            cls.write_byte(value.value, format_str_list, values_tobe_packed)

    @classmethod
    def write_str(cls, s: str, format_str_list, values_tobe_packed):
        if s is None:
            cls.write_int(cls.NO_BYTE_TO_READ, format_str_list, values_tobe_packed)

        value_bytes = bytes(s, "utf-8")
        format_str_list.append("i")
        format_str_list.append(str(len(value_bytes)))
        format_str_list.append("s")

        values_tobe_packed.append(len(value_bytes))
        values_tobe_packed.append(value_bytes)

    @classmethod
    def write_int(cls, i: int, format_str_list, values_tobe_packed):
        format_str_list.append("i")
        values_tobe_packed.append(i)

    @classmethod
    def write_bool(cls, flag: bool, format_str_list, values_tobe_packed):
        format_str_list.append("?")
        values_tobe_packed.append(flag)

    @classmethod
    def write_byte(cls, b, format_str_list, values_tobe_packed):
        format_str_list.append("b")
        values_tobe_packed.append(b)
