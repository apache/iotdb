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

# for package
from .IoTDBConstants import TSDataType
import numpy as np
import pandas as pd


class Field(object):
    def __init__(self, data_type, value=None):
        """
        :param data_type: TSDataType
        """
        self.__data_type = data_type
        self.value = value

    @staticmethod
    def copy(field):
        output = Field(field.get_data_type())
        if output.get_data_type() is not None:
            if output.get_data_type() == TSDataType.BOOLEAN:
                output.set_bool_value(field.get_bool_value())
            elif output.get_data_type() == TSDataType.INT32:
                output.set_int_value(field.get_int_value())
            elif output.get_data_type() == TSDataType.INT64:
                output.set_long_value(field.get_long_value())
            elif output.get_data_type() == TSDataType.FLOAT:
                output.set_float_value(field.get_float_value())
            elif output.get_data_type() == TSDataType.DOUBLE:
                output.set_double_value(field.get_double_value())
            elif output.get_data_type() == TSDataType.TEXT:
                output.set_binary_value(field.get_binary_value())
            else:
                raise Exception(
                    "unsupported data type {}".format(output.get_data_type())
                )
        return output

    def get_data_type(self):
        return self.__data_type

    def is_null(self):
        return self.__data_type is None or self.value is None or self.value is pd.NA

    def set_bool_value(self, value: bool):
        self.value = value

    def get_bool_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type != TSDataType.BOOLEAN:
            return None
        return self.value

    def set_int_value(self, value: int):
        self.value = value

    def get_int_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            return None
        return np.int32(self.value)

    def set_long_value(self, value: int):
        self.value = value

    def get_long_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type != TSDataType.INT64:
            return None
        return np.int64(self.value)

    def set_float_value(self, value: float):
        self.value = value

    def get_float_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type != TSDataType.FLOAT:
            return None
        return np.float32(self.value)

    def set_double_value(self, value: float):
        self.value = value

    def get_double_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type != TSDataType.DOUBLE:
            return None
        return np.float64(self.value)

    def set_binary_value(self, value: bytes):
        self.value = value

    def get_binary_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            raise Exception("Null Field Exception!")
        if self.__data_type != TSDataType.TEXT:
            return None
        return self.value

    def get_string_value(self):
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            return "None"
        elif self.__data_type == 5:
            return self.value.decode("utf-8")
        else:
            return str(self.get_object_value(self.__data_type))

    def __str__(self):
        return self.get_string_value()

    def get_object_value(self, data_type):
        """
        :param data_type: TSDataType
        """
        if self.__data_type is None or self.value is None or self.value is pd.NA:
            return None
        if data_type == 0:
            return bool(self.value)
        elif data_type == 1:
            return np.int32(self.value)
        elif data_type == 2:
            return np.int64(self.value)
        elif data_type == 3:
            return np.float32(self.value)
        elif data_type == 4:
            return np.float64(self.value)
        return self.value

    @staticmethod
    def get_field(value, data_type):
        """
        :param value: field value corresponding to the data type
        :param data_type: TSDataType
        """
        if value is None or value is pd.NA:
            return None
        field = Field(data_type, value)
        return field
