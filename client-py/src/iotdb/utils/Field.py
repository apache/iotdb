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

# for debug
# from IoTDBConstants import TSDataType

class Field(object):

    def __init__(self, data_type):
        """
        :param data_type: TSDataType
        """
        self.__data_type = data_type
        self.__bool_value = None
        self.__int_value = None
        self.__long_value = None
        self.__float_value = None
        self.__double_value = None
        self.__binary_value = None

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
                raise Exception("unsupported data type {}".format(output.get_data_type()))
        return output

    def get_data_type(self):
        return self.__data_type

    def is_null(self):
        return self.__data_type is None

    def set_bool_value(self, value):
        self.__bool_value = value

    def get_bool_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__bool_value

    def set_int_value(self, value):
        self.__int_value = value

    def get_int_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__int_value

    def set_long_value(self, value):
        self.__long_value = value

    def get_long_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__long_value

    def set_float_value(self, value):
        self.__float_value = value

    def get_float_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__float_value

    def set_double_value(self, value):
        self.__double_value = value

    def get_double_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__double_value

    def set_binary_value(self, value):
        self.__binary_value = value

    def get_binary_value(self):
        if self.__data_type is None:
            raise Exception("Null Field Exception!")
        return self.__binary_value

    def get_string_value(self):
        if self.__data_type is None:
            return "None"
        elif self.__data_type == TSDataType.BOOLEAN:
            return str(self.__bool_value)
        elif self.__data_type == TSDataType.INT64:
            return str(self.__long_value)
        elif self.__data_type == TSDataType.INT32:
            return str(self.__int_value)
        elif self.__data_type == TSDataType.FLOAT:
            return str(self.__float_value)
        elif self.__data_type == TSDataType.DOUBLE:
            return str(self.__double_value)
        elif self.__data_type == TSDataType.TEXT:
            return self.__binary_value.decode('utf-8')
        else:
            raise Exception("unsupported data type {}".format(self.__data_type))

    def __str__(self):
        return self.get_string_value()

    def get_object_value(self, data_type):
        """
        :param data_type: TSDataType
        """
        if self.__data_type is None:
            return None
        elif data_type == TSDataType.BOOLEAN:
            return self.get_bool_value()
        elif data_type == TSDataType.INT32:
            return self.get_int_value()
        elif data_type == TSDataType.INT64:
            return self.get_long_value()
        elif data_type == TSDataType.FLOAT:
            return self.get_float_value()
        elif data_type == TSDataType.DOUBLE:
            return self.get_double_value()
        elif data_type == TSDataType.TEXT:
            return self.get_binary_value()
        else:
            raise Exception("unsupported data type {}".format(data_type))

    @staticmethod
    def get_field(value, data_type):
        """
        :param value: field value corresponding to the data type
        :param data_type: TSDataType
        """
        if value is None:
            return None
        field = Field(data_type)
        if data_type == TSDataType.BOOLEAN:
            field.set_bool_value(value)
        elif data_type == TSDataType.INT32:
            field.set_int_value(value)
        elif data_type == TSDataType.INT64:
            field.set_long_value(value)
        elif data_type == TSDataType.FLOAT:
            field.set_float_value(value)
        elif data_type == TSDataType.DOUBLE:
            field.set_double_value(value)
        elif data_type == TSDataType.TEXT:
            field.set_binary_value(value)
        else:
            raise Exception("unsupported data type {}".format(data_type))
        return field

