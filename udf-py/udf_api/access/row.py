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

from abc import ABCMeta, abstractmethod

from udf_api.type.binary import Binary
from udf_api.type.type import Type


class Row(metaclass=ABCMeta):
    @abstractmethod
    def get_time(self) -> int:
        """
        Returns the timestamp of this row.

        :return: timestamp
        """
        pass

    @abstractmethod
    def get_int(self, column_index: int) -> int:
        """
        Returns the int value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.INT32.

        :return: the int value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_long(self, column_index: int) -> int:
        """
        Returns the long value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.INT64.

        :return: the long value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_float(self, column_index: int) -> float:
        """
        Returns the float value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.FLOAT.

        :return: the float value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_double(self, column_index: int) -> float:
        """
        Returns the double value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.DOUBLE.

        :return: the double value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_boolean(self, column_index: int) -> bool:
        """
        Returns the bool value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.BOOLEAN.

        :return: the bool value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_binary(self, column_index: int) -> Binary:
        """
        Returns the Binary value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.TEXT.

        :return: the Binary value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_string(self, column_index: int) -> str:
        """
        Returns the str value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.TEXT.

        :return: the str value at the specified column in this row
        """
        pass

    @abstractmethod
    def get_data_type(self, column_index: int) -> Type:
        """
        Returns the actual data type of the value at the specified column in this row.

        Users need to ensure that the data type of the specified column is Type.TEXT.

        :return: the actual data type of the value at the specified column in this row
        """
        pass

    @abstractmethod
    def is_null(self, column_index: int) -> bool:
        """
        Returns true if the value of the specified column is null.

        :return: true if the value of the specified column is null
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        Returns the number of columns

        :return: the number of columns
        """
        pass
