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
from iotdb.utils.Field import Field


class RowRecord(object):
    def __init__(self, timestamp, field_list: list = None):
        self.__timestamp = timestamp
        self.__field_list = field_list

    def add_field(self, value, data_type):
        self.__field_list.append(Field.get_field(value, data_type))

    def __str__(self):
        str_list = [str(self.__timestamp)]
        for field in self.__field_list:
            str_list.append("\t\t")
            str_list.append(str(field))
        return "".join(str_list)

    def get_timestamp(self):
        return self.__timestamp

    def set_timestamp(self, timestamp):
        self.__timestamp = timestamp

    def get_fields(self):
        return self.__field_list

    def set_fields(self, field_list):
        self.__field_list = field_list

    def set_field(self, index, field):
        self.__field_list[index] = field
