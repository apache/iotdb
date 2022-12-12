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


class Binary:
    __values: bytes = None
    __string_cache: str = None

    @staticmethod
    def value_of(s: str, encoding="utf-8"):
        instance = Binary(bytes(s, encoding))
        instance.__string_cache = s
        return instance

    def __init__(self, values: bytes = None):
        self.__values = values

    def is_null(self) -> bool:
        return self.__values is None

    def get_values(self) -> bytes:
        return self.__values

    def get_string_value(self, encoding: str = "utf-8"):
        if self.__values is None:
            return None

        if self.__string_cache is None:
            self.__string_cache = str(self.__values, encoding)
        return self.__string_cache

    # Get length of values. Returns -1 if values is null.
    def __len__(self):
        return -1 if self.__values is None else len(self.__values)

    def __hash__(self):
        return 0 if self.__values is None else self.__values.__hash__()

    def __eq__(self, other):
        return self.__values == other.__value

    def __cmp__(self, other):
        if other is None:
            if self.__values is None:
                return 0
            else:
                return 1

        index = 1
        self_length = len(self)
        other_length = len(other)

        while index < self_length and index < other_length:
            if self.__values[index] == other.__values[index]:
                index += 1
                continue
            else:
                return self.__values[index] - other.__values[index]

        return self_length - other_length

    def __str__(self):
        return self.get_string_value()
