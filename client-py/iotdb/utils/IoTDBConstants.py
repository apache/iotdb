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

from enum import Enum, unique


@unique
class TSDataType(Enum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value


@unique
class TSEncoding(Enum):
    PLAIN = 0
    DICTIONARY = 1
    RLE = 2
    DIFF = 3
    TS_2DIFF = 4
    BITMAP = 5
    GORILLA_V1 = 6
    REGULAR = 7
    GORILLA = 8

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value


@unique
class Compressor(Enum):
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    SDT = 4
    PAA = 5
    PLA = 6
    LZ4 = 7

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value