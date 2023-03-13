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
import numpy as np


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

    def __hash__(self):
        return self.value

    def np_dtype(self):
        return {
            TSDataType.BOOLEAN: np.dtype(">?"),
            TSDataType.FLOAT: np.dtype(">f4"),
            TSDataType.DOUBLE: np.dtype(">f8"),
            TSDataType.INT32: np.dtype(">i4"),
            TSDataType.INT64: np.dtype(">i8"),
            TSDataType.TEXT: np.dtype("str"),
        }[self]


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
    ZIGZAG = 9
    FREQ = 10
    CHIMP = 11

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self):
        return self.value


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
    ZSTD = 8

    # this method is implemented to avoid the issue reported by:
    # https://bugs.python.org/issue30545
    def __eq__(self, other) -> bool:
        return self.value == other.value

    def __hash__(self):
        return self.value
