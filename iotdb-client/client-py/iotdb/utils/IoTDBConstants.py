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
from datetime import date
from enum import unique, IntEnum
import numpy as np


@unique
class TSDataType(IntEnum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5
    TIMESTAMP = 8
    DATE = 9
    BLOB = 10
    STRING = 11
    OBJECT = 12

    def np_dtype(self):
        return {
            TSDataType.BOOLEAN: np.dtype(">?"),
            TSDataType.FLOAT: np.dtype(">f4"),
            TSDataType.DOUBLE: np.dtype(">f8"),
            TSDataType.INT32: np.dtype(">i4"),
            TSDataType.INT64: np.dtype(">i8"),
            TSDataType.TEXT: str,
            TSDataType.TIMESTAMP: np.dtype(">i8"),
            TSDataType.DATE: date,
            TSDataType.BLOB: bytes,
            TSDataType.STRING: str,
            TSDataType.OBJECT: np.dtype("O"),
        }[self]


# --- Groupings for tablet serialization and TsBlock / RPC result handling (protocol type bytes) ---

TS_TABLET_NUMERIC_FIXED = frozenset(
    {
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TIMESTAMP,
    }
)

TS_TABLET_LENGTH_PREFIXED = frozenset(
    {
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.OBJECT,
    }
)

TS_BLOCK_VALUE_TYPES = frozenset(
    {
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
        TSDataType.TIMESTAMP,
        TSDataType.DATE,
        TSDataType.BLOB,
        TSDataType.STRING,
        TSDataType.OBJECT,
    }
)

TS_RPC_BUFFER_AS_NUMPY = frozenset(
    {
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.BLOB,
        TSDataType.OBJECT,
    }
)

TS_RPC_BUFFER_UTF8_STRING = frozenset({TSDataType.TEXT, TSDataType.STRING})

TS_RPC_NO_BYTESWAP = frozenset({TSDataType.BLOB, TSDataType.OBJECT})

TS_RPC_NULLABLE_OBJECT_COLUMNS = frozenset(
    {
        TSDataType.TEXT,
        TSDataType.STRING,
        TSDataType.BLOB,
        TSDataType.OBJECT,
        TSDataType.DATE,
        TSDataType.TIMESTAMP,
    }
)


@unique
class TSEncoding(IntEnum):
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
    CHIMP = 11
    SPRINTZ = 12
    RLBE = 13


@unique
class Compressor(IntEnum):
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    SDT = 4
    PAA = 5
    PLA = 6
    LZ4 = 7
    ZSTD = 8
    LZMA2 = 9
