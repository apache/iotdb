
from enum import Enum, unique


@unique
class TSDataType(Enum):
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5


@unique
class TSEncoding(Enum):
    PLAIN = 0
    PLAIN_DICTIONARY = 1
    RLE = 2
    DIFF = 3
    TS_2DIFF = 4
    BITMAP = 5
    GORILLA = 6
    REGULAR = 7


@unique
class Compressor(Enum):
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    SDT = 4
    PAA = 5
    PLA = 6
