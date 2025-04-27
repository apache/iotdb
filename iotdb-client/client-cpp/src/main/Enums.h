/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef IOTDB_DATATYPES_H
#define IOTDB_DATATYPES_H

namespace Version {
    enum Version {
        V_0_12, V_0_13, V_1_0
    };
}

namespace CompressionType {
    enum CompressionType {
        UNCOMPRESSED = (char) 0,
        SNAPPY = (char) 1,
        GZIP = (char) 2,
        LZO = (char) 3,
        SDT = (char) 4,
        PAA = (char) 5,
        PLA = (char) 6,
        LZ4 = (char) 7,
        ZSTD = (char) 8,
        LZMA2 = (char) 9,
    };
}

namespace TSDataType {
    enum TSDataType {
        BOOLEAN = (char) 0,
        INT32 = (char) 1,
        INT64 = (char) 2,
        FLOAT = (char) 3,
        DOUBLE = (char) 4,
        TEXT = (char) 5,
        VECTOR = (char) 6,
        NULLTYPE = (char) 254,
        INVALID_DATATYPE = (char) 255
    };
}

namespace TSEncoding {
    enum TSEncoding {
        PLAIN = (char) 0,
        DICTIONARY = (char) 1,
        RLE = (char) 2,
        DIFF = (char) 3,
        TS_2DIFF = (char) 4,
        BITMAP = (char) 5,
        GORILLA_V1 = (char) 6,
        REGULAR = (char) 7,
        GORILLA = (char) 8,
        ZIGZAG = (char) 9,
	    FREQ = (char) 10,
        INVALID_ENCODING = (char) 255
    };
}

namespace TSStatusCode {
    enum TSStatusCode {
        SUCCESS_STATUS = 200,

        // System level
        INCOMPATIBLE_VERSION = 201,
        CONFIGURATION_ERROR = 202,
        START_UP_ERROR = 203,
        SHUT_DOWN_ERROR = 204,

        // General Error
        UNSUPPORTED_OPERATION = 300,
        EXECUTE_STATEMENT_ERROR = 301,
        MULTIPLE_ERROR = 302,
        ILLEGAL_PARAMETER = 303,
        OVERLAP_WITH_EXISTING_TASK = 304,
        INTERNAL_SERVER_ERROR = 305,

        // Client,
        REDIRECTION_RECOMMEND = 400,

        // Schema Engine
        DATABASE_NOT_EXIST = 500,
        DATABASE_ALREADY_EXISTS = 501,
        SERIES_OVERFLOW = 502,
        TIMESERIES_ALREADY_EXIST = 503,
        TIMESERIES_IN_BLACK_LIST = 504,
        ALIAS_ALREADY_EXIST = 505,
        PATH_ALREADY_EXIST = 506,
        METADATA_ERROR = 507,
        PATH_NOT_EXIST = 508,
        ILLEGAL_PATH = 509,
        CREATE_TEMPLATE_ERROR = 510,
        DUPLICATED_TEMPLATE = 511,
        UNDEFINED_TEMPLATE = 512,
        TEMPLATE_NOT_SET = 513,
        DIFFERENT_TEMPLATE = 514,
        TEMPLATE_IS_IN_USE = 515,
        TEMPLATE_INCOMPATIBLE = 516,
        SEGMENT_NOT_FOUND = 517,
        PAGE_OUT_OF_SPACE = 518,
        RECORD_DUPLICATED=519,
        SEGMENT_OUT_OF_SPACE = 520,
        PBTREE_FILE_NOT_EXISTS = 521,
        OVERSIZE_RECORD = 522,
        PBTREE_FILE_REDO_LOG_BROKEN = 523,
        TEMPLATE_NOT_ACTIVATED = 524,

        // Storage Engine
        SYSTEM_READ_ONLY = 600,
        STORAGE_ENGINE_ERROR = 601,
        STORAGE_ENGINE_NOT_READY = 602,
    };
}

#endif
