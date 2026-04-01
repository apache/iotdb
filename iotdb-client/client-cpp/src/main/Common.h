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
#ifndef IOTDB_COMMON_H
#define IOTDB_COMMON_H

#include <string>
#include <vector>
#include <exception>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TBufferTransports.h>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <cstdint>
#include <boost/optional/optional.hpp>

#include "client_types.h"
#include "common_types.h"

using namespace std;

using ::apache::thrift::protocol::TBinaryProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransport;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::transport::TBufferedTransport;
using ::apache::thrift::transport::TFramedTransport;
using ::apache::thrift::TException;

using namespace std;

constexpr int32_t EMPTY_DATE_INT = 10000101;

int32_t parseDateExpressionToInt(const boost::gregorian::date& date);
boost::gregorian::date parseIntToDate(int32_t dateInt);

std::string getTimePrecision(int32_t timeFactor);

std::string formatDatetime(const std::string& format, const std::string& precision,
                           int64_t timestamp, const std::string& zoneId);

std::tm convertToTimestamp(int64_t value, int32_t timeFactor);
std::tm int32ToDate(int32_t value);

namespace Version {
enum Version {
    V_0_12, V_0_13, V_1_0
};
}

namespace CompressionType {
enum CompressionType {
    UNCOMPRESSED = (char)0,
    SNAPPY = (char)1,
    GZIP = (char)2,
    LZO = (char)3,
    SDT = (char)4,
    PAA = (char)5,
    PLA = (char)6,
    LZ4 = (char)7,
    ZSTD = (char)8,
    LZMA2 = (char)9,
};
}

namespace TSDataType {
enum TSDataType {
    BOOLEAN = (char)0,
    INT32 = (char)1,
    INT64 = (char)2,
    FLOAT = (char)3,
    DOUBLE = (char)4,
    TEXT = (char)5,
    VECTOR = (char)6,
    UNKNOWN = (char)7,
    TIMESTAMP = (char)8,
    DATE = (char)9,
    BLOB = (char)10,
    STRING = (char)11,
    OBJECT = (char)12
};
}

TSDataType::TSDataType getDataTypeByStr(const std::string& typeStr);

namespace TSEncoding {
enum TSEncoding {
    PLAIN = (char)0,
    DICTIONARY = (char)1,
    RLE = (char)2,
    DIFF = (char)3,
    TS_2DIFF = (char)4,
    BITMAP = (char)5,
    GORILLA_V1 = (char)6,
    REGULAR = (char)7,
    GORILLA = (char)8,
    ZIGZAG = (char)9,
    FREQ = (char)10,
    INVALID_ENCODING = (char)255
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
    RECORD_DUPLICATED = 519,
    SEGMENT_OUT_OF_SPACE = 520,
    PBTREE_FILE_NOT_EXISTS = 521,
    OVERSIZE_RECORD = 522,
    PBTREE_FILE_REDO_LOG_BROKEN = 523,
    TEMPLATE_NOT_ACTIVATED = 524,

    // Storage Engine
    SYSTEM_READ_ONLY = 600,
    STORAGE_ENGINE_ERROR = 601,
    STORAGE_ENGINE_NOT_READY = 602,

    // Query Engine
    PLAN_FAILED_NETWORK_PARTITION = 721
};
}

class Field {
public:
    TSDataType::TSDataType dataType = TSDataType::UNKNOWN;
    boost::optional<bool> boolV;
    boost::optional<int> intV;
    boost::optional<boost::gregorian::date> dateV;
    boost::optional<int64_t> longV;
    boost::optional<float> floatV;
    boost::optional<double> doubleV;
    boost::optional<std::string> stringV;

    explicit Field(TSDataType::TSDataType a) {
        dataType = a;
    }

    Field() = default;
};

enum class ColumnCategory {
    TAG,
    FIELD,
    ATTRIBUTE
};

class MyStringBuffer {
public:
    MyStringBuffer();
    explicit MyStringBuffer(const std::string& str);

    void reserve(size_t n);
    void clear();
    bool hasRemaining();
    int getInt();
    boost::gregorian::date getDate();
    int64_t getInt64();
    float getFloat();
    double getDouble();
    char getChar();
    bool getBool();
    std::string getString();

    void putInt(int ins);
    void putDate(boost::gregorian::date date);
    void putInt64(int64_t ins);
    void putFloat(float ins);
    void putDouble(double ins);
    void putChar(char ins);
    void putBool(bool ins);
    void putString(const std::string& ins);
    void concat(const std::string& ins);

public:
    std::string str;
    size_t pos;

private:
    void checkBigEndian();
    const char* getOrderedByte(size_t len);
    void putOrderedByte(char* buf, int len);

private:
    bool isBigEndian{};
    char numericBuf[8]{}; //only be used by int, long, float, double etc.
};

class BitMap {
public:
    explicit BitMap(size_t size = 0);
    void resize(size_t size);
    bool mark(size_t position);
    bool unmark(size_t position);
    void markAll();
    void reset();
    bool isMarked(size_t position) const;
    bool isAllUnmarked() const;
    bool isAllMarked() const;
    const std::vector<char>& getByteArray() const;
    size_t getSize() const;

private:
    size_t size;
    std::vector<char> bits;
};

class IoTDBException : public std::exception {
public:
    IoTDBException() = default;

    explicit IoTDBException(const std::string& m) : message(m) {
    }

    explicit IoTDBException(const char* m) : message(m) {
    }

    virtual const char* what() const noexcept override {
        return message.c_str();
    }

private:
    std::string message;
};

class DateTimeParseException : public IoTDBException {
private:
    std::string parsedString;
    int errorIndex;

public:
    explicit DateTimeParseException(const std::string& message,
                                    std::string parsedData,
                                    int errorIndex)
        : IoTDBException(message),
          parsedString(std::move(parsedData)),
          errorIndex(errorIndex) {
    }

    explicit DateTimeParseException(const std::string& message,
                                    std::string parsedData,
                                    int errorIndex,
                                    const std::exception& cause)
        : IoTDBException(message + " [Caused by: " + cause.what() + "]"),
          parsedString(std::move(parsedData)),
          errorIndex(errorIndex) {
    }

    const std::string& getParsedString() const noexcept {
        return parsedString;
    }

    int getErrorIndex() const noexcept {
        return errorIndex;
    }

    const char* what() const noexcept override {
        static std::string fullMsg;
        fullMsg = std::string(IoTDBException::what()) +
            "\nParsed data: " + parsedString +
            "\nError index: " + std::to_string(errorIndex);
        return fullMsg.c_str();
    }
};

class IoTDBConnectionException : public IoTDBException {
public:
    IoTDBConnectionException() {
    }

    explicit IoTDBConnectionException(const char* m) : IoTDBException(m) {
    }

    explicit IoTDBConnectionException(const std::string& m) : IoTDBException(m) {
    }
};

class ExecutionException : public IoTDBException {
public:
    ExecutionException() {
    }

    explicit ExecutionException(const char* m) : IoTDBException(m) {
    }

    explicit ExecutionException(const std::string& m) : IoTDBException(m) {
    }

    explicit ExecutionException(const std::string& m, const TSStatus& tsStatus) : IoTDBException(m), status(tsStatus) {
    }

    TSStatus status;
};

class BatchExecutionException : public IoTDBException {
public:
    BatchExecutionException() {
    }

    explicit BatchExecutionException(const char* m) : IoTDBException(m) {
    }

    explicit BatchExecutionException(const std::string& m) : IoTDBException(m) {
    }

    explicit BatchExecutionException(const std::vector<TSStatus>& statusList) : statusList(statusList) {
    }

    BatchExecutionException(const std::string& m, const std::vector<TSStatus>& statusList) : IoTDBException(m),
        statusList(statusList) {
    }

    std::vector<TSStatus> statusList;
};

class RedirectException : public IoTDBException {
public:
    RedirectException() {
    }

    explicit RedirectException(const char* m) : IoTDBException(m) {
    }

    explicit RedirectException(const std::string& m) : IoTDBException(m) {
    }

    RedirectException(const std::string& m, const TEndPoint& endPoint) : IoTDBException(m), endPoint(endPoint) {
    }

    RedirectException(const std::string& m, const map<string, TEndPoint>& deviceEndPointMap) : IoTDBException(m),
        deviceEndPointMap(deviceEndPointMap) {
    }

    RedirectException(const std::string& m, const vector<TEndPoint>& endPointList) : IoTDBException(m),
        endPointList(endPointList) {
    }

    TEndPoint endPoint;
    map<string, TEndPoint> deviceEndPointMap;
    vector<TEndPoint> endPointList;
};

class UnSupportedDataTypeException : public IoTDBException {
public:
    UnSupportedDataTypeException() {
    }

    explicit UnSupportedDataTypeException(const char* m) : IoTDBException(m) {
    }

    explicit UnSupportedDataTypeException(const std::string& m) : IoTDBException("UnSupported dataType: " + m) {
    }
};

class SchemaNotFoundException : public IoTDBException {
public:
    SchemaNotFoundException() {
    }

    explicit SchemaNotFoundException(const char* m) : IoTDBException(m) {
    }

    explicit SchemaNotFoundException(const std::string& m) : IoTDBException(m) {
    }
};

class StatementExecutionException : public IoTDBException {
public:
    StatementExecutionException() {
    }

    explicit StatementExecutionException(const char* m) : IoTDBException(m) {
    }

    explicit StatementExecutionException(const std::string& m) : IoTDBException(m) {
    }
};

enum LogLevelType {
    LEVEL_DEBUG = 0,
    LEVEL_INFO,
    LEVEL_WARN,
    LEVEL_ERROR
};

extern LogLevelType LOG_LEVEL;

#define log_debug(fmt,...) do {if(LOG_LEVEL <= LEVEL_DEBUG) {string s=string("[DEBUG] %s:%d (%s) - ") + fmt + "\n"; printf(s.c_str(), __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__);}} while(0)
#define log_info(fmt,...)  do {if(LOG_LEVEL <= LEVEL_INFO)  {string s=string("[INFO]  %s:%d (%s) - ") + fmt + "\n"; printf(s.c_str(), __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__);}} while(0)
#define log_warn(fmt,...)  do {if(LOG_LEVEL <= LEVEL_WARN)  {string s=string("[WARN]  %s:%d (%s) - ") + fmt + "\n"; printf(s.c_str(), __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__);}} while(0)
#define log_error(fmt,...) do {if(LOG_LEVEL <= LEVEL_ERROR) {string s=string("[ERROR] %s:%d (%s) - ") + fmt + "\n"; printf(s.c_str(), __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__);}} while(0)

class RpcUtils {
public:
    std::shared_ptr<TSStatus> SUCCESS_STATUS;

    RpcUtils() {
        SUCCESS_STATUS = std::make_shared<TSStatus>();
        SUCCESS_STATUS->__set_code(TSStatusCode::SUCCESS_STATUS);
    }

    static void verifySuccess(const TSStatus& status);

    static void verifySuccessWithRedirection(const TSStatus& status);

    static void verifySuccessWithRedirectionForMultiDevices(const TSStatus& status, vector<string> devices);

    static void verifySuccess(const std::vector<TSStatus>& statuses);

    static TSStatus getStatus(TSStatusCode::TSStatusCode tsStatusCode);

    static TSStatus getStatus(int code, const std::string& message);

    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode);

    static std::shared_ptr<TSExecuteStatementResp>
    getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string& message);

    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(const TSStatus& status);

    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode);

    static std::shared_ptr<TSFetchResultsResp>
    getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string& appendMessage);

    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(const TSStatus& status);
};

class UrlUtils {
private:
    static const std::string PORT_SEPARATOR;
    static const std::string ABB_COLON;

    UrlUtils() = delete;
    ~UrlUtils() = delete;

public:
    /**
     * Parse TEndPoint from a given TEndPointUrl
     * example:[D80:0000:0000:0000:ABAA:0000:00C2:0002]:22227
     *
     * @param endPointUrl ip:port
     * @return TEndPoint with default values if parse error
     */
    static TEndPoint parseTEndPointIpv4AndIpv6Url(const std::string& endPointUrl);
};

#endif
