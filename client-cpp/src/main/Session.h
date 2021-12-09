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
#ifndef __IOTDB_SESSION
#define __IOTDB_SESSION

#include <string>
#include <vector>
#include <exception>
#include <iostream>
#include <algorithm>
#include <map>
#include <utility>
#include <memory>
#include <new>
#include <thread>
#include <mutex>
#include <stdexcept>
#include <cstdlib>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TBufferTransports.h>
#include "TSIService.h"

using ::apache::thrift::protocol::TBinaryProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransport;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::transport::TBufferedTransport;
using ::apache::thrift::transport::TFramedTransport;
using ::apache::thrift::TException;

class IoTDBConnectionException : public std::exception {
public:
    IoTDBConnectionException() : message() {}

    IoTDBConnectionException(const char *m) : message(m) {}

    IoTDBConnectionException(const std::string &m) : message(m) {}

    virtual const char *what() const throw() {
        return message.c_str();
    }

private:
    std::string message;
};

class BatchExecutionException : public std::exception {
public:
    BatchExecutionException() : message() {}

    BatchExecutionException(const char *m) : message(m) {}

    BatchExecutionException(const std::string &m) : message(m) {}

    BatchExecutionException(const std::vector <TSStatus> &statusList) : statusList(statusList) {}

    BatchExecutionException(const std::vector <TSStatus> &statusList, const std::string &m) : statusList(statusList),
                                                                                              message(m) {}

    virtual const char *what() const throw() {
        return message.c_str();
    }

    std::vector <TSStatus> statusList;
private:
    std::string message;

};

class UnSupportedDataTypeException : public std::exception {
private:
    std::string message;
public:
    UnSupportedDataTypeException() : message() {}

    UnSupportedDataTypeException(const char *m) : message(m) {}

    UnSupportedDataTypeException(const std::string &m) : message("UnSupported dataType: " + m) {}
};

namespace CompressionType {
    enum CompressionType {
        UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA, LZ4
    };
}
namespace TSDataType {
    enum TSDataType {
        BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NULLTYPE
    };
}
namespace TSEncoding {
    enum TSEncoding {
        PLAIN = 0,
        DICTIONARY = 1,
        RLE = 2,
        DIFF = 3,
        TS_2DIFF = 4,
        BITMAP = 5,
        GORILLA_V1 = 6,
        REGULAR = 7,
        GORILLA = 8
    };
}
namespace TSStatusCode {
    enum TSStatusCode {
        SUCCESS_STATUS = 200,
        STILL_EXECUTING_STATUS = 201,
        INVALID_HANDLE_STATUS = 202,

        NODE_DELETE_FAILED_ERROR = 298,
        ALIAS_ALREADY_EXIST_ERROR = 299,
        PATH_ALREADY_EXIST_ERROR = 300,
        PATH_NOT_EXIST_ERROR = 301,
        UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR = 302,
        METADATA_ERROR = 303,
        OUT_OF_TTL_ERROR = 305,
        CONFIG_ADJUSTER = 306,
        MERGE_ERROR = 307,
        SYSTEM_CHECK_ERROR = 308,
        SYNC_DEVICE_OWNER_CONFLICT_ERROR = 309,
        SYNC_CONNECTION_EXCEPTION = 310,
        STORAGE_GROUP_PROCESSOR_ERROR = 311,
        STORAGE_GROUP_ERROR = 312,
        STORAGE_ENGINE_ERROR = 313,
        TSFILE_PROCESSOR_ERROR = 314,
        PATH_ILLEGAL = 315,
        LOAD_FILE_ERROR = 316,
        STORAGE_GROUP_NOT_READY = 317,

        EXECUTE_STATEMENT_ERROR = 400,
        SQL_PARSE_ERROR = 401,
        GENERATE_TIME_ZONE_ERROR = 402,
        SET_TIME_ZONE_ERROR = 403,
        NOT_STORAGE_GROUP_ERROR = 404,
        QUERY_NOT_ALLOWED = 405,
        AST_FORMAT_ERROR = 406,
        LOGICAL_OPERATOR_ERROR = 407,
        LOGICAL_OPTIMIZE_ERROR = 408,
        UNSUPPORTED_FILL_TYPE_ERROR = 409,
        PATH_ERROR = 410,
        QUERY_PROCESS_ERROR = 411,
        WRITE_PROCESS_ERROR = 412,

        INTERNAL_SERVER_ERROR = 500,
        CLOSE_OPERATION_ERROR = 501,
        READ_ONLY_SYSTEM_ERROR = 502,
        DISK_SPACE_INSUFFICIENT_ERROR = 503,
        START_UP_ERROR = 504,
        SHUT_DOWN_ERROR = 505,
        MULTIPLE_ERROR = 506,
        WRONG_LOGIN_PASSWORD_ERROR = 600,
        NOT_LOGIN_ERROR = 601,
        NO_PERMISSION_ERROR = 602,
        UNINITIALIZED_AUTH_ERROR = 603,
        PARTITION_NOT_READY = 700,
        TIME_OUT = 701,
        NO_LEADER = 702,
        UNSUPPORTED_OPERATION = 703,
        NODE_READ_ONLY = 704,
        INCOMPATIBLE_VERSION = 203,
    };
}

class RpcUtils {
public:
    std::shared_ptr <TSStatus> SUCCESS_STATUS;

    RpcUtils() {
        SUCCESS_STATUS = std::make_shared<TSStatus>();
        SUCCESS_STATUS->__set_code(TSStatusCode::SUCCESS_STATUS);
    }

    static void verifySuccess(const TSStatus &status);

    static void verifySuccess(const std::vector<TSStatus> &statuses);

    static TSStatus getStatus(TSStatusCode::TSStatusCode tsStatusCode);

    static TSStatus getStatus(int code, const std::string &message);

    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode);

    static std::shared_ptr<TSExecuteStatementResp>
    getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string &message);

    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(const TSStatus &status);

    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode);

    static std::shared_ptr<TSFetchResultsResp>
    getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string &appendMessage);

    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(const TSStatus &status);
};

// Simulate the ByteBuffer class in Java
class MyStringBuffer {
public:
    MyStringBuffer():pos(0) {
        checkBigEndian();
    }

    MyStringBuffer(std::string str):str(str),pos(0) {
        checkBigEndian();
    }

    bool hasRemaining() {
        return pos < str.size();
    }
    
    int getInt() {
        return *(int *) getOrderedByte(4);
    }

    int64_t getLong() {
        return *(int64_t *) getOrderedByte(8);
    }

    float getFloat() {
        return *(float *) getOrderedByte(4);
    }

    double getDouble() {
        return *(double *) getOrderedByte(8);
    }

    char getChar() {
        return str[pos++];
    }

    bool getBool() {
        return getChar() == 1;
    }

    std::string getString() {
        size_t len = getInt();
        size_t tmpPos = pos;
        pos += len;
        return str.substr(tmpPos, len);
    }

    void putInt(int ins) {
        putOrderedByte((char *) &ins, 4);
    }

    void putLong(int64_t ins) {
        putOrderedByte((char *) &ins, 8);
    }

    void putFloat(float ins) {
        putOrderedByte((char *) &ins, 4);
    }

    void putDouble(double ins) {
        putOrderedByte((char *) &ins, 8);
    }

    void putChar(char ins) {
        str += ins;
    }

    void putBool(bool ins) {
        char tmp = ins? 1 : 0;
        str += tmp;
    }

    void putString(std::string ins) {
        putInt(ins.size());
        str += ins;
    }

public:
    std::string str;
    size_t pos;

private:
    void checkBigEndian() {
        static int chk = 0x0201;  //used to distinguish CPU's type (BigEndian or LittleEndian)
        isBigEndian = (0x01 != *(char *) (&chk));
    }

    const char *getOrderedByte(size_t len) {
        const char *p = NULL;
        if (isBigEndian) {
            p = str.c_str() + pos;
        } else {
            const char *tmp = str.c_str();
            for (size_t i = pos; i < pos + len; i++) {
                numericBuf[pos + len - 1 - i] = tmp[i];
            }
            p = numericBuf;
        }
        pos += len;
        return p;
    }

    void putOrderedByte(char *buf, int len) {
        if (isBigEndian) {
            str.assign(buf, len);
        }
        else {
            for (int i = len - 1; i > -1; i--) {
                str += buf[i];
            }
        }
    }

private:
    bool isBigEndian;
    char numericBuf[8];  //only be used by int, long, float, double etc.
};


class Field {
public:
    TSDataType::TSDataType dataType;
    bool boolV;
    int intV;
    int64_t longV;
    float floatV;
    double doubleV;
    std::string stringV;

    Field(TSDataType::TSDataType a) {
        dataType = a;
    }

    Field() {}
};

/*
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 *
 * for example:  device root.sg1.d1
 *
 * time, m1, m2, m3
 *    1,  1,  2,  3
 *    2,  1,  2,  3
 *    3,  1,  2,  3
 *
 * Notice: The tablet should not have empty cell
 *
 */
class Tablet {
private:
    static const int DEFAULT_SIZE = 1024;
public:
    std::string deviceId; // deviceId of this tablet
    std::vector <std::pair<std::string, TSDataType::TSDataType>> schemas; // the list of measurement schemas for creating the tablet
    std::vector <int64_t> timestamps;   //timestamps in this tablet
    std::vector <std::vector<std::string>> values;
    int rowSize;    //the number of rows to include in this tablet
    int maxRowNumber;   // the maximum number of rows for this tablet

    Tablet() {}

    /**
   * Return a tablet with default specified row number. This is the standard
   * constructor (all Tablet should be the same size).
   *
   * @param deviceId   the name of the device specified to be written in
   * @param timeseries the list of measurement schemas for creating the tablet
   */
    Tablet(const std::string &deviceId, const std::vector <std::pair<std::string, TSDataType::TSDataType>> &timeseries) {
        Tablet(deviceId, timeseries, DEFAULT_SIZE);
    }

    /**
     * Return a tablet with the specified number of rows (maxBatchSize). Only
     * call this constructor directly for testing purposes. Tablet should normally
     * always be default size.
     *
     * @param deviceId     the name of the device specified to be written in
     * @param schemas   the list of measurement schemas for creating the row
     *                     batch
     * @param maxRowNumber the maximum number of rows for this tablet
     */
    Tablet(const std::string &deviceId, const std::vector <std::pair<std::string, TSDataType::TSDataType>> &schemas,
           int maxRowNumber) : deviceId(deviceId), schemas(schemas), maxRowNumber(maxRowNumber){
        // create timestamp column
        timestamps.resize(maxRowNumber);
        // create value columns
        values.resize(schemas.size());
        for (size_t i = 0; i < schemas.size(); i++) {
            values[i].resize(maxRowNumber);
        }

        this->rowSize = 0;
    }

    void reset(); // Reset Tablet to the default state - set the rowSize to 0
    void createColumns();

    int getTimeBytesSize();

    int getValueByteSize(); // total byte size that values occupies
};

class SessionUtils {
public:
    static std::string getTime(const Tablet &tablet);

    static std::string getValue(const Tablet &tablet);
};

class RowRecord {
public:
    int64_t timestamp;
    std::vector <Field> fields;

    RowRecord(int64_t timestamp) {
        this->timestamp = timestamp;
    }

    RowRecord(int64_t timestamp, const std::vector <Field> &fields)
            : timestamp(timestamp), fields(fields) {
    }

    RowRecord() {
        this->timestamp = -1;
    }

    void addField(const Field &f) {
        this->fields.push_back(f);
    }

    std::string toString() {
        std::string ret = std::to_string(timestamp);
        for (size_t i = 0; i < fields.size(); i++) {
            ret.append("\t");
            TSDataType::TSDataType dataType = fields[i].dataType;
            switch (dataType) {
                case TSDataType::BOOLEAN: {
                    std::string field = fields[i].boolV ? "true" : "false";
                    ret.append(field);
                    break;
                }
                case TSDataType::INT32: {
                    ret.append(std::to_string(fields[i].intV));
                    break;
                }
                case TSDataType::INT64: {
                    ret.append(std::to_string(fields[i].longV));
                    break;
                }
                case TSDataType::FLOAT: {
                    ret.append(std::to_string(fields[i].floatV));
                    break;
                }
                case TSDataType::DOUBLE: {
                    ret.append(std::to_string(fields[i].doubleV));
                    break;
                }
                case TSDataType::TEXT: {
                    ret.append(fields[i].stringV);
                    break;
                }
                case TSDataType::NULLTYPE: {
                    ret.append("NULL");
                }
            }
        }
        ret.append("\n");
        return ret;
    }
};

class SessionDataSet {
private:
    bool hasCachedRecord = false;
    std::string sql;
    int64_t queryId;
    int64_t statementId;
    int64_t sessionId;
    std::shared_ptr <TSIServiceIf> client;
    int batchSize = 1024;
    std::vector <std::string> columnNameList;
    std::vector <std::string> columnTypeDeduplicatedList;
    // duplicated column index -> origin index
    std::map<int, int> duplicateLocation;
    // column name -> column location
    std::map<std::string, int> columnMap;
    // column size
    int columnSize = 0;

    int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
    std::shared_ptr <TSQueryDataSet> tsQueryDataSet;
    MyStringBuffer tsQueryDataSetTimeBuffer;
    std::vector <std::unique_ptr<MyStringBuffer>> valueBuffers;
    std::vector <std::unique_ptr<MyStringBuffer>> bitmapBuffers;
    RowRecord rowRecord;
    char *currentBitmap = NULL; // used to cache the current bitmap for every column
    static const int flag = 0x80; // used to do `or` operation with bitmap to judge whether the value is null

public:
    SessionDataSet() {}

    SessionDataSet(const std::string &sql, const std::vector <std::string> &columnNameList,
                   const std::vector <std::string> &columnTypeList, int64_t queryId, int64_t statementId,
                   std::shared_ptr <TSIServiceIf> client, int64_t sessionId,
                   std::shared_ptr <TSQueryDataSet> queryDataSet) : tsQueryDataSetTimeBuffer(queryDataSet->time) {
        this->sessionId = sessionId;
        this->sql = sql;
        this->queryId = queryId;
        this->statementId = statementId;
        this->client = client;
        this->columnNameList = columnNameList;
        this->currentBitmap = new char[columnNameList.size()];
        this->columnSize = columnNameList.size();

        // column name -> column location
        for (size_t i = 0; i < columnNameList.size(); i++) {
            std::string name = columnNameList[i];
            if (this->columnMap.find(name) != this->columnMap.end()) {
                duplicateLocation[i] = columnMap[name];
            } else {
                this->columnMap[name] = i;
                this->columnTypeDeduplicatedList.push_back(columnTypeList[i]);
            }
            this->valueBuffers.push_back(
                    std::unique_ptr<MyStringBuffer>(new MyStringBuffer(queryDataSet->valueList[i])));
            this->bitmapBuffers.push_back(
                    std::unique_ptr<MyStringBuffer>(new MyStringBuffer(queryDataSet->bitmapList[i])));
        }
        this->tsQueryDataSet = queryDataSet;
    }

    ~SessionDataSet() {
        if (currentBitmap != NULL) {
            delete[] currentBitmap;
            currentBitmap = NULL;
        }
    }

    int getBatchSize();

    void setBatchSize(int batchSize);

    std::vector <std::string> getColumnNames();

    bool hasNext();

    void constructOneRow();

    bool isNull(int index, int rowNum);

    RowRecord *next();

    void closeOperationHandle();
};

template<typename T>
std::vector <T> sortList(const std::vector <T> &valueList, const int *index, int indexLength) {
    std::vector <T> sortedValues(valueList.size());
    for (int i = 0; i < indexLength; i++) {
        sortedValues[i] = valueList[index[i]];
    }
    return sortedValues;
}

class Session {
private:
    std::string host;
    int rpcPort;
    std::string username;
    std::string password;
    const TSProtocolVersion::type protocolVersion = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3;
    std::shared_ptr <TSIServiceIf> client;
    std::shared_ptr <TTransport> transport;
    bool isClosed = true;
    int64_t sessionId;
    int64_t statementId;
    std::string zoneId;
    int fetchSize;
    const static int DEFAULT_FETCH_SIZE = 10000;
    const static int DEFAULT_TIMEOUT_MS = 0;

    bool checkSorted(const Tablet &tablet);

    bool checkSorted(const std::vector <int64_t> &times);

    void sortTablet(Tablet &tablet);

    void sortIndexByTimestamp(int *index, std::vector <int64_t> &timestamps, int length);

    std::string getTimeZone();

    void setTimeZone(const std::string &zoneId);

    void appendValues(std::string &buffer, const char *value, int size);

    void
    putValuesIntoBuffer(const std::vector <TSDataType::TSDataType> &types, const std::vector<char *> &values, std::string &buf);

    int8_t getDataTypeNumber(TSDataType::TSDataType type);

    struct TsCompare {
        std::vector <int64_t> &timestamps;
        TsCompare(std::vector <int64_t> &inTimestamps):timestamps(inTimestamps) {};
        bool operator() (int i, int j) { return (timestamps[i] < timestamps[j]) ;};
    };

public:
    Session(const std::string &host, int rpcPort) : username("user"), password("password") {
        this->host = host;
        this->rpcPort = rpcPort;
    }

    Session(const std::string &host, int rpcPort, const std::string &username, const std::string &password)
            : fetchSize(10000) {
        this->host = host;
        this->rpcPort = rpcPort;
        this->username = username;
        this->password = password;
        this->zoneId = "UTC+08:00";
    }

    Session(const std::string &host, int rpcPort, const std::string &username, const std::string &password, int fetchSize) {
        this->host = host;
        this->rpcPort = rpcPort;
        this->username = username;
        this->password = password;
        this->fetchSize = fetchSize;
        this->zoneId = "UTC+08:00";
    }

    Session(const std::string &host, const std::string &rpcPort, const std::string &username = "user",
            const std::string &password = "password", int fetchSize = 10000) {
        this->host = host;
        this->rpcPort = stoi(rpcPort);
        this->username = username;
        this->password = password;
        this->fetchSize = fetchSize;
        this->zoneId = "UTC+08:00";
    }

    ~Session();

    void open();

    void open(bool enableRPCCompression);

    void open(bool enableRPCCompression, int connectionTimeoutInMs);

    void close();

    void insertRecord(const std::string &deviceId, int64_t time, const std::vector <std::string> &measurements,
                      const std::vector <std::string> &values);

    void insertRecord(const std::string &deviceId, int64_t time, const std::vector <std::string> &measurements,
                      const std::vector <TSDataType::TSDataType> &types, const std::vector<char *> &values);

    void insertRecords(const std::vector <std::string> &deviceIds,
                       const std::vector <int64_t> &times,
                       const std::vector <std::vector<std::string>> &measurementsList,
                       const std::vector <std::vector<std::string>> &valuesList);

    void insertRecords(const std::vector <std::string> &deviceIds,
                       const std::vector <int64_t> &times,
                       const std::vector <std::vector<std::string>> &measurementsList,
                       const std::vector <std::vector<TSDataType::TSDataType>> &typesList,
                       const std::vector <std::vector<char *>> &valuesList);

    void insertRecordsOfOneDevice(const std::string &deviceId,
                                  std::vector <int64_t> &times,
                                  std::vector <std::vector<std::string>> &measurementsList,
                                  std::vector <std::vector<TSDataType::TSDataType>> &typesList,
                                  std::vector <std::vector<char *>> &valuesList);

    void insertRecordsOfOneDevice(const std::string &deviceId,
                                  std::vector <int64_t> &times,
                                  std::vector <std::vector<std::string>> &measurementsList,
                                  std::vector <std::vector<TSDataType::TSDataType>> &typesList,
                                  std::vector <std::vector<char *>> &valuesList,
                                  bool sorted);

    void insertTablet(Tablet &tablet);

    void insertTablet(Tablet &tablet, bool sorted);

    void insertTablets(std::map<std::string, Tablet *> &tablets);

    void insertTablets(std::map<std::string, Tablet *> &tablets, bool sorted);

    void testInsertRecord(const std::string &deviceId, int64_t time,
                          const std::vector <std::string> &measurements,
                          const std::vector <std::string> &values);

    void testInsertTablet(const Tablet &tablet);

    void testInsertRecords(const std::vector <std::string> &deviceIds,
                           const std::vector <int64_t> &times,
                           const std::vector <std::vector<std::string>> &measurementsList,
                           const std::vector <std::vector<std::string>> &valuesList);

    void deleteTimeseries(const std::string &path);

    void deleteTimeseries(const std::vector <std::string> &paths);

    void deleteData(const std::string &path, int64_t time);

    void deleteData(const std::vector <std::string> &deviceId, int64_t time);

    void setStorageGroup(const std::string &storageGroupId);

    void deleteStorageGroup(const std::string &storageGroup);

    void deleteStorageGroups(const std::vector <std::string> &storageGroups);

    void createTimeseries(const std::string &path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor);

    void createTimeseries(const std::string &path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor,
                          std::map <std::string, std::string> *props, std::map <std::string, std::string> *tags,
                          std::map <std::string, std::string> *attributes,
                          const std::string &measurementAlias);

    void createMultiTimeseries(const std::vector <std::string> &paths,
                               const std::vector <TSDataType::TSDataType> &dataTypes,
                               const std::vector <TSEncoding::TSEncoding> &encodings,
                               const std::vector <CompressionType::CompressionType> &compressors,
                               std::vector <std::map<std::string, std::string>> *propsList,
                               std::vector <std::map<std::string, std::string>> *tagsList,
                               std::vector <std::map<std::string, std::string>> *attributesList,
                               std::vector <std::string> *measurementAliasList);

    bool checkTimeseriesExists(const std::string &path);

    std::unique_ptr <SessionDataSet> executeQueryStatement(const std::string &sql);

    void executeNonQueryStatement(const std::string &sql);
};

#endif // __IOTDB_SESSION
