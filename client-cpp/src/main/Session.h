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
#ifndef IOTDB_SESSION_H
#define IOTDB_SESSION_H

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <exception>
#include <iostream>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <stack>
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

    explicit IoTDBConnectionException(const char *m) : message(m) {}

    explicit IoTDBConnectionException(std::string m) : message(std::move(m)) {}

    const char *what() const noexcept override {
        return message.c_str();
    }

private:
    std::string message;
};

class BatchExecutionException : public std::exception {
public:
    BatchExecutionException() : message() {}

    explicit BatchExecutionException(const char *m) : message(m) {}

    BatchExecutionException(std::string m) : message(std::move(m)) {}

    BatchExecutionException(std::vector<TSStatus> statusList) : statusList(std::move(statusList)) {}

    BatchExecutionException(std::vector<TSStatus> statusList, std::string m) : statusList(std::move(statusList)),
                                                                               message(std::move(m)) {}

    const char *what() const noexcept override {
        return message.c_str();
    }

    std::vector<TSStatus> statusList;
private:
    std::string message;

};

class UnSupportedDataTypeException : public std::exception {
private:
    std::string message;
public:
    UnSupportedDataTypeException() : message() {}

    UnSupportedDataTypeException(const char *m) : message(m) {}

    explicit UnSupportedDataTypeException(const std::string &m) : message("UnSupported dataType: " + m) {}
};

namespace Version {
    enum Version {
        V_0_12, V_0_13
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
        LZ4 = (char) 7
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
        NULLTYPE = (char) 7
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
        GORILLA = (char) 8
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
    std::shared_ptr<TSStatus> SUCCESS_STATUS;

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
    MyStringBuffer() : pos(0) {
        checkBigEndian();
    }

    explicit MyStringBuffer(std::string str) : str(std::move(str)), pos(0) {
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
        char tmp = ins ? 1 : 0;
        str += tmp;
    }

    void putString(const std::string &ins) {
        putInt((int)(ins.size()));
        str += ins;
    }

    void concat(const std::string &ins) {
        str.append(ins);
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
        const char *p = nullptr;
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
        } else {
            for (int i = len - 1; i > -1; i--) {
                str += buf[i];
            }
        }
    }

private:
    bool isBigEndian{};
    char numericBuf[8]{};  //only be used by int, long, float, double etc.
};

class BitMap {
public:
    /** Initialize a BitMap with given size. */
    explicit BitMap(size_t size) {
        this->size = size;
        this->bits.resize((size >> 3) + 1); // equal to "size/8 + 1"
        std::fill(bits.begin(), bits.end(), (char) 0);
    }

    /** mark as 1 at the given bit position. */
    void mark(int position) {
        bits[position >> 3] |= BIT_UTIL[position % 8];
    }

    /** mark as 0 at the given bit position. */
    void unmark(int position) {
        bits[position >> 3] &= UNMARK_BIT_UTIL[position % 8];
    }

    /** mark as 1 at all positions. */
    void markAll() {
        std::fill(bits.begin(), bits.end(), (char) 0XFF);
    }

    /** mark as 0 at all positions. */
    void reset() {
        std::fill(bits.begin(), bits.end(), (char) 0);
    }

    /** returns the value of the bit with the specified index. */
    bool isMarked(int position) {
        return (bits[position >> 3] & BIT_UTIL[position % 8]) != 0;
    }

    /** whether all bits are zero, i.e., no Null value */
    bool isAllUnmarked() {
        int j;
        for (j = 0; j < size >> 3; j++) {
            if (bits[j] != (char) 0) {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++) {
            if ((bits[size >> 3] & BIT_UTIL[j]) != 0) {
                return false;
            }
        }
        return true;
    }

    /** whether all bits are one, i.e., all are Null */
    bool isAllMarked() {
        int j;
        for (j = 0; j < size >> 3; j++) {
            if (bits[j] != (char) 0XFF) {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++) {
            if ((bits[size >> 3] & BIT_UTIL[j]) == 0) {
                return false;
            }
        }
        return true;
    }

    std::vector<char> getByteArray() {
        return this->bits;
    }

    size_t getSize() {
        return this->size;
    }

private:
    std::vector<char> BIT_UTIL = {
            (char) 1, (char) 2, (char) 4, (char) 8, (char) 16, (char) 32, (char) 64, (char) -128
    };
    std::vector<char> UNMARK_BIT_UTIL = {
            (char) 0XFE, // 11111110
            (char) 0XFD, // 11111101
            (char) 0XFB, // 11111011
            (char) 0XF7, // 11110111
            (char) 0XEF, // 11101111
            (char) 0XDF, // 11011111
            (char) 0XBF, // 10111111
            (char) 0X7F // 01111111
    };

    std::vector<char> bits;
    size_t size;
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

    explicit Field(TSDataType::TSDataType a) {
        dataType = a;
    }

    Field() = default;
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
    void createColumns();
    void deleteColumns();
public:
    std::string deviceId; // deviceId of this tablet
    std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas; // the list of measurement schemas for creating the tablet
    std::vector<int64_t> timestamps;   // timestamps in this tablet
    std::vector<void*> values; // each object is a primitive type array, which represents values of one measurement
    std::vector<std::unique_ptr<BitMap>> bitMaps; // each bitmap represents the existence of each value in the current column
    int rowSize;    //the number of rows to include in this tablet
    int maxRowNumber;   // the maximum number of rows for this tablet
    bool isAligned;   // whether this tablet store data of aligned timeseries or not

    Tablet() = default;

    /**
   * Return a tablet with default specified row number. This is the standard
   * constructor (all Tablet should be the same size).
   *
   * @param deviceId   the name of the device specified to be written in
   * @param timeseries the list of measurement schemas for creating the tablet
   */
    Tablet(const std::string &deviceId,
           const std::vector<std::pair<std::string, TSDataType::TSDataType>> &timeseries) {
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
    Tablet(const std::string &deviceId, const std::vector<std::pair<std::string, TSDataType::TSDataType>> &schemas,
           int maxRowNumber, bool _isAligned = false) : deviceId(deviceId), schemas(schemas),
                                                        maxRowNumber(maxRowNumber), isAligned(_isAligned) {
        // create timestamp column
        timestamps.resize(maxRowNumber);
        // create value columns
        values.resize(schemas.size());
        createColumns();
        // create bitMaps
        bitMaps.resize(schemas.size());
        for (size_t i = 0; i < schemas.size(); i++) {
            bitMaps[i] = std::unique_ptr<BitMap>(new BitMap(maxRowNumber));
        }
        this->rowSize = 0;
    }

    ~Tablet() {
        deleteColumns();
    }

    void addValue(int schemaId, int rowIndex, void *value);

    void reset(); // Reset Tablet to the default state - set the rowSize to 0

    int getTimeBytesSize();

    int getValueByteSize(); // total byte size that values occupies

    void setAligned(bool isAligned);
};

class SessionUtils {
public:
    static std::string getTime(const Tablet &tablet);

    static std::string getValue(const Tablet &tablet);
};

class RowRecord {
public:
    int64_t timestamp;
    std::vector<Field> fields;

    RowRecord(int64_t timestamp) {
        this->timestamp = timestamp;
    }

    RowRecord(int64_t timestamp, const std::vector<Field> &fields)
            : timestamp(timestamp), fields(fields) {
    }

    explicit RowRecord(const std::vector<Field> &fields)
            : timestamp(-1), fields(fields) {
    }

    RowRecord() {
        this->timestamp = -1;
    }

    void addField(const Field &f) {
        this->fields.push_back(f);
    }

    std::string toString() {
        std::string ret;
        if (this->timestamp != -1) {
            ret.append(std::to_string(timestamp));
            ret.append("\t");
        }
        for (size_t i = 0; i < fields.size(); i++) {
            if (i != 0) {
                ret.append("\t");
            }
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
    std::shared_ptr<TSIServiceIf> client;
    int batchSize = 1024;
    std::vector<std::string> columnNameList;
    std::vector<std::string> columnTypeDeduplicatedList;
    // duplicated column index -> origin index
    std::unordered_map<int, int> duplicateLocation;
    // column name -> column location
    std::unordered_map<std::string, int> columnMap;
    // column size
    int columnSize = 0;
    bool isIgnoreTimeStamp = false;

    int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
    std::shared_ptr<TSQueryDataSet> tsQueryDataSet;
    MyStringBuffer tsQueryDataSetTimeBuffer;
    std::vector<std::unique_ptr<MyStringBuffer>> valueBuffers;
    std::vector<std::unique_ptr<MyStringBuffer>> bitmapBuffers;
    RowRecord rowRecord;
    char *currentBitmap = nullptr; // used to cache the current bitmap for every column
    static const int flag = 0x80; // used to do `or` operation with bitmap to judge whether the value is null

public:
    SessionDataSet() {}

    SessionDataSet(const std::string &sql,
                   const std::vector<std::string> &columnNameList,
                   const std::vector<std::string> &columnTypeList,
                   std::map<std::string, int> &columnNameIndexMap,
                   bool isIgnoreTimeStamp,
                   int64_t queryId, int64_t statementId,
                   std::shared_ptr<TSIServiceIf> client, int64_t sessionId,
                   const std::shared_ptr<TSQueryDataSet> &queryDataSet) : tsQueryDataSetTimeBuffer(queryDataSet->time) {
        this->sessionId = sessionId;
        this->sql = sql;
        this->queryId = queryId;
        this->statementId = statementId;
        this->client = client;
        this->columnNameList = columnNameList;
        this->currentBitmap = new char[columnNameList.size()];
        this->columnSize = (int)columnNameList.size();
        this->isIgnoreTimeStamp = isIgnoreTimeStamp;

        // column name -> column location
        for (size_t i = 0; i < columnNameList.size(); i++) {
            std::string name = columnNameList[i];
            if (this->columnMap.find(name) != this->columnMap.end()) {
                duplicateLocation[i] = columnMap[name];
            } else {
                this->columnMap[name] = (int)i;
                this->columnTypeDeduplicatedList.push_back(columnTypeList[i]);
            }
            if (!columnNameIndexMap.empty()) {
                this->valueBuffers.push_back(
                        std::unique_ptr<MyStringBuffer>(
                                new MyStringBuffer(queryDataSet->valueList[columnNameIndexMap[name]])));
                this->bitmapBuffers.push_back(
                        std::unique_ptr<MyStringBuffer>(
                                new MyStringBuffer(queryDataSet->bitmapList[columnNameIndexMap[name]])));
            } else {
                this->valueBuffers.push_back(
                        std::unique_ptr<MyStringBuffer>(new MyStringBuffer(queryDataSet->valueList[columnMap[name]])));
                this->bitmapBuffers.push_back(
                        std::unique_ptr<MyStringBuffer>(new MyStringBuffer(queryDataSet->bitmapList[columnMap[name]])));
            }
        }
        this->tsQueryDataSet = queryDataSet;
    }

    ~SessionDataSet() {
        if (currentBitmap != nullptr) {
            delete[] currentBitmap;
            currentBitmap = nullptr;
        }
    }

    int getBatchSize();

    void setBatchSize(int batchSize);

    std::vector<std::string> getColumnNames();

    bool hasNext();

    void constructOneRow();

    bool isNull(int index, int rowNum);

    RowRecord *next();

    void closeOperationHandle();
};

class TemplateNode {
public:

    TemplateNode() = default;

    explicit TemplateNode(std::string name_) {
        this->name_ = std::move(name_);
    }

    const std::string &getName() const {
        return name_;
    }

    virtual const std::unordered_map<std::string, std::shared_ptr<TemplateNode>> &getChildren() const {
        throw BatchExecutionException("Should call exact sub class!");
    }

    virtual bool isMeasurement() = 0;

    virtual bool isAligned() {
        throw BatchExecutionException("Should call exact sub class!");
    }

    virtual std::string serialize() const {
        throw BatchExecutionException("Should call exact sub class!");
    }

private:
    std::string name_;
};

class MeasurementNode : public TemplateNode {
public:

    MeasurementNode(std::string name_, TSDataType::TSDataType data_type_, TSEncoding::TSEncoding encoding_,
                    CompressionType::CompressionType compression_type_) : TemplateNode(std::move(name_)) {
        this->data_type_ = data_type_;
        this->encoding_ = encoding_;
        this->compression_type_ = compression_type_;
    }

    TSDataType::TSDataType getDataType() const {
        return data_type_;
    }

    TSEncoding::TSEncoding getEncoding() const {
        return encoding_;
    }

    CompressionType::CompressionType getCompressionType() const {
        return compression_type_;
    }

    bool isMeasurement() override {
        return true;
    }

    std::string serialize() const override;

private:
    TSDataType::TSDataType data_type_;
    TSEncoding::TSEncoding encoding_;
    CompressionType::CompressionType compression_type_;
};

class InternalNode : public TemplateNode {
public:

    InternalNode(std::string name_, bool is_aligned_) : TemplateNode(std::move(name_)) {
        this->is_aligned_ = is_aligned_;
    }

    void addChild(const InternalNode &node) {
        if (this->children_.count(node.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[node.getName()] = std::make_shared<InternalNode>(node);
    }

    void addChild(const MeasurementNode &node) {
        if (this->children_.count(node.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[node.getName()] = std::make_shared<MeasurementNode>(node);
    }

    void deleteChild(const TemplateNode &node) {
        this->children_.erase(node.getName());
    }

    const std::unordered_map<std::string, std::shared_ptr<TemplateNode>> &getChildren() const override {
        return children_;
    }

    bool isMeasurement() override {
        return false;
    }

    bool isAligned() override {
        return is_aligned_;
    }

private:
    std::unordered_map<std::string, std::shared_ptr<TemplateNode>> children_;
    bool is_aligned_;
};

namespace TemplateQueryType {
    enum TemplateQueryType {
        COUNT_MEASUREMENTS, IS_MEASUREMENT, PATH_EXIST, SHOW_MEASUREMENTS
    };
}

class Template {
public:

    Template(std::string name_, bool is_aligned_) {
        this->name_ = std::move(name_);
        this->is_aligned_ = is_aligned_;
    }

    const std::string &getName() const {
        return name_;
    }

    bool isAligned() const {
        return is_aligned_;
    }

    void addToTemplate(const InternalNode &child) {
        if (this->children_.count(child.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[child.getName()] = std::make_shared<InternalNode>(child);
    }

    void addToTemplate(const MeasurementNode &child) {
        if (this->children_.count(child.getName())) {
            throw BatchExecutionException("Duplicated child of node in template.");
        }
        this->children_[child.getName()] = std::make_shared<MeasurementNode>(child);
    }

    std::string serialize() const;

private:
    std::string name_;
    std::unordered_map<std::string, std::shared_ptr<TemplateNode>> children_;
    bool is_aligned_;
};

class Session {
private:
    std::string host;
    int rpcPort;
    std::string username;
    std::string password;
    const TSProtocolVersion::type protocolVersion = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3;
    std::shared_ptr<TSIServiceIf> client;
    std::shared_ptr<TTransport> transport;
    bool isClosed = true;
    int64_t sessionId;
    int64_t statementId;
    std::string zoneId;
    int fetchSize;
    const static int DEFAULT_FETCH_SIZE = 10000;
    const static int DEFAULT_TIMEOUT_MS = 0;
    Version::Version version;

    bool checkSorted(const Tablet &tablet);

    bool checkSorted(const std::vector<int64_t> &times);

    void sortTablet(Tablet &tablet);

    void sortIndexByTimestamp(int *index, std::vector<int64_t> &timestamps, int length);

    std::string getTimeZone();

    void setTimeZone(const std::string &zoneId);

    void appendValues(std::string &buffer, const char *value, int size);

    void
    putValuesIntoBuffer(const std::vector<TSDataType::TSDataType> &types, const std::vector<char *> &values,
                        std::string &buf);

    int8_t getDataTypeNumber(TSDataType::TSDataType type);

    struct TsCompare {
        std::vector<int64_t> &timestamps;

        explicit TsCompare(std::vector<int64_t> &inTimestamps) : timestamps(inTimestamps) {};

        bool operator()(int i, int j) { return (timestamps[i] < timestamps[j]); };
    };

    std::string getVersionString(Version::Version version);

public:
    Session(const std::string &host, int rpcPort) : username("user"), password("password"), version(Version::V_0_13) {
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
        this->version = Version::V_0_13;
    }

    Session(const std::string &host, int rpcPort, const std::string &username, const std::string &password,
            int fetchSize) {
        this->host = host;
        this->rpcPort = rpcPort;
        this->username = username;
        this->password = password;
        this->fetchSize = fetchSize;
        this->zoneId = "UTC+08:00";
        this->version = Version::V_0_13;
    }

    Session(const std::string &host, const std::string &rpcPort, const std::string &username = "user",
            const std::string &password = "password", int fetchSize = 10000) {
        this->host = host;
        this->rpcPort = stoi(rpcPort);
        this->username = username;
        this->password = password;
        this->fetchSize = fetchSize;
        this->zoneId = "UTC+08:00";
        this->version = Version::V_0_13;
    }

    ~Session();

    void open();

    void open(bool enableRPCCompression);

    void open(bool enableRPCCompression, int connectionTimeoutInMs);

    void close();

    void insertRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                      const std::vector<std::string> &values);

    void insertRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                      const std::vector<TSDataType::TSDataType> &types, const std::vector<char *> &values);

    void insertAlignedRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                             const std::vector<std::string> &values);

    void insertAlignedRecord(const std::string &deviceId, int64_t time, const std::vector<std::string> &measurements,
                             const std::vector<TSDataType::TSDataType> &types, const std::vector<char *> &values);

    void insertRecords(const std::vector<std::string> &deviceIds,
                       const std::vector<int64_t> &times,
                       const std::vector<std::vector<std::string>> &measurementsList,
                       const std::vector<std::vector<std::string>> &valuesList);

    void insertRecords(const std::vector<std::string> &deviceIds,
                       const std::vector<int64_t> &times,
                       const std::vector<std::vector<std::string>> &measurementsList,
                       const std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                       const std::vector<std::vector<char *>> &valuesList);

    void insertAlignedRecords(const std::vector<std::string> &deviceIds,
                              const std::vector<int64_t> &times,
                              const std::vector<std::vector<std::string>> &measurementsList,
                              const std::vector<std::vector<std::string>> &valuesList);

    void insertAlignedRecords(const std::vector<std::string> &deviceIds,
                              const std::vector<int64_t> &times,
                              const std::vector<std::vector<std::string>> &measurementsList,
                              const std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                              const std::vector<std::vector<char *>> &valuesList);

    void insertRecordsOfOneDevice(const std::string &deviceId,
                                  std::vector<int64_t> &times,
                                  std::vector<std::vector<std::string>> &measurementsList,
                                  std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                                  std::vector<std::vector<char *>> &valuesList);

    void insertRecordsOfOneDevice(const std::string &deviceId,
                                  std::vector<int64_t> &times,
                                  std::vector<std::vector<std::string>> &measurementsList,
                                  std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                                  std::vector<std::vector<char *>> &valuesList,
                                  bool sorted);

    void insertAlignedRecordsOfOneDevice(const std::string &deviceId,
                                         std::vector<int64_t> &times,
                                         std::vector<std::vector<std::string>> &measurementsList,
                                         std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                                         std::vector<std::vector<char *>> &valuesList);

    void insertAlignedRecordsOfOneDevice(const std::string &deviceId,
                                         std::vector<int64_t> &times,
                                         std::vector<std::vector<std::string>> &measurementsList,
                                         std::vector<std::vector<TSDataType::TSDataType>> &typesList,
                                         std::vector<std::vector<char *>> &valuesList,
                                         bool sorted);

    void insertTablet(Tablet &tablet);

    void insertTablet(Tablet &tablet, bool sorted);

    void insertAlignedTablet(Tablet &tablet);

    void insertAlignedTablet(Tablet &tablet, bool sorted);

    void insertTablets(std::unordered_map<std::string, Tablet *> &tablets);

    void insertTablets(std::unordered_map<std::string, Tablet *> &tablets, bool sorted);

    void insertAlignedTablets(std::unordered_map<std::string, Tablet *> &tablets);

    void insertAlignedTablets(std::unordered_map<std::string, Tablet *> &tablets, bool sorted);

    void testInsertRecord(const std::string &deviceId, int64_t time,
                          const std::vector<std::string> &measurements,
                          const std::vector<std::string> &values);

    void testInsertTablet(const Tablet &tablet);

    void testInsertRecords(const std::vector<std::string> &deviceIds,
                           const std::vector<int64_t> &times,
                           const std::vector<std::vector<std::string>> &measurementsList,
                           const std::vector<std::vector<std::string>> &valuesList);

    void deleteTimeseries(const std::string &path);

    void deleteTimeseries(const std::vector<std::string> &paths);

    void deleteData(const std::string &path, int64_t time);

    void deleteData(const std::vector<std::string> &deviceId, int64_t time);

    void setStorageGroup(const std::string &storageGroupId);

    void deleteStorageGroup(const std::string &storageGroup);

    void deleteStorageGroups(const std::vector<std::string> &storageGroups);

    void createTimeseries(const std::string &path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor);

    void createTimeseries(const std::string &path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                          CompressionType::CompressionType compressor,
                          std::map<std::string, std::string> *props, std::map<std::string, std::string> *tags,
                          std::map<std::string, std::string> *attributes,
                          const std::string &measurementAlias);

    void createMultiTimeseries(const std::vector<std::string> &paths,
                               const std::vector<TSDataType::TSDataType> &dataTypes,
                               const std::vector<TSEncoding::TSEncoding> &encodings,
                               const std::vector<CompressionType::CompressionType> &compressors,
                               std::vector<std::map<std::string, std::string>> *propsList,
                               std::vector<std::map<std::string, std::string>> *tagsList,
                               std::vector<std::map<std::string, std::string>> *attributesList,
                               std::vector<std::string> *measurementAliasList);

    void createAlignedTimeseries(const std::string &deviceId,
                                 const std::vector<std::string> &measurements,
                                 const std::vector<TSDataType::TSDataType> &dataTypes,
                                 const std::vector<TSEncoding::TSEncoding> &encodings,
                                 const std::vector<CompressionType::CompressionType> &compressors);

    bool checkTimeseriesExists(const std::string &path);

    std::unique_ptr<SessionDataSet> executeQueryStatement(const std::string &sql);

    void executeNonQueryStatement(const std::string &sql);

    void createSchemaTemplate(const Template &templ);

    void setSchemaTemplate(const std::string &template_name, const std::string &prefix_path);

    void unsetSchemaTemplate(const std::string &prefix_path, const std::string &template_name);

    void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                          const std::vector<std::string> &measurements,
                                          const std::vector<TSDataType::TSDataType> &dataTypes,
                                          const std::vector<TSEncoding::TSEncoding> &encodings,
                                          const std::vector<CompressionType::CompressionType> &compressors);

    void addAlignedMeasurementsInTemplate(const std::string &template_name,
                                          const std::string &measurement,
                                          TSDataType::TSDataType dataType,
                                          TSEncoding::TSEncoding encoding,
                                          CompressionType::CompressionType compressor);

    void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                            const std::vector<std::string> &measurements,
                                            const std::vector<TSDataType::TSDataType> &dataTypes,
                                            const std::vector<TSEncoding::TSEncoding> &encodings,
                                            const std::vector<CompressionType::CompressionType> &compressors);

    void addUnalignedMeasurementsInTemplate(const std::string &template_name,
                                            const std::string &measurement,
                                            TSDataType::TSDataType dataType,
                                            TSEncoding::TSEncoding encoding,
                                            CompressionType::CompressionType compressor);

    void deleteNodeInTemplate(const std::string &template_name, const std::string &path);

    int countMeasurementsInTemplate(const std::string &template_name);

    bool isMeasurementInTemplate(const std::string &template_name, const std::string &path);

    bool isPathExistInTemplate(const std::string &template_name, const std::string &path);

    std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name);

    std::vector<std::string> showMeasurementsInTemplate(const std::string &template_name, const std::string &pattern);

    bool checkTemplateExists(const std::string &template_name);
};

#endif // IOTDB_SESSION_H
