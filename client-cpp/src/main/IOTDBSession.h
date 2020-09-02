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
 
#include <string>
#include <vector>
#include <exception> 
#include <iostream>
#include <algorithm>
#include <map>
#include <utility>
#include <memory>
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

class IoTDBConnectionException : public std::exception
{
    public:
        IoTDBConnectionException() : message() {}
        IoTDBConnectionException(const char* m) : message(m) {}
        IoTDBConnectionException(std::string m) : message(m) {}
        virtual const char* what() const throw () 
        {
            return message.c_str();
        }

    private:
        std::string message;
};

class BatchExecutionException : public std::exception
{
public:
    BatchExecutionException() : message() {}
    BatchExecutionException(const char* m) : message(m) {}
    BatchExecutionException(std::string m) : message(m) {}
    BatchExecutionException(std::vector<TSStatus> statusList) : message(), statusList(statusList) {}
    BatchExecutionException(std::vector<TSStatus> statusList, std::string m) : message(m), statusList(statusList) {}
    virtual const char* what() const throw ()
    {
        return message.c_str();
    }
    std::vector<TSStatus> statusList;
private:
    std::string message;
    
};

class UnSupportedDataTypeException : public std::exception
{
private:
    std::string message;
public:
    UnSupportedDataTypeException() : message() {}
    UnSupportedDataTypeException(const char* m) : message(m) {}
    UnSupportedDataTypeException(std::string m) : message("UnSupported dataType: " + m) {}
};

namespace CompressionType{

    enum CompressionType
    {
        UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA
    };
}
namespace TSDataType{
    enum TSDataType
    {
        BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NULLTYPE
    };
}
namespace TSEncoding {
    enum TSEncoding {
        PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA, REGULAR
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
        NODE_READ_ONLY= 704,
        INCOMPATIBLE_VERSION = 203,
    };
}

class Config
{
public:
    static const std::string DEFAULT_USER;
    static const std::string DEFAULT_PASSWORD;
    static const int DEFAULT_FETCH_SIZE = 10000;
    static const int DEFAULT_TIMEOUT_MS = 0;
};

class RpcUtils
{
public:
    std::shared_ptr<TSStatus> SUCCESS_STATUS;
    RpcUtils() {
        SUCCESS_STATUS = std::make_shared<TSStatus>();
        SUCCESS_STATUS->__set_code(TSStatusCode::SUCCESS_STATUS);
    }
    static void verifySuccess(TSStatus& status);
    static void verifySuccess(std::vector<TSStatus>& statuses);
    static TSStatus getStatus(TSStatusCode::TSStatusCode tsStatusCode);
    static TSStatus getStatus(int code, std::string message);
    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode);
    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, std::string message);
    static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(TSStatus& status);
    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode);
    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, std::string appendMessage);
    static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(TSStatus& status);
};

// Simulate the ByteBuffer class in Java
class MyStringBuffer {
private:
    char* getchar(int len)
    {
        char* ret = new char[len];
        for (int i = pos; i < pos + len; i++)
            ret[pos + len - 1 - i] = str[i];
        pos += len;
        return ret;
    }

    void putchar(int len, char* ins)
    {
        for (int i = len - 1; i > -1; i--)
            str += ins[i];
    }
public:
    std::string str;
    int pos;

    bool hasRemaining() {
        return pos < str.size();
    }

    MyStringBuffer() {}

    MyStringBuffer(std::string str) {
        this->str = str;
        this->pos = 0;
    }

    //byte get() {
    //    char tmpChar = getChar();
    //    return (byte)tmpChar;
    //}

    int getInt()
    {
        char* data = getchar(4);
        int ret = *(int*)data;
        delete[]data;
        return ret;
    }

    int64_t getLong()
    {
        char* data = getchar(8);
        int64_t ret = *(int64_t*)data;
        delete[]data;
        return ret;
    }

    float getFloat()
    {
        char* data = getchar(4);
        float ret = *(float*)data;
        delete[]data;
        return ret;
    }

    double getDouble()
    {
        char* data = getchar(8);
        double ret = *(double*)data;
        delete[]data;
        return ret;
    }

    char getChar()
    {
        char* data = getchar(1);
        char ret = *(char*)data;
        delete[]data;
        return ret;
    }

    bool getBool()
    {
        char bo = getChar();
        return bo == 1;
    }

    std::string getString()
    {
        int len = getInt();
        std::string ret;
        for (int i = 0; i < len; i++) ret.append(1, getChar());
        return ret;
    }

    void putInt(int ins)
    {
        char* data = (char*)&ins;
        putchar(4, data);
    }

    void putLong(int64_t ins)
    {
        char* data = (char*)&ins;
        putchar(8, data);
    }

    void putFloat(float ins)
    {
        char* data = (char*)&ins;
        putchar(4, data);
    }

    void putDouble(double ins)
    {
        char* data = (char*)&ins;
        putchar(8, data);
    }

    void putChar(char ins)
    {
        char* data = (char*)&ins;
        putchar(1, data);
    }

    void putBool(bool ins)
    {
        char tmp = 0;
        if (ins) tmp = 1;
        putChar(tmp);
    }

    void putString(std::string ins)
    {
        int len = ins.size();
        putInt(len);
        for (int i = 0; i < len; i++) putChar(ins[i]);
    }
};

class Field
{
public:
    TSDataType::TSDataType dataType;
    bool boolV;
    int intV;
    int64_t longV;
    float floatV;
    double doubleV;
    std::string stringV;
    Field(TSDataType::TSDataType a)
    {
        dataType = a;
    }
    Field(){}
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
    std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas; // the list of measurement schemas for creating the tablet
    int64_t* timestamps;   //timestamps in this tablet
    std::vector<std::vector<std::string>> values;
    int rowSize;    //the number of rows to include in this tablet
    int maxRowNumber;   // the maximum number of rows for this tablet

    Tablet(){}
    /**
   * Return a tablet with default specified row number. This is the standard
   * constructor (all Tablet should be the same size).
   *
   * @param deviceId   the name of the device specified to be written in
   * @param timeseries the list of measurement schemas for creating the tablet
   */
    Tablet(std::string deviceId, std::vector<std::pair<std::string, TSDataType::TSDataType>>& timeseries) {
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
    Tablet(std::string deviceId, std::vector<std::pair<std::string, TSDataType::TSDataType>>& schemas, int maxRowNumber) {
        this->deviceId = deviceId;
        this->schemas = schemas;
        this->maxRowNumber = maxRowNumber;

        // create timestamp column
        timestamps = new int64_t[maxRowNumber];
        // create value columns
        values.resize(schemas.size());
        for (int i = 0; i < schemas.size(); i++) {
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
    static std::string getTime(Tablet& tablet);
    static std::string getValue(Tablet& tablet);
};

class RowRecord
{
public:
    int64_t timestamp;
    std::vector<Field*> fields;
    RowRecord(int64_t timestamp)
    {
        this->timestamp = timestamp;
    }
    RowRecord(int64_t timestamp, std::vector<Field*> fields) {
        this->timestamp = timestamp;
        this->fields = fields;
    }
    RowRecord()
    {
        this->timestamp = -1;
    }
    std::string toString()
    {
        char buf[111];
        sprintf(buf,"%lld",timestamp);
        std::string ret = buf;
        for (int i = 0; i < fields.size(); i++)
        {
            ret.append("\t");
            TSDataType::TSDataType dataType = fields[i]->dataType;
            switch (dataType)
            {
                case TSDataType::BOOLEAN:{
                    if (fields[i]->boolV) ret.append("true");
                    else ret.append("false");
                    break;
                }
                case TSDataType::INT32:{
                    char buf[111];
                    sprintf(buf,"%d",fields[i]->intV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::INT64: {
                    char buf[111];
                    sprintf(buf,"%lld",fields[i]->longV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::FLOAT:{
                    char buf[111];
                    sprintf(buf,"%f",fields[i]->floatV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::DOUBLE:{
                    char buf[111];
                    sprintf(buf,"%lf",fields[i]->doubleV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::TEXT: {
                    ret.append(fields[i]->stringV);
                    break;
                }
                case TSDataType::NULLTYPE:{
                    ret.append("NULL");
                }
            }
        }
        ret.append("\n");
        return ret;
    }
};

class SessionDataSet
{
private:
    bool hasCachedRecord = false;
    std::string sql;
    int64_t queryId;
    int64_t sessionId;
	std::shared_ptr<TSIServiceIf> client;
    int batchSize = 1024;
    std::vector<std::string> columnNameList;
    std::vector<std::string> columnTypeDeduplicatedList;
    // duplicated column index -> origin index
    std::map<int, int> duplicateLocation;
    // column name -> column location
    std::map<std::string, int> columnMap;
    // column size
    int columnSize = 0;

    int rowsIndex = 0; // used to record the row index in current TSQueryDataSet
    std::shared_ptr<TSQueryDataSet> tsQueryDataSet;
    MyStringBuffer tsQueryDataSetTimeBuffer;
    RowRecord rowRecord;
    char* currentBitmap; // used to cache the current bitmap for every column
    static const int flag = 0x80; // used to do `or` operation with bitmap to judge whether the value is null
    
public:
    SessionDataSet(){}
    SessionDataSet(std::string sql, std::vector<std::string>& columnNameList, std::vector<std::string>& columnTypeList, int64_t queryId,
        std::shared_ptr<TSIServiceIf> client, int64_t sessionId, std::shared_ptr<TSQueryDataSet> queryDataSet) : tsQueryDataSetTimeBuffer(queryDataSet->time)
    {
        this->sessionId = sessionId;
        this->sql = sql;
        this->queryId = queryId;
        this->client = client;
        this->columnNameList = columnNameList;
        this->currentBitmap = new char[columnNameList.size()];
        this->columnSize = columnNameList.size();

        // column name -> column location
        for (int i = 0; i < columnNameList.size(); i++) {
            std::string name = columnNameList[i];
            if (this->columnMap.find(name) != this->columnMap.end()) {
                duplicateLocation[i] = columnMap[name];
            }
            else {
                this->columnMap[name] = i;
                this->columnTypeDeduplicatedList.push_back(columnTypeList[i]);
            }
        }
        this->tsQueryDataSet = queryDataSet;
    }

    int getBatchSize();
    void setBatchSize(int batchSize);
    std::vector<std::string> getColumnNames();
    bool hasNext();
    void constructOneRow();
    bool isNull(int index, int rowNum);
    RowRecord* next();
    void closeOperationHandle();
};


class Session
{
    private:
        std::string host;
        int port;
        std::string username;
        std::string password;
        TSProtocolVersion::type protocolVersion = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V2;
        std::shared_ptr<TSIServiceIf> client;
        std::shared_ptr<apache::thrift::transport::TSocket> transport;
        bool isClosed = true;
        int64_t sessionId;
        int64_t statementId;
        std::string zoneId;
        int fetchSize;
        
        bool checkSorted(Tablet& tablet);
        void sortTablet(Tablet& tablet);
        std::vector<std::string> sortList(std::vector<std::string>& valueList, int* index, int indexLength);
        void sortIndexByTimestamp(int* index, int64_t* timestamps, int length);
        std::string getTimeZone();
        void setTimeZone(std::string zoneId);
    public:
        Session(std::string host, int port) { Session(host, port, Config::DEFAULT_USER, Config::DEFAULT_PASSWORD); }
        Session(std::string host, int port, std::string username, std::string password)
        {
            this->host = host;
            this->port = port;
            this->username = username;
            this->password = password;
        }
        Session(std::string host, std::string port, std::string username, std::string password)
        {
            Session(host, stoi(port), username, password);
        }
        Session(std::string host, int port, std::string username, std::string password, int fetchSize)
        {
            this->host = host;
            this->port = port;
            this->username = username;
            this->password = password;
            this->fetchSize = fetchSize;
        }

        void open();
        void open(bool enableRPCCompression);
        void open(bool enableRPCCompression, int connectionTimeoutInMs);
        void close();
        void insertRecord(std::string deviceId, int64_t time, std::vector<std::string>& measurements, std::vector<std::string>& values);
        //void insertRecord(std::string deviceId, int64_t time, std::vector<std::string>& measurements, std::vector<TSDataType::Type>& types, )
        void insertRecords(std::vector<std::string>& deviceIds, std::vector<int64_t>& times, std::vector<std::vector<std::string>>& measurementsList, std::vector<std::vector<std::string>>& valuesList);
        void insertTablet(Tablet& tablet);
        void insertTablet(Tablet& tablet, bool sorted);
        void insertTablets(std::map<std::string, Tablet*>& tablets);
        void insertTablets(std::map<std::string, Tablet*>& tablets, bool sorted);
        void testInsertRecord(std::string deviceId, int64_t time, std::vector<std::string>& measurements, std::vector<std::string>& values);
        void testInsertTablet(Tablet& tablet);
        void testInsertRecords(std::vector<std::string>& deviceIds, std::vector<int64_t>& times, std::vector<std::vector<std::string>>& measurementsList, std::vector<std::vector<std::string>>& valuesList);
        void deleteTimeseries(std::string path);
        void deleteTimeseries(std::vector<std::string>& paths);
        void deleteData(std::string path, int64_t time);
        void deleteData(std::vector<std::string>& deviceId, int64_t time);
        void setStorageGroup(std::string storageGroupId);
        void deleteStorageGroup(std::string storageGroup);
        void deleteStorageGroups(std::vector<std::string>& storageGroups);
        void createTimeseries(std::string path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding, CompressionType::CompressionType compressor);
        void createTimeseries(std::string path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding, CompressionType::CompressionType compressor,
            std::map<std::string, std::string>* props, std::map<std::string, std::string>* tags, std::map<std::string, std::string>* attributes, std::string measurementAlias);
        void createMultiTimeseries(std::vector<std::string> paths, std::vector<TSDataType::TSDataType> dataTypes, std::vector<TSEncoding::TSEncoding> encodings, std::vector<CompressionType::CompressionType> compressors,
            std::vector<std::map<std::string, std::string>>* propsList, std::vector<std::map<std::string, std::string>>* tagsList, std::vector<std::map<std::string, std::string>>* attributesList, std::vector<std::string>* measurementAliasList);
        bool checkTimeseriesExists(std::string path);
        SessionDataSet* executeQueryStatement(std::string sql);
        void executeNonQueryStatement(std::string sql);
};