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
 
#include<string>
#include<vector>
#include<exception> 
#include<iostream>
#include<algorithm>
#include<thrift/protocol/TBinaryProtocol.h>
#include<thrift/protocol/TCompactProtocol.h>
#include<thrift/transport/TSocket.h>
#include<thrift/transport/TTransportException.h>
#include "TSIService.h"
using namespace std;
using ::apache::thrift::protocol::TBinaryProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::TException;

class IoTDBSessionException : public exception
{
    public:
        IoTDBSessionException() : message() {}
        IoTDBSessionException(const char* m) : message(m) {}
        IoTDBSessionException(string m) : message(m) {}
        virtual const char* what() const throw () 
        {
            return message.c_str();
        }

    private:
        string message;
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
class Field
{
public:
    TSDataType::TSDataType dataType;
    bool boolV;
    int intV;
    long long longV;
    float floatV;
    double doubleV;
    string stringV;
    Field(TSDataType::TSDataType a)
    {
        dataType = a;
    }
};

class RowRecord
{
public:
    long long timestamp;
    vector<Field> fields;
    RowRecord(long long timestamp)
    {
        this->timestamp = timestamp;
    }
    RowRecord()
    {
        this->timestamp = -1;
    }
    string toString()
    {
        char buf[111];
        sprintf(buf,"%lld",timestamp);
        string ret = buf;
        for (int i = 0; i < fields.size(); i++)
        {
            ret.append("\t");
            TSDataType::TSDataType dataType = fields[i].dataType;
            switch (dataType)
            {
                case TSDataType::BOOLEAN:{
                    if (fields[i].boolV) ret.append("true");
                    else ret.append("false");
                    break;
                }
                case TSDataType::INT32:{
                    char buf[111];
                    sprintf(buf,"%d",fields[i].intV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::INT64: {
                    char buf[111];
                    sprintf(buf,"%lld",fields[i].longV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::FLOAT:{
                    char buf[111];
                    sprintf(buf,"%f",fields[i].floatV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::DOUBLE:{
                    char buf[111];
                    sprintf(buf,"%lf",fields[i].doubleV);
                    ret.append(buf);
                    break;
                }
                case TSDataType::TEXT: {
                    ret.append(fields[i].stringV);
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
    bool getFlag = false;
    string sql;
    long long queryId;
    RowRecord record;
    shared_ptr<TSIServiceIf> client;
    int batchSize = 512;
    vector<string> columnTypeDeduplicatedList;
    TSOperationHandle operationHandle;
    int recordItr = -1;
    vector<RowRecord> records;
public:
    SessionDataSet(string sql, vector<string> columnNameList, vector<string> columnTypeList, long long queryId, shared_ptr<TSIServiceIf> client, TSOperationHandle operationHandle)
    {
        this->sql = sql;
        this->queryId = queryId;
        this->client = client;
        this->operationHandle = operationHandle;
        map<string,bool> columnSet;
        for (int i = 0; i < columnNameList.size(); i++)
        {
            string name = columnNameList[i];
            if (!columnSet[name])
            {
                columnSet[name] = 1;
                columnTypeDeduplicatedList.push_back(columnTypeList[i]);
            }
        }
    }
    int getBatchSize();
    void setBatchSize(int batchSize);
    bool hasNext();
    RowRecord next();
    bool nextWithoutConstraints(string sql, long long queryId);
    void closeOperationHandle();
    string columntoString();


};


class Session
{
    private:
        string host;
        int port;
        string username;
        string password;
        TSProtocolVersion::type protocolVersion = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V1;
        shared_ptr<TSIServiceIf> client;
        TS_SessionHandle sessionHandle;
        shared_ptr<apache::thrift::transport::TSocket> transport;
        bool isClosed = true;
        long long queryId = 0;
        string zoneId;
        
    public:
        Session(string host, int port, string username, string password) 
        {
            this->host = host;
            this->port = port;
            this->username = username;
            this->password = password;
        }
        Session(string host, string port, string username, string password) 
        {
            Session(host, stoi(port), username, password);
        }
        void open();
        void open(bool enableRPCCompression, int connectionTimeoutInMs);
        void close();
        TSStatus insert(string deviceId, long long time, vector<string> measurements, vector<string> values);
        TSStatus deleteData(string path, long long time);
        TSStatus deleteData(vector<string> deviceId, long long time);
        TSStatus setStorageGroup(string storageGroupId);
        TSStatus deleteStorageGroup(string storageGroup);
        TSStatus deleteStorageGroups(vector<string> storageGroups);
        TSStatus createTimeseries(string path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding, CompressionType::CompressionType compressor);
        TSStatus deleteTimeseries(string path);
        TSStatus deleteTimeseries(vector<string> paths);
        SessionDataSet* executeQueryStatement(string sql);
        void executeNonQueryStatement(string sql);
        void insertBatch(string deviceId, int rowCount, vector<string> measurements, vector<TSDataType::TSDataType> types, vector<long long> timestamps, vector<vector<string> > values);
        void insertBatch(string deviceId, int rowCount, vector<string> measurements, vector<TSDataType::TSDataType> types, vector<long long> timestamps, vector<vector<Field> > values);
        string getTimeZone();
        void setTimeZone(string zoneId);
};

class MyStringStream {
private:
    char *getchar(int len)
    {
        char *ret = new char[len];
        for (int i = pos; i < pos + len; i++)
            ret[pos + len - 1 - i] = str[i];
        pos += len;
        return ret;
    }

    void putchar(int len, char * ins)
    {
        for (int i = len - 1; i > -1; i--)
            str += ins[i];
    }
public:
    string str;
    int pos;

    MyStringStream(string str) {
        this->str = str;
        this->pos = 0;
    }

    int getInt()
    {
        char * data = getchar(4);
        int ret = *(int *)data;
        delete []data;
        return ret;
    }

    long long getLong()
    {
        char * data = getchar(8);
        long long ret = *(long long *)data;
        delete []data;
        return ret;
    }

    float getFloat()
    {
        char * data = getchar(4);
        float ret = *(float *)data;
        delete []data;
        return ret;
    }

    double getDouble()
    {
        char * data = getchar(8);
        double ret = *(double *)data;
        delete []data;
        return ret;
    }

    char getChar()
    {
        char * data = getchar(1);
        char ret = *(char *)data;
        delete []data;
        return ret;
    }

    bool getBool()
    {
        char bo = getChar();
        return bo == 1;
    }

    string getString()
    {
        int len = getInt();
        string ret;
        for (int i = 0; i < len ; i++) ret.append(1,getChar());
        return ret;
    }

    void putInt(int ins)
    {
        char * data = (char *)&ins;
        putchar(4,data);
    }

    void putLong(long long ins)
    {
        char * data = (char *)&ins;
        putchar(8,data);
    }

    void putFloat(float ins)
    {
        char * data = (char *)&ins;
        putchar(4,data);
    }

    void putDouble(double ins)
    {
        char * data = (char *)&ins;
        putchar(8,data);
    }

    void putChar(char ins)
    {
        char * data = (char *)&ins;
        putchar(1,data);
    }

    void putBool(bool ins)
    {
        char tmp = 0;
        if (ins) tmp = 1;
        putChar(tmp);
    }

    void putString(string ins)
    {
        int len = ins.size();
        putInt(len);
        for (int i = 0; i < len ; i++) putChar(ins[i]);
    }
};


