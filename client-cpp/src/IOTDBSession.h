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


enum CompressionType 
{
    UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA
};
enum TSDataType 
{
    BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT
};
enum TSEncoding 
{
    PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA, REGULAR
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
        TSStatus createTimeseries(string path, TSDataType dataType, TSEncoding encoding, CompressionType compressor);
        TSStatus deleteTimeseries(string path);
        TSStatus deleteTimeseries(vector<string> paths);
        string getTimeZone();
        void setTimeZone(string zoneId);
};
