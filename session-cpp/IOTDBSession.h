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


class IoTDBSessionException : public std::exception
{
    public:
        IoTDBSessionException() : message() {}
        IoTDBSessionException(const char* m) : message(m) {}
        IoTDBSessionException(std::string m) : message(m) {}
        virtual const char* what() const throw () 
        {
            return message.c_str();
        }

    private:
        std::string message;
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
        std::string host;
        int port;
        std::string username;
        std::string password;
        TSProtocolVersion protocolVersion;
        TSIServiceIf* client = NULL;
        TS_SessionHandle* sessionHandle = NULL;
        std::shared_ptr<apache::thrift::transport::TSocket> transport;
        bool isClosed = true;
        std::string zoneId;
        
    public:
        Session(std::string host, int port, std::string username, std::string password) 
        {
            this->host = host;
            this->port = port;
            this->username = username;
            this->password = password;
        }
        Session(std::string host, std::string port, std::string username, std::string password) 
        {
            Session(host, std::stoi(port), username, password);
        }
        void open();
        void open(bool enableRPCCompression, int connectionTimeoutInMs);
        void close();
        TSStatus insert(std::string deviceId, long time, std::vector<std::string> measurements, std::vector<std::string> values);
        TSStatus deletedata(std::vector<std::string> deviceId, long time);
        TSStatus setStorageGroup(std::string storageGroupId);
        TSStatus createTimeseries(std::string path, TSDataType dataType, TSEncoding encoding, CompressionType compressor);
        std::string getTimeZone();
        void setTimeZone(std::string zoneId);
};
