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
 
#include "IOTDBSession.h"

using namespace std;
using ::apache::thrift::protocol::TBinaryProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::TException;



void verifySuccess(TSStatus status)
{
    if (status.statusType.code != 200) 
    {
        throw IoTDBSessionException(status.statusType.message);
    }
}



void Session::open()
{
    open(false, 0);
}



void Session::open(bool enableRPCCompression, int connectionTimeoutInMs)
{
    if (!isClosed) 
    {
        return;
    }
    shared_ptr<TSocket> tmp(new TSocket(host, port));
    transport = tmp;       
    transport->setConnTimeout(connectionTimeoutInMs);
    if (!transport->isOpen()) 
    {
        try 
        {
            transport->open();
        } 
        catch (TTransportException e) 
        {
            throw IoTDBSessionException(e.what());
        }
    }
    if (enableRPCCompression) 
    {
        shared_ptr<TCompactProtocol> tmp2(new TCompactProtocol(transport));
        client = new TSIServiceClient(tmp2);
    }
    else 
    {
        shared_ptr<TBinaryProtocol> tmp2(new TBinaryProtocol(transport));
        client = new TSIServiceClient(tmp2);
    }
    TSOpenSessionReq* openReq = new TSOpenSessionReq();
    openReq->__set_client_protocol(TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V1);
    openReq->__set_username(username);
    openReq->__set_password(password);
    try 
    {
        TSOpenSessionResp* openResp = new TSOpenSessionResp();
        client->openSession(*openResp,*openReq);
        verifySuccess(openResp->status);
        if (TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V1 != openResp->serverProtocolVersion) 
        {
            char buf[111];
            sprintf(buf,"Protocol not supported");
            throw IoTDBSessionException(buf);
        }
        sessionHandle = &(openResp->sessionHandle);
    } 
    catch (exception e) 
    {
        transport->close();
        char buf[111];
        sprintf(buf,"Can not open session to %s:%d with user: %s.",host.c_str(), port, username.c_str());
        throw IoTDBSessionException(buf);
    }
    isClosed = false;
}



void Session::close()
{
    if (isClosed) 
    {
        return;
    }
    TSCloseSessionReq* req = new TSCloseSessionReq();
    req->__set_sessionHandle(*sessionHandle);
    try 
    {
        TSStatus *resp = new TSStatus();
        client->closeSession(*resp,*req);
    } 
    catch (TException e) 
    {
        throw IoTDBSessionException("Error occurs when closing session at server. Maybe server is down.");
    } 
    isClosed = true;
    if (transport != NULL) 
    {
        transport->close();
    }
}

 

TSStatus Session::insert(string deviceId, long time, vector<string> measurements, vector<string> values)
{
    TSInsertReq* request = new TSInsertReq();
    request->__set_deviceId(deviceId);
    request->__set_timestamp(time);
    request->__set_measurements(measurements);
    request->__set_values(values);
    TSStatus* resp = new TSStatus();
    try 
    {
        client->insertRow(*resp,*request);
    } 
    catch (TException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::deleteData(vector<string> deviceId, long time) 
{
    TSDeleteDataReq* request = new TSDeleteDataReq();
    request->__set_paths(deviceId);
    request->__set_timestamp(time);
    TSStatus* resp = new TSStatus();
    try 
    {
        client->deleteData(*resp,*request);
    } 
    catch (TException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::setStorageGroup(string storageGroupId)
{
    TSStatus* resp = new TSStatus();
    try 
    {
        client->setStorageGroup(*resp,storageGroupId);
    } 
    catch (TException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::createTimeseries(string path, TSDataType dataType, TSEncoding encoding, CompressionType compressor) 
{
    TSCreateTimeseriesReq* request = new TSCreateTimeseriesReq();
    request->__set_path(path);
    request->__set_dataType(dataType);
    request->__set_encoding(encoding);
    request->__set_compressor(compressor);
    TSStatus* resp = new TSStatus();
    try 
    {
        client->createTimeseries(*resp,*request);
    } 
    catch (TException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



string Session::getTimeZone() 
{
    if (zoneId != "") 
    {
        return zoneId;
    }
    TSGetTimeZoneResp* resp = new TSGetTimeZoneResp();
    client->getTimeZone(*resp);
    verifySuccess(resp->status);
    return resp->timeZone;
}



void Session::setTimeZone(string zoneId)
{
    TSSetTimeZoneReq* req = new TSSetTimeZoneReq();
    req->__set_timeZone(zoneId);
    TSStatus* resp = new TSStatus();
    client->setTimeZone(*resp,*req);
    verifySuccess(*resp);
    this->zoneId = zoneId;
}




