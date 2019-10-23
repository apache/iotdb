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
        shared_ptr<TSIServiceIf> tmp3(new TSIServiceClient(tmp2));
        client = tmp3;
    }
    else 
    {
        shared_ptr<TBinaryProtocol> tmp2(new TBinaryProtocol(transport));
        shared_ptr<TSIServiceIf> tmp3(new TSIServiceClient(tmp2));
        client = tmp3;
    }
    shared_ptr<TSOpenSessionReq> openReq(new TSOpenSessionReq());
    openReq->__set_client_protocol(TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V1);
    openReq->__set_username(username);
    openReq->__set_password(password);
    try 
    {
        shared_ptr<TSOpenSessionResp> openResp(new TSOpenSessionResp());
        client->openSession(*openResp,*openReq);
        verifySuccess(openResp->status);
        if (protocolVersion != openResp->serverProtocolVersion) 
        {
            char buf[111];
            sprintf(buf,"Protocol not supported.");
            throw IoTDBSessionException(buf);
        }
        sessionHandle = openResp->sessionHandle;
    } 
    catch (IoTDBSessionException e) 
    {
        transport->close();
        char buf[111];
        sprintf(buf,"Can not open session to %s:%d with user: %s. %s",host.c_str(), port, username.c_str(),e.what());
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
    shared_ptr<TSCloseSessionReq> req(new TSCloseSessionReq());
    req->__set_sessionHandle(sessionHandle);
    try 
    {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->closeSession(*resp,*req);
    } 
    catch (IoTDBSessionException e) 
    {
        char buf[111];
        sprintf(buf,"Error occurs when closing session at server. Maybe server is down. %s",e.what());
        throw IoTDBSessionException(buf);
    } 
    isClosed = true;
    if (transport != NULL) 
    {
        transport->close();
    }
}

 

TSStatus Session::insert(string deviceId, long long time, vector<string> measurements, vector<string> values)
{
    shared_ptr<TSInsertReq> request(new TSInsertReq());
    request->__set_deviceId(deviceId);
    request->__set_timestamp(time);
    request->__set_measurements(measurements);
    request->__set_values(values);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->insertRow(*resp,*request);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::deleteData(string path, long long time) 
{
    vector<string> paths;
    paths.push_back(path);
    return deleteData(paths, time);
}



TSStatus Session::deleteData(vector<string> deviceId, long long time) 
{
    shared_ptr<TSDeleteDataReq> request(new TSDeleteDataReq());
    request->__set_paths(deviceId);
    request->__set_timestamp(time);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteData(*resp,*request);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::setStorageGroup(string storageGroupId)
{
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->setStorageGroup(*resp,storageGroupId);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::deleteStorageGroup(string storageGroup)
{
    vector<string> storageGroups;
    storageGroups.push_back(storageGroup);
    return deleteStorageGroups(storageGroups);
}

TSStatus Session::deleteStorageGroups(vector<string> storageGroups)
{
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteStorageGroups(*resp, storageGroups);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::createTimeseries(string path, TSDataType dataType, TSEncoding encoding, CompressionType compressor) 
{
    shared_ptr<TSCreateTimeseriesReq> request(new TSCreateTimeseriesReq());
    request->__set_path(path);
    request->__set_dataType(dataType);
    request->__set_encoding(encoding);
    request->__set_compressor(compressor);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->createTimeseries(*resp,*request);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::deleteTimeseries(string path) 
{
    vector<string> paths;
    paths.push_back(path);
    return deleteTimeseries(paths);
}

TSStatus Session::deleteTimeseries(vector<string> paths)
{
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteTimeseries(*resp, paths);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
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
    shared_ptr<TSGetTimeZoneResp> resp(new TSGetTimeZoneResp());
    try 
    {
        client->getTimeZone(*resp);
        verifySuccess(resp->status);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return resp->timeZone;
}



void Session::setTimeZone(string zoneId)
{
    shared_ptr<TSSetTimeZoneReq> req(new TSSetTimeZoneReq());
    req->__set_timeZone(zoneId);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->setTimeZone(*resp,*req);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    this->zoneId = zoneId;
}

