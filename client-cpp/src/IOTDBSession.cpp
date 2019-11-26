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

vector<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet,vector<string> columnTypeList)
{
    int rowCount = tsQueryDataSet.rowCount;
    MyStringStream byteBuffer(tsQueryDataSet.values);
    vector<RowRecord> rowRecordList;
    map<string,TSDataType> stringtoTSDataType;
    stringtoTSDataType["BOOLEAN"] = BOOLEAN;
    stringtoTSDataType["INT32"] = INT32;
    stringtoTSDataType["INT64"] = INT64;
    stringtoTSDataType["FLOAT"] = FLOAT;
    stringtoTSDataType["DOUBLE"] = DOUBLE;
    stringtoTSDataType["TEXT"] = TEXT;
    for (int i = 0; i < rowCount; i++)
    {
        long long timestamp = byteBuffer.getLong();
        RowRecord * rowRecord = new RowRecord(timestamp);
        rowRecordList.push_back(*rowRecord);
    }
    for (int j = 0; j < columnTypeList.size(); j++)
    {
        string type = columnTypeList[j];
        for (int i = 0; i < rowCount; i++)
        {
            Field *field;
            bool is_empty = byteBuffer.getBool();
            if (is_empty)
            {
                field = new Field(NULLTYPE);
            }
            else
            {
                TSDataType dataType = stringtoTSDataType[type];
                field = new Field(dataType);
                switch (dataType)
                {
                    case BOOLEAN:{
                        bool booleanValue = byteBuffer.getBool();
                        field->boolV = booleanValue;
                        break;
                    }
                    case INT32:{
                        int intValue = byteBuffer.getInt();
                        field->intV = intValue;
                        break;
                    }
                    case INT64: {
                        long long longValue = byteBuffer.getLong();
                        field->longV = longValue;
                        break;
                    }
                    case FLOAT:{
                        float floatValue = byteBuffer.getFloat();
                        field->floatV = floatValue;
                        break;
                    }
                    case DOUBLE:{
                        double doubleValue = byteBuffer.getDouble();
                        field->doubleV = doubleValue;
                        break;
                    }
                    case TEXT: {
                        string stringValue = byteBuffer.getString();
                        field->stringV = stringValue;
                        break;
                    }
                    default:
                    {
                        char buf[111];
                        sprintf(buf,"Data type %s is not supported.",type.c_str());
                        throw IoTDBSessionException(buf);
                    }
                }
            }
            rowRecordList[i].fields.push_back(*field);
        }
    }
    return rowRecordList;
}

int SessionDataSet::getBatchSize()
{
    return batchSize;
}

void SessionDataSet::setBatchSize(int batchSize)
{
    this->batchSize = batchSize;
}

bool SessionDataSet::hasNext()
{
    return getFlag || nextWithoutConstraints(sql, queryId);
}

RowRecord SessionDataSet::next()
{
    if (!getFlag)
    {
        nextWithoutConstraints(sql, queryId);
    }
    getFlag = false;
    return record;
}

bool SessionDataSet::nextWithoutConstraints(string sql, long long queryId)
{
    if ((recordItr == -1 || recordItr == records.size()))
    {
        shared_ptr<TSFetchResultsReq> req(new TSFetchResultsReq());
        req->__set_statement(sql);
        req->__set_fetch_size(batchSize);
        req->__set_queryId(queryId);
        shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
        try
        {
            client->fetchResults(*resp,*req);
            verifySuccess(resp->status);
            if (!resp->hasResultSet)
            {
                return false;
            }
            else
            {
                TSQueryDataSet tsQueryDataSet = resp->queryDataSet;
                records = convertRowRecords(tsQueryDataSet, columnTypeDeduplicatedList);
                recordItr = 0;
            }
        }
        catch (IoTDBSessionException e)
        {
            char buf[111];
            sprintf(buf,"Cannot fetch result from server, because of network connection : %s",e.what());
            throw IoTDBSessionException(buf);
        }
    }
    record = records[recordItr++];
    getFlag = true;
    return true;
}

void SessionDataSet::closeOperationHandle()
{
    shared_ptr<TSCloseOperationReq> req(new TSCloseOperationReq());
    req->__set_queryId(queryId);
    req->__set_operationHandle(operationHandle);
    shared_ptr<TSStatus> resp(new TSStatus());
    try
    {
        client->closeOperation(*resp,*req);
        verifySuccess(*resp);
    }
    catch (IoTDBSessionException e)
    {
        char buf[111];
        sprintf(buf,"Error occurs for close opeation in server side. The reason is  %s",e.what());
        throw IoTDBSessionException(buf);
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
    shared_ptr<TSOpenSessionReq> req(new TSOpenSessionReq());
    req->__set_client_protocol(TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V1);
    req->__set_username(username);
    req->__set_password(password);
    try 
    {
        shared_ptr<TSOpenSessionResp> resp(new TSOpenSessionResp());
        client->openSession(*resp,*req);
        verifySuccess(resp->status);
        if (protocolVersion != resp->serverProtocolVersion) 
        {
            char buf[111];
            sprintf(buf,"Protocol not supported.");
            throw IoTDBSessionException(buf);
        }
        sessionHandle = resp->sessionHandle;
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

 

TSStatus Session::insert(string deviceId,  long long time, vector<string> measurements, vector<string> values)
{
    shared_ptr<TSInsertReq> req(new TSInsertReq());
    req->__set_deviceId(deviceId);
    req->__set_timestamp(time);
    req->__set_measurements(measurements);
    req->__set_values(values);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->insertRow(*resp,*req);
        verifySuccess(*resp);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return *resp;
}



TSStatus Session::deleteData(string path,  long long time)
{
    vector<string> paths;
    paths.push_back(path);
    return deleteData(paths, time);
}



TSStatus Session::deleteData(vector<string> deviceId, long long time)
{
    shared_ptr<TSDeleteDataReq> req(new TSDeleteDataReq());
    req->__set_paths(deviceId);
    req->__set_timestamp(time);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteData(*resp,*req);
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
    shared_ptr<TSCreateTimeseriesReq> req(new TSCreateTimeseriesReq());
    req->__set_path(path);
    req->__set_dataType(dataType);
    req->__set_encoding(encoding);
    req->__set_compressor(compressor);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->createTimeseries(*resp,*req);
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


SessionDataSet* Session::executeQueryStatement(string sql)
{
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionHandle(sessionHandle);
    req->__set_statement(sql);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try
    {
        client->executeStatement(*resp,*req);
        verifySuccess(resp->status);
    } 
    catch (IoTDBSessionException e) 
    {
        throw IoTDBSessionException(e.what());
    }
    return new SessionDataSet(sql, resp->columns, resp->dataTypeList,resp->operationHandle.operationId.queryId, client, resp->operationHandle);
}

void Session::executeNonQueryStatement(string sql)
{
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionHandle(sessionHandle);
    req->__set_statement(sql);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try
    {
        client->executeStatement(*resp,*req);
        verifySuccess(resp->status);
    }
    catch (IoTDBSessionException e)
    {
        throw IoTDBSessionException(e.what());
    }
}