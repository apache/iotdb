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

const string Config::DEFAULT_USER = "user";
const string Config::DEFAULT_PASSWORD = "password";

TSDataType::TSDataType getTSDataTypeFromString(string str) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NULLTYPE
    if (str == "BOOLEAN")   return TSDataType::BOOLEAN;
    else if(str == "INT32") return TSDataType::INT32;
    else if(str == "INT64") return TSDataType::INT64;
    else if(str == "FLOAT") return TSDataType::FLOAT;
    else if(str == "DOUBLE") return TSDataType::DOUBLE;
    else if(str == "TEXT") return TSDataType::TEXT;
    else if(str == "NULLTYPE") return TSDataType::NULLTYPE;
    return TSDataType::TEXT;
}

void RpcUtils::verifySuccess(TSStatus& status) {
    if (status.code != TSStatusCode::SUCCESS_STATUS) {
        char buf[111];
        sprintf(buf, "%d: %s", status.code, status.message.c_str());
        throw IoTDBConnectionException(buf);
    }
}
void RpcUtils::verifySuccess(vector<TSStatus>& statuses) {
    for (TSStatus status : statuses) {
        if (status.code != TSStatusCode::SUCCESS_STATUS) {
            char buf[111];
            sprintf(buf, "%s", status.message.c_str());
            throw BatchExecutionException(statuses, buf);
        }
    }
}

TSStatus RpcUtils::getStatus(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus tmpTSStatus = TSStatus();
    tmpTSStatus.__set_code(tsStatusCode);
    return tmpTSStatus;
}
TSStatus RpcUtils::getStatus(int code, string message) {
    TSStatus status = TSStatus();
    status.__set_code(code);
    status.__set_message(message);
    return status;
}
shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSExecuteStatementResp(status);
}
shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, string message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
}
shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(TSStatus& status) {
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    TSStatus tsStatus(status);
    resp->status = status;
    return resp;
}
shared_ptr<TSExecuteBatchStatementResp> RpcUtils::getTSBatchExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSBatchExecuteStatementResp(status);
}
shared_ptr<TSExecuteBatchStatementResp> RpcUtils::getTSBatchExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, string message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSBatchExecuteStatementResp(status);
}
shared_ptr<TSExecuteBatchStatementResp> RpcUtils::getTSBatchExecuteStatementResp(TSStatus& status) {
    shared_ptr<TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
    resp->statusList.push_back(status);
    return resp;
}
shared_ptr<TSExecuteBatchStatementResp> RpcUtils::getTSBatchExecuteStatementResp(vector<TSStatus>& statusList) {
    shared_ptr<TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
    resp->__set_statusList(statusList);
    return resp;
}
shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
}
shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, string appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
}
shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(TSStatus& status) {
    shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
    TSStatus tsStatus(status);
    resp->__set_status(tsStatus);
    return resp;
}

void Tablet::reset() {
    rowSize = 0;
}

void Tablet::createColumns() {
    // create timestamp column
    timestamps = new int64_t[maxRowNumber];
    // create value columns
    values.resize(schemas.size());
    for (int i = 0; i < schemas.size(); i++) {
        values[i].resize(maxRowNumber);
    }
}

int Tablet::getTimeBytesSize() {
    return rowSize * 8;
}

int Tablet::getValueByteSize() {
    int valueOccupation = 0;
    for (int i = 0; i < schemas.size(); i++) {
        switch (schemas[i].second) {
        case TSDataType::BOOLEAN:
            valueOccupation += rowSize;
            break;
        case TSDataType::INT32:
            valueOccupation += rowSize * 4;
            break;
        case TSDataType::INT64:
            valueOccupation += rowSize * 8;
            break;
        case TSDataType::FLOAT:
            valueOccupation += rowSize * 4;
            break;
        case TSDataType::DOUBLE:
            valueOccupation += rowSize * 8;
            break;
        case TSDataType::TEXT:
            valueOccupation += rowSize * 4;
            for (string value : values[i]) {
                valueOccupation += value.size();
            }
            break;
        default:
            char buf[111];
            sprintf(buf, "Data type %d is not supported.", schemas[i].second);
            throw UnSupportedDataTypeException(buf);
        }
    }
    return valueOccupation;
}

string SessionUtils::getTime(Tablet& tablet) {
    MyStringBuffer timeBuffer;
    for (int i = 0; i < tablet.rowSize; i++) {
        timeBuffer.putLong(tablet.timestamps[i]);
    }
    return timeBuffer.str;
}

string SessionUtils::getValue(Tablet& tablet) {
    MyStringBuffer valueBuffer;
    for (int i = 0; i < tablet.schemas.size(); i++) {
        TSDataType::TSDataType dataType = tablet.schemas[i].second;
        switch (dataType)
        {
        case TSDataType::BOOLEAN:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putBool(tablet.values[i][index] == "true" ? true : false);
            }
            break;
        case TSDataType::INT32:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putInt(stoi(tablet.values[i][index]));
            }
            break;
        case TSDataType::INT64:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putLong(stol(tablet.values[i][index]));
            }
            break;
        case TSDataType::FLOAT:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putFloat(stof(tablet.values[i][index]));
            }
            break;
        case TSDataType::DOUBLE:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putDouble(stod(tablet.values[i][index]));
            }
            break;
        case TSDataType::TEXT:
            for (int index = 0; index < tablet.rowSize; index++) {
                valueBuffer.putString(tablet.values[i][index]);
            }
            break;
        default:
            char buf[111];
            sprintf(buf, "Data type %d is not supported.", dataType);
            throw UnSupportedDataTypeException(buf);
            break;
        }
    }
    return valueBuffer.str;
}

int SessionDataSet::getBatchSize()
{
    return batchSize;
}

void SessionDataSet::setBatchSize(int batchSize)
{
    this->batchSize = batchSize;
}

vector<string> SessionDataSet::getColumnNames() { return this->columnNameList; }

bool SessionDataSet::hasNext()
{
    if (hasCachedRecord) {
        return true;
    }
    if (!tsQueryDataSetTimeBuffer.hasRemaining()) {
        shared_ptr<TSFetchResultsReq> req(new TSFetchResultsReq());
        req->__set_sessionId(sessionId);
        req->__set_statement(sql);
        req->__set_fetchSize(batchSize);
        req->__set_queryId(queryId);
        req->__set_isAlign(true);
        try {
            shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
            client->fetchResults(*resp, *req);
            RpcUtils::verifySuccess(resp->status);

            if (!resp->hasResultSet) {
                return false;
            }
            else {
                tsQueryDataSet = make_shared<TSQueryDataSet>(resp->queryDataSet);
                tsQueryDataSetTimeBuffer = tsQueryDataSet->time;
                rowsIndex = 0;
            }
        }
        catch (IoTDBConnectionException e)
        {
            char buf[111];
            sprintf(buf, "Cannot fetch result from server, because of network connection: %s", e.what());
            throw IoTDBConnectionException(buf);
        }
    }

    constructOneRow();
    hasCachedRecord = true;
    return true;
}

void SessionDataSet::constructOneRow() {
    vector<Field*> outFields;
    int loc = 0;
    Field* field;
    for (int i = 0; i < columnSize; i++) {

        if (duplicateLocation.find(i) != duplicateLocation.end()) {
            field = new Field(*outFields[duplicateLocation[i]]);
        }
        else {
            MyStringBuffer bitmapBuffer = MyStringBuffer(tsQueryDataSet->bitmapList[loc]);
            // another new 8 row, should move the bitmap buffer position to next byte
            if (rowsIndex % 8 == 0) {
                currentBitmap[loc] = bitmapBuffer.getChar();
            }

            if (!isNull(loc, rowsIndex)) {
                MyStringBuffer valueBuffer = MyStringBuffer(tsQueryDataSet->valueList[loc]);
                TSDataType::TSDataType dataType = getTSDataTypeFromString(columnTypeDeduplicatedList[loc]);
                field = new Field(dataType);
                switch (dataType) {
                case TSDataType::BOOLEAN: {
                    bool booleanValue = valueBuffer.getBool();
                    field->boolV = booleanValue;
                    break;
                }
                case TSDataType::INT32: {
                    int intValue = valueBuffer.getInt();
                    field->intV = intValue;
                    break;
                }
                case TSDataType::INT64: {
                    int64_t longValue = valueBuffer.getLong();
                    field->longV = longValue;
                    break;
                }
                case TSDataType::FLOAT: {
                    float floatValue = valueBuffer.getFloat();
                    field->floatV = floatValue;
                    break;
                }
                case TSDataType::DOUBLE: {
                    double doubleValue = valueBuffer.getDouble();
                    field->doubleV = doubleValue;
                    break;
                }
                case TSDataType::TEXT: {
                    string stringValue = valueBuffer.getString();
                    field->stringV = stringValue;
                    break;
                }
                default: {
                    char buf[111];
                    sprintf(buf, "Data type %s is not supported.", columnTypeDeduplicatedList[i].c_str());
                    throw UnSupportedDataTypeException(buf);
                }
                }
            }
            else {
                field = new Field(TSDataType::NULLTYPE);
            }
            loc++;
        }
        outFields.push_back(field);
    }

    rowRecord = RowRecord(tsQueryDataSetTimeBuffer.getLong(), outFields);
    rowsIndex++;
}

bool SessionDataSet::isNull(int index, int rowNum) {
    char bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((flag >> shift) & bitmap) == 0;
}

RowRecord* SessionDataSet::next()
{
    if (!hasCachedRecord) {
        if (!hasNext()) {
            return NULL;
        }
    }

    hasCachedRecord = false;
    return &rowRecord;
}

void SessionDataSet::closeOperationHandle()
{
    shared_ptr<TSCloseOperationReq> closeReq(new TSCloseOperationReq());
    closeReq->__set_sessionId(sessionId);
    closeReq->__set_queryId(queryId);
    shared_ptr<TSStatus> closeResp(new TSStatus());
    try
    {
        client->closeOperation(*closeResp,*closeReq);
        RpcUtils::verifySuccess(*closeResp);
    }
    catch (IoTDBConnectionException e)
    {
        char buf[111];
        sprintf(buf,"Error occurs when connecting to server for close operation, because: %s",e.what());
        throw IoTDBConnectionException(buf);
    }
}


/**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
bool Session::checkSorted(Tablet& tablet) {
    for (int i = 1; i < tablet.rowSize; i++) {
        if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
            return false;
        }
    }
    return true;
}

void Session::sortTablet(Tablet& tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
     // sort to get index, and use index to sort value list
    int* index = new int[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
        index[i] = i;
    }

    this->sortIndexByTimestamp(index, tablet.timestamps, tablet.rowSize);
    sort(tablet.timestamps, tablet.timestamps + tablet.rowSize);
    for (int i = 0; i < tablet.schemas.size(); i++) {
        tablet.values[i] = sortList(tablet.values[i], index, tablet.rowSize);
    }
}

void Session::sortIndexByTimestamp(int* index, int64_t* timestamps, int length) {
    // Use Insert Sort Algorithm
    if (length >= 2) {
        for (int i = 1; i < length; i++) {
            int x = timestamps[i];
            int tmpIndex = index[i];
            int j = i - 1;

            while (j >= 0 && timestamps[j] > x) {
                timestamps[j + 1] = timestamps[j];
                index[j + 1] = index[j];
                j--;
            }

            timestamps[j + 1] = x;
            index[j + 1] = tmpIndex;
        }
    }
}

vector<string> Session::sortList(vector<string>& valueList, int* index, int indexLength) {
    vector<string> sortedValues(valueList.size());
    for (int i = 0; i < indexLength; i++) {
        sortedValues[i] = valueList[index[i]];
    }
    return sortedValues;
}

void Session::open()
{
    try
    {
        open(false, Config::DEFAULT_TIMEOUT_MS);
    }
    catch (IoTDBConnectionException e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::open(bool enableRPCCompression) {
    try
    {
        open(enableRPCCompression, Config::DEFAULT_TIMEOUT_MS);
    }
    catch (IoTDBConnectionException e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::open(bool enableRPCCompression, int connectionTimeoutInMs)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    if (!isClosed) 
    {
        return;
    }
	shared_ptr<TSocket> socket(new TSocket(host, port));
	shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	socket->setConnTimeout(connectionTimeoutInMs);
    if (!transport->isOpen()) 
    {
        try 
        {
            transport->open();
        } 
        catch (TTransportException e) 
        {
            throw IoTDBConnectionException(e.what());
        }
    }
    if (enableRPCCompression) 
    {
        shared_ptr<TCompactProtocol> protocol(new TCompactProtocol(transport));
		shared_ptr<TSIServiceIf> client_instance(new TSIServiceClient(protocol));
		client = client_instance;
    }
    else 
    {
		shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
		shared_ptr<TSIServiceIf> client_instance(new TSIServiceClient(protocol));
		client = client_instance;
    }
    shared_ptr<TSOpenSessionReq> openReq(new TSOpenSessionReq());
    openReq->__set_username(username);
    openReq->__set_password(password);
    try 
    {
        shared_ptr<TSOpenSessionResp> openResp(new TSOpenSessionResp());
        client->openSession(*openResp,*openReq);
        RpcUtils::verifySuccess(openResp->status);
        if (protocolVersion != openResp->serverProtocolVersion)
        {
            if (openResp->serverProtocolVersion == 0) {// less than 0.10
                char buf[111];
                sprintf(buf, "Protocol not supported, Client version is %d, but Server version is %d", protocolVersion, openResp->serverProtocolVersion);
                logic_error e(buf);
                throw exception(e);
            }
        }

        sessionId = openResp->sessionId;
        statementId = client->requestStatementId(sessionId);
        
        if (zoneId != "") {
            setTimeZone(zoneId);
        }
        else {
            zoneId = getTimeZone();
        }
    } 
    catch (exception e) 
    {
        transport->close();
        throw IoTDBConnectionException(e.what());
    }
    isClosed = false;
}



void Session::close()
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    if (isClosed) 
    {
        return;
    }
    shared_ptr<TSCloseSessionReq> req(new TSCloseSessionReq());
    req->__set_sessionId(sessionId);
    try 
    {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->closeSession(*resp,*req);
    } 
    catch (exception e) 
    {
        char buf[111];
        sprintf(buf,"Error occurs when closing session at server. Maybe server is down. %s",e.what());
        throw IoTDBConnectionException(buf);
    } 
    isClosed = true;
    if (transport != NULL) 
    {
        transport->close();
    }
}

 

void Session::insertRecord(string deviceId,  int64_t time, vector<string>& measurements, vector<string>& values)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSInsertRecordReq> req(new TSInsertRecordReq());
    req->__set_sessionId(sessionId);
    req->__set_deviceId(deviceId);
    req->__set_timestamp(time);
    req->__set_measurements(measurements);
    req->__set_values(values);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->insertRecord(*resp,*req);
        RpcUtils::verifySuccess(*resp);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertRecords(vector<string>& deviceIds, vector<int64_t>& times, vector<vector<string>>& measurementsList, vector<vector<string>>& valuesList) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }
    shared_ptr<TSInsertRecordsReq> request(new TSInsertRecordsReq());
    request->__set_sessionId(sessionId);
    request->__set_deviceIds(deviceIds);
    request->__set_timestamps(times);
    request->__set_measurementsList(measurementsList);
    request->__set_valuesList(valuesList);

    try
    {
        shared_ptr< TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
        client->insertRecords(*resp, *request);
        RpcUtils::verifySuccess(resp->statusList);
    }
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}


void Session::insertTablet(Tablet& tablet) {
    try
    {
        insertTablet(tablet, false);
    }
    catch (const std::exception& e)
    {
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablet(Tablet& tablet, bool sorted) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    if (sorted) {
        if (!checkSorted(tablet)) {
            throw BatchExecutionException("Times in Tablet are not in ascending order");
        }
    }
    else {
        sortTablet(tablet);
    }

    shared_ptr<TSInsertTabletReq> request(new TSInsertTabletReq());
    request->__set_sessionId(sessionId);
    request->deviceId = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema : tablet.schemas) {
        request->measurements.push_back(schema.first);
        request->types.push_back(schema.second);
    }
    request->__set_timestamps(SessionUtils::getTime(tablet));
    request->__set_values(SessionUtils::getValue(tablet));
    request->__set_size(tablet.rowSize);

    try
    {
        shared_ptr<TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
        client->insertTablet(*resp, *request);
        RpcUtils::verifySuccess(resp->statusList);
    }
    catch (IoTDBConnectionException& e)
    {
        throw new IoTDBConnectionException(e.what());
    }
}

void Session::insertTablets(map<string, Tablet*>& tablets) {
    try
    {
        insertTablets(tablets, false);
    }
    catch (const std::exception& e)
    {
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(map<string, Tablet*>& tablets, bool sorted) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSInsertTabletsReq> request(new TSInsertTabletsReq());
    request->__set_sessionId(sessionId);

    for (auto &item : tablets) {
        if (sorted) {
            if (!checkSorted(*(item.second))) {
                throw BatchExecutionException("Times in Tablet are not in ascending order");
            }
        }
        else {
            sortTablet(*(tablets[item.first]));
        }

        request->deviceIds.push_back(item.second->deviceId);
        vector<string> measurements;
        vector<int> dataTypes;
        for (pair<string, TSDataType::TSDataType> schema : item.second->schemas) {
            measurements.push_back(schema.first);
            dataTypes.push_back(schema.second);
        }
        request->measurementsList.push_back(measurements);
        request->typesList.push_back(dataTypes);
        request->timestampsList.push_back(SessionUtils::getTime(*(item.second)));
        request->valuesList.push_back(SessionUtils::getValue(*(item.second)));
        request->sizeList.push_back(item.second->rowSize);

        try
        {
            shared_ptr<TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
            client->insertTablets(*resp, *request);
            RpcUtils::verifySuccess(resp->statusList);
        }
        catch (const std::exception& e)
        {
            throw IoTDBConnectionException(e.what());
        }
    }
}

void Session::testInsertRecord(string deviceId, int64_t time, vector<string>& measurements, vector<string>& values) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSInsertRecordReq> req(new TSInsertRecordReq());
    req->__set_sessionId(sessionId);
    req->__set_deviceId(deviceId);
    req->__set_timestamp(time);
    req->__set_measurements(measurements);
    req->__set_values(values);
    shared_ptr<TSStatus> resp(new TSStatus());
    try
    {
        client->testInsertRecord(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::testInsertTablet(Tablet& tablet) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSInsertTabletReq> request(new TSInsertTabletReq());
    request->__set_sessionId(sessionId);
    request->deviceId = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema : tablet.schemas) {
        request->measurements.push_back(schema.first);
        request->types.push_back(schema.second);
    }
    request->__set_timestamps(SessionUtils::getTime(tablet));
    request->__set_values(SessionUtils::getValue(tablet));
    request->__set_size(tablet.rowSize);

    try
    {
        shared_ptr<TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
        client->testInsertTablet(*resp, *request);
        RpcUtils::verifySuccess(resp->statusList);
    }
    catch (IoTDBConnectionException& e)
    {
        throw new IoTDBConnectionException(e.what());
    }
}

void Session::testInsertRecords(vector<string>& deviceIds, vector<int64_t>& times, vector<vector<string>>& measurementsList, vector<vector<string>>& valuesList) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error error("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(error);
    }
    shared_ptr<TSInsertRecordsReq> request(new TSInsertRecordsReq());
    request->__set_sessionId(sessionId);
    request->__set_deviceIds(deviceIds);
    request->__set_timestamps(times);
    request->__set_measurementsList(measurementsList);
    request->__set_valuesList(valuesList);

    try
    {
        shared_ptr< TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
        client->testInsertRecords(*resp, *request);
        RpcUtils::verifySuccess(resp->statusList);
    }
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteTimeseries(string path)
{
    vector<string> paths;
    paths.push_back(path);
    deleteTimeseries(paths);
}

void Session::deleteTimeseries(vector<string>& paths)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSStatus> resp(new TSStatus());
    try
    {
        client->deleteTimeseries(*resp, sessionId, paths);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteData(string path,  int64_t time)
{
    vector<string> paths;
    paths.push_back(path);
    deleteData(paths, time);
}

void Session::deleteData(vector<string>& deviceId, int64_t time)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSDeleteDataReq> req(new TSDeleteDataReq());
    req->__set_sessionId(sessionId);
    req->__set_paths(deviceId);
    req->__set_timestamp(time);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteData(*resp,*req);
        RpcUtils::verifySuccess(*resp);
    } 
    catch (exception& e) 
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::setStorageGroup(string storageGroupId)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->setStorageGroup(*resp,sessionId, storageGroupId);
        RpcUtils::verifySuccess(*resp);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteStorageGroup(string storageGroup)
{
    vector<string> storageGroups;
    storageGroups.push_back(storageGroup);
    deleteStorageGroups(storageGroups);
}

void Session::deleteStorageGroups(vector<string>& storageGroups)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->deleteStorageGroups(*resp, sessionId, storageGroups);
        RpcUtils::verifySuccess(*resp);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createTimeseries(string path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding, CompressionType::CompressionType compressor) {
    try
    {
        createTimeseries(path, dataType, encoding, compressor, NULL, NULL, NULL, "");
    }
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createTimeseries(string path, TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding, CompressionType::CompressionType compressor,
    map<string, string>* props, map<string, string>* tags, map<string, string>* attributes, string measurementAlias)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSCreateTimeseriesReq> req(new TSCreateTimeseriesReq());
    req->__set_sessionId(sessionId);
    req->__set_path(path);
    req->__set_dataType(dataType);
    req->__set_encoding(encoding);
    req->__set_compressor(compressor);
    if (props != NULL) {
        req->__set_props(*props);
    }
    
    if (tags != NULL) {
        req->__set_tags(*tags);
    }
    if (attributes != NULL) {
        req->__set_attributes(*attributes);
    }
    if (measurementAlias != "") {
        req->__set_measurementAlias(measurementAlias);
    }

    shared_ptr<TSStatus> resp(new TSStatus());
    try 
    {
        client->createTimeseries(*resp,*req);
        RpcUtils::verifySuccess(*resp);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createMultiTimeseries(vector<string> paths, vector<TSDataType::TSDataType> dataTypes, vector<TSEncoding::TSEncoding> encodings, vector<CompressionType::CompressionType> compressors,
    vector<map<string, string>>* propsList, vector<map<string, string>>* tagsList, vector<map<string, string>>* attributesList, vector<string>* measurementAliasList) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSCreateMultiTimeseriesReq> request(new TSCreateMultiTimeseriesReq());
    request->__set_sessionId(sessionId);
    request->__set_paths(paths);

    vector<int> dataTypesOrdinal;
    for (TSDataType::TSDataType dataType : dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request->__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    for (TSEncoding::TSEncoding encoding : encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request->__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request->__set_compressors(compressorsOrdinal);

    if (propsList != NULL) {
        request->__set_propsList(*propsList);
    }
    
    if (tagsList != NULL) {
        request->__set_tagsList(*tagsList);
    }
    if (attributesList != NULL) {
        request->__set_attributesList(*attributesList);
    }
    if (measurementAliasList != NULL) {
        request->__set_measurementAliasList(*measurementAliasList);
    }

    try
    {
        shared_ptr< TSExecuteBatchStatementResp> resp(new TSExecuteBatchStatementResp());
        client->createMultiTimeseries(*resp, *request);
        RpcUtils::verifySuccess(resp->statusList);
    }
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
}

bool Session::checkTimeseriesExists(string path) {
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    try {
        string sql = "SHOW TIMESERIES " + path;
        return executeQueryStatement(sql)->hasNext();
    }
    catch (exception e) {
        throw IoTDBConnectionException(e.what());
    }
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
        client->getTimeZone(*resp, sessionId);
        RpcUtils::verifySuccess(resp->status);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
    return resp->timeZone;
}

void Session::setTimeZone(string zoneId)
{
    shared_ptr<TSSetTimeZoneReq> req(new TSSetTimeZoneReq());
    req->__set_sessionId(sessionId);
    req->__set_timeZone(zoneId);
    shared_ptr<TSStatus> resp(new TSStatus());
    try
    {
        client->setTimeZone(*resp,*req);
    } 
    catch (IoTDBConnectionException& e)
    {
        throw IoTDBConnectionException(e.what());
    }
    RpcUtils::verifySuccess(*resp);
    this->zoneId = zoneId;
}

SessionDataSet* Session::executeQueryStatement(string sql)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionId(sessionId);
    req->__set_statementId(statementId);
    req->__set_statement(sql);
    req->__set_fetchSize(fetchSize);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try
    {
        client->executeStatement(*resp,*req);
        RpcUtils::verifySuccess(resp->status);
    } 
    catch (IoTDBConnectionException e)
    {
        throw IoTDBConnectionException(e.what());
    }
    shared_ptr<TSQueryDataSet> queryDataSet(new TSQueryDataSet(resp->queryDataSet));
    return new SessionDataSet(sql, resp->columns, resp->dataTypeList, resp->queryId, client, sessionId, queryDataSet);
}

void Session::executeNonQueryStatement(string sql)
{
    lock_guard<std::mutex> mtx_locker(sessionMutex);
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionId(sessionId);
    req->__set_statementId(statementId);
    req->__set_statement(sql);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try
    {
        client->executeUpdateStatement(*resp,*req);
        RpcUtils::verifySuccess(resp->status);
    }
    catch (IoTDBConnectionException e)
    {
        throw IoTDBConnectionException(e.what());
    }
}