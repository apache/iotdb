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

#include "Session.h"
#include <algorithm>
#include <memory>

using namespace std;

TSDataType::TSDataType getTSDataTypeFromString(const string &str) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NULLTYPE
    if (str == "BOOLEAN") return TSDataType::BOOLEAN;
    else if (str == "INT32") return TSDataType::INT32;
    else if (str == "INT64") return TSDataType::INT64;
    else if (str == "FLOAT") return TSDataType::FLOAT;
    else if (str == "DOUBLE") return TSDataType::DOUBLE;
    else if (str == "TEXT") return TSDataType::TEXT;
    else if (str == "NULLTYPE") return TSDataType::NULLTYPE;
    return TSDataType::TEXT;
}

void RpcUtils::verifySuccess(const TSStatus &status) {
    if (status.code == TSStatusCode::MULTIPLE_ERROR) {
        verifySuccess(status.subStatus);
        return;
    }
    if (status.code != TSStatusCode::SUCCESS_STATUS) {
        throw IoTDBConnectionException(to_string(status.code) + ": " + status.message);
    }
}

void RpcUtils::verifySuccess(const vector<TSStatus> &statuses) {
    for (const TSStatus &status: statuses) {
        if (status.code != TSStatusCode::SUCCESS_STATUS) {
            throw BatchExecutionException(statuses, status.message);
        }
    }
}

TSStatus RpcUtils::getStatus(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status;
    status.__set_code(tsStatusCode);
    return status;
}

TSStatus RpcUtils::getStatus(int code, const string &message) {
    TSStatus status;
    status.__set_code(code);
    status.__set_message(message);
    return status;
}

shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSExecuteStatementResp(status);
}

shared_ptr<TSExecuteStatementResp>
RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, const string &message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
}

shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(const TSStatus &status) {
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    TSStatus tsStatus(status);
    resp->status = tsStatus;
    return resp;
}

shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp>
RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, const string &appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(const TSStatus &status) {
    shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
    TSStatus tsStatus(status);
    resp->__set_status(tsStatus);
    return resp;
}

void Tablet::reset() {
    rowSize = 0;
    for (int i = 0; i < schemas.size(); i++) {
        BitMap *bitMap = bitMaps[i].get();
        bitMap->reset();
    }
}

void Tablet::createColumns() {
    // create timestamp column
    timestamps.resize(maxRowNumber);
    // create value columns
    values.resize(schemas.size());
    for (size_t i = 0; i < schemas.size(); i++) {
        values[i].resize(maxRowNumber);
    }
}

int Tablet::getTimeBytesSize() {
    return rowSize * 8;
}

int Tablet::getValueByteSize() {
    int valueOccupation = 0;
    for (size_t i = 0; i < schemas.size(); i++) {
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
                for (const string &value: values[i]) {
                    valueOccupation += value.size();
                }
                break;
            default:
                throw UnSupportedDataTypeException(
                        string("Data type ") + to_string(schemas[i].second) + " is not supported.");
        }
    }
    return valueOccupation;
}

void Tablet::setAligned(bool isAligned) {
    this->isAligned = isAligned;
}

string SessionUtils::getTime(const Tablet &tablet) {
    MyStringBuffer timeBuffer;
    for (int i = 0; i < tablet.rowSize; i++) {
        timeBuffer.putLong(tablet.timestamps[i]);
    }
    return timeBuffer.str;
}

string SessionUtils::getValue(const Tablet &tablet) {
    MyStringBuffer valueBuffer;
    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        TSDataType::TSDataType dataType = tablet.schemas[i].second;
        BitMap *bitMap = tablet.bitMaps[i].get();
        switch (dataType) {
            case TSDataType::BOOLEAN:
                for (int index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap->isMarked(index)) {
                        valueBuffer.putBool(tablet.values[i][index] == "true");
                    } else {
                        valueBuffer.putBool(false);
                    }
                }
                break;
            case TSDataType::INT32:
                for (int index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap->isMarked(index)) {
                        valueBuffer.putInt(stoi(tablet.values[i][index]));
                    } else {
                        valueBuffer.putInt((numeric_limits<int>::min)());
                    }
                }
                break;
            case TSDataType::INT64:
                for (int index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap->isMarked(index)) {
                        valueBuffer.putLong(stol(tablet.values[i][index]));
                    } else {
                        valueBuffer.putLong((numeric_limits<int64_t>::min)());
                    }
                }
                break;
            case TSDataType::FLOAT:
                for (int index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap->isMarked(index)) {
                        valueBuffer.putFloat(stof(tablet.values[i][index]));
                    } else {
                        valueBuffer.putFloat((numeric_limits<float>::min)());
                    }
                }
                break;
            case TSDataType::DOUBLE:
                for (int index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap->isMarked(index)) {
                        valueBuffer.putDouble(stod(tablet.values[i][index]));
                    } else {
                        valueBuffer.putDouble((numeric_limits<double>::min)());
                    }
                }
                break;
            case TSDataType::TEXT:
                for (int index = 0; index < tablet.rowSize; index++) {
                    valueBuffer.putString(tablet.values[i][index]);
                }
                break;
            default:
                throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
    }
    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        BitMap *bitMap = tablet.bitMaps[i].get();
        bool columnHasNull = !bitMap->isAllUnmarked();
        valueBuffer.putChar(columnHasNull ? (char) 1 : (char) 0);
        if (columnHasNull) {
            vector<char> bytes = bitMap->getByteArray();
            for (char byte: bytes) {
                valueBuffer.putChar(byte);
            }
        }
    }
    return valueBuffer.str;
}

int SessionDataSet::getBatchSize() {
    return batchSize;
}

void SessionDataSet::setBatchSize(int batchSize) {
    this->batchSize = batchSize;
}

vector<string> SessionDataSet::getColumnNames() { return this->columnNameList; }

bool SessionDataSet::hasNext() {
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
            } else {
                tsQueryDataSet = make_shared<TSQueryDataSet>(resp->queryDataSet);
                tsQueryDataSetTimeBuffer.str = tsQueryDataSet->time;
                rowsIndex = 0;
            }
        }
        catch (IoTDBConnectionException &e) {
            throw IoTDBConnectionException(
                    string("Cannot fetch result from server, because of network connection: ") + e.what());
        }
    }

    constructOneRow();
    hasCachedRecord = true;
    return true;
}

void SessionDataSet::constructOneRow() {
    vector<Field> outFields;
    int loc = 0;
    for (int i = 0; i < columnSize; i++) {
        Field field;
        if (duplicateLocation.find(i) != duplicateLocation.end()) {
            field = outFields[duplicateLocation[i]];
        } else {
            MyStringBuffer *bitmapBuffer = bitmapBuffers[loc].get();
            // another new 8 row, should move the bitmap buffer position to next byte
            if (rowsIndex % 8 == 0) {
                currentBitmap[loc] = bitmapBuffer->getChar();
            }

            if (!isNull(loc, rowsIndex)) {
                MyStringBuffer *valueBuffer = valueBuffers[loc].get();
                TSDataType::TSDataType dataType = getTSDataTypeFromString(columnTypeDeduplicatedList[loc]);
                field.dataType = dataType;
                switch (dataType) {
                    case TSDataType::BOOLEAN: {
                        bool booleanValue = valueBuffer->getBool();
                        field.boolV = booleanValue;
                        break;
                    }
                    case TSDataType::INT32: {
                        int intValue = valueBuffer->getInt();
                        field.intV = intValue;
                        break;
                    }
                    case TSDataType::INT64: {
                        int64_t longValue = valueBuffer->getLong();
                        field.longV = longValue;
                        break;
                    }
                    case TSDataType::FLOAT: {
                        float floatValue = valueBuffer->getFloat();
                        field.floatV = floatValue;
                        break;
                    }
                    case TSDataType::DOUBLE: {
                        double doubleValue = valueBuffer->getDouble();
                        field.doubleV = doubleValue;
                        break;
                    }
                    case TSDataType::TEXT: {
                        string stringValue = valueBuffer->getString();
                        field.stringV = stringValue;
                        break;
                    }
                    default: {
                        throw UnSupportedDataTypeException(
                                string("Data type ") + columnTypeDeduplicatedList[i] + " is not supported.");
                    }
                }
            } else {
                field.dataType = TSDataType::NULLTYPE;
            }
            loc++;
        }
        outFields.push_back(field);
    }

    if (!this->isIgnoreTimeStamp) {
        rowRecord = RowRecord(tsQueryDataSetTimeBuffer.getLong(), outFields);
    } else {
        tsQueryDataSetTimeBuffer.getLong();
        rowRecord = RowRecord(outFields);
    }
    rowsIndex++;
}

bool SessionDataSet::isNull(int index, int rowNum) {
    char bitmap = currentBitmap[index];
    int shift = rowNum % 8;
    return ((flag >> shift) & bitmap) == 0;
}

RowRecord *SessionDataSet::next() {
    if (!hasCachedRecord) {
        if (!hasNext()) {
            return nullptr;
        }
    }

    hasCachedRecord = false;
    return &rowRecord;
}

void SessionDataSet::closeOperationHandle() {
    shared_ptr<TSCloseOperationReq> closeReq(new TSCloseOperationReq());
    closeReq->__set_sessionId(sessionId);
    closeReq->__set_statementId(statementId);
    closeReq->__set_queryId(queryId);
    shared_ptr<TSStatus> closeResp(new TSStatus());
    try {
        client->closeOperation(*closeResp, *closeReq);
        RpcUtils::verifySuccess(*closeResp);
    }
    catch (IoTDBConnectionException &e) {
        throw IoTDBConnectionException(
                string("Error occurs when connecting to server for close operation, because: ") + e.what());
    }
}

string MeasurementNode::serialize() const {
    MyStringBuffer buffer;
    buffer.putString(getName());
    buffer.putChar(getDataType());
    buffer.putChar(getEncoding());
    buffer.putChar(getCompressionType());
    return buffer.str;
}

string Template::serialize() const {
    MyStringBuffer buffer;
    stack<pair<string, shared_ptr<TemplateNode>>> stack;
    unordered_set<string> alignedPrefix;
    buffer.putString(getName());
    buffer.putBool(isAligned());
    if (isAligned()) {
        alignedPrefix.emplace("");
    }

    for (const auto &child: children_) {
        stack.push(make_pair("", child.second));
    }

    while (!stack.empty()) {
        auto cur = stack.top();
        stack.pop();

        string prefix = cur.first;
        shared_ptr<TemplateNode> cur_node_ptr = cur.second;
        string fullPath(prefix);

        if (!cur_node_ptr->isMeasurement()) {
            if (!prefix.empty()) {
                fullPath.append(".");
            }
            fullPath.append(cur_node_ptr->getName());
            if (cur_node_ptr->isAligned()) {
                alignedPrefix.emplace(fullPath);
            }
            for (const auto &child: cur_node_ptr->getChildren()) {
                stack.push(make_pair(fullPath, child.second));
            }
        } else {
            buffer.putString(prefix);
            buffer.putBool(alignedPrefix.find(prefix) != alignedPrefix.end());
            buffer.concat(cur_node_ptr->serialize());
        }
    }

    return buffer.str;
}

/**
 * When delete variable, make sure release all resource.
 */
Session::~Session() {
    close();
}

/**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
bool Session::checkSorted(const Tablet &tablet) {
    for (int i = 1; i < tablet.rowSize; i++) {
        if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
            return false;
        }
    }
    return true;
}

bool Session::checkSorted(const vector<int64_t> &times) {
    for (size_t i = 1; i < times.size(); i++) {
        if (times[i] < times[i - 1]) {
            return false;
        }
    }
    return true;
}

void Session::sortTablet(Tablet &tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    int *index = new int[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
        index[i] = i;
    }

    this->sortIndexByTimestamp(index, tablet.timestamps, tablet.rowSize);
    tablet.timestamps = sortList(tablet.timestamps, index, tablet.rowSize);
    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        tablet.values[i] = sortList(tablet.values[i], index, tablet.rowSize);
    }

    delete[] index;
}

void Session::sortIndexByTimestamp(int *index, std::vector<int64_t> &timestamps, int length) {
    if (length <= 1) {
        return;
    }

    TsCompare tsCompareObj(timestamps);
    std::sort(&index[0], &index[length], tsCompareObj);
}

/**
 * Append value into buffer in Big Endian order to comply with IoTDB server
 */
void Session::appendValues(string &buffer, const char *value, int size) {
    static bool hasCheckedEndianFlag = false;
    static bool localCpuIsBigEndian = false;
    if (!hasCheckedEndianFlag) {
        hasCheckedEndianFlag = true;
        int chk = 0x0201;  //used to distinguish CPU's type (BigEndian or LittleEndian)
        localCpuIsBigEndian = (0x01 != *(char *) (&chk));
    }

    if (localCpuIsBigEndian) {
        buffer.append(value, size);
    } else {
        for (int i = size - 1; i >= 0; i--) {
            buffer.append(value + i, 1);
        }
    }
}

void
Session::putValuesIntoBuffer(const vector<TSDataType::TSDataType> &types, const vector<char *> &values, string &buf) {
    for (size_t i = 0; i < values.size(); i++) {
        int8_t typeNum = getDataTypeNumber(types[i]);
        buf.append((char *) (&typeNum), sizeof(int8_t));
        switch (types[i]) {
            case TSDataType::BOOLEAN:
                buf.append(values[i], 1);
                break;
            case TSDataType::INT32:
                appendValues(buf, values[i], sizeof(int32_t));
                break;
            case TSDataType::INT64:
                appendValues(buf, values[i], sizeof(int64_t));
                break;
            case TSDataType::FLOAT:
                appendValues(buf, values[i], sizeof(float));
                break;
            case TSDataType::DOUBLE:
                appendValues(buf, values[i], sizeof(double));
                break;
            case TSDataType::TEXT: {
                int len = strlen(values[i]);
                appendValues(buf, (char *) (&len), sizeof(int));
                // no need to change the byte order of string value
                buf.append(values[i], len);
                break;
            }
            case TSDataType::NULLTYPE:
                break;
        }
    }
}

int8_t Session::getDataTypeNumber(TSDataType::TSDataType type) {
    switch (type) {
        case TSDataType::BOOLEAN:
            return 0;
        case TSDataType::INT32:
            return 1;
        case TSDataType::INT64:
            return 2;
        case TSDataType::FLOAT:
            return 3;
        case TSDataType::DOUBLE:
            return 4;
        case TSDataType::TEXT:
            return 5;
        default:
            return -1;
    }
}

string Session::getVersionString(Version::Version version) {
    switch (version) {
        case Version::V_0_12:
            return "V_0_12";
        case Version::V_0_13:
            return "V_0_13";
        default:
            return "V_0_12";
    }
}

void Session::open() {
    open(false, DEFAULT_TIMEOUT_MS);
}

void Session::open(bool enableRPCCompression) {
    open(enableRPCCompression, DEFAULT_TIMEOUT_MS);
}

void Session::open(bool enableRPCCompression, int connectionTimeoutInMs) {
    if (!isClosed) {
        return;
    }

    shared_ptr<TSocket> socket(new TSocket(host, rpcPort));
    transport = std::make_shared<TFramedTransport>(socket);
    socket->setConnTimeout(connectionTimeoutInMs);
    if (!transport->isOpen()) {
        try {
            transport->open();
        }
        catch (TTransportException &e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    if (enableRPCCompression) {
        shared_ptr<TCompactProtocol> protocol(new TCompactProtocol(transport));
        client = std::make_shared<IClientRPCServiceClient>(protocol);
    } else {
        shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
        client = std::make_shared<IClientRPCServiceClient>(protocol);
    }

    std::map<std::string, std::string> configuration;
    configuration["version"] = getVersionString(version);

    TSOpenSessionReq openReq;
    openReq.__set_username(username);
    openReq.__set_password(password);
    openReq.__set_zoneId(zoneId);
    openReq.__set_configuration(configuration);

    try {
        TSOpenSessionResp openResp;
        client->openSession(openResp, openReq);
        RpcUtils::verifySuccess(openResp.status);
        if (protocolVersion != openResp.serverProtocolVersion) {
            if (openResp.serverProtocolVersion == 0) {// less than 0.10
                throw logic_error(string("Protocol not supported, Client version is ") + to_string(protocolVersion) +
                                  ", but Server version is " + to_string(openResp.serverProtocolVersion));
            }
        }

        sessionId = openResp.sessionId;
        statementId = client->requestStatementId(sessionId);

        if (!zoneId.empty()) {
            setTimeZone(zoneId);
        } else {
            zoneId = getTimeZone();
        }
    }
    catch (exception &e) {
        transport->close();
        throw IoTDBConnectionException(e.what());
    }

    isClosed = false;
}


void Session::close() {
    if (isClosed) {
        return;
    }
    shared_ptr<TSCloseSessionReq> req(new TSCloseSessionReq());
    req->__set_sessionId(sessionId);
    try {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->closeSession(*resp, *req);
    }
    catch (exception &e) {
        throw IoTDBConnectionException(
                string("Error occurs when closing session at server. Maybe server is down. ") + e.what());
    }
    isClosed = true;
    if (transport != nullptr) {
        transport->close();
    }
}


void Session::insertRecord(const string &deviceId, int64_t time,
                           const vector<string> &measurements,
                           const vector<string> &values) {
    TSInsertStringRecordReq req;
    req.__set_sessionId(sessionId);
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(false);
    TSStatus respStatus;
    try {
        client->insertStringRecord(respStatus, req);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertRecord(const string &prefixPath, int64_t time,
                           const vector<string> &measurements,
                           const vector<TSDataType::TSDataType> &types,
                           const vector<char *> &values) {
    TSInsertRecordReq req;
    req.__set_sessionId(sessionId);
    req.__set_prefixPath(prefixPath);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    string buffer;
    putValuesIntoBuffer(types, values, buffer);
    req.__set_values(buffer);
    req.__set_isAligned(false);
    TSStatus respStatus;
    try {
        client->insertRecord(respStatus, req);
        RpcUtils::verifySuccess(respStatus);
    } catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedRecord(const string &deviceId, int64_t time,
                                  const vector<string> &measurements,
                                  const vector<string> &values) {
    TSInsertStringRecordReq req;
    req.__set_sessionId(sessionId);
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(true);
    TSStatus respStatus;
    try {
        client->insertStringRecord(respStatus, req);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedRecord(const string &prefixPath, int64_t time,
                                  const vector<string> &measurements,
                                  const vector<TSDataType::TSDataType> &types,
                                  const vector<char *> &values) {
    TSInsertRecordReq req;
    req.__set_sessionId(sessionId);
    req.__set_prefixPath(prefixPath);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    string buffer;
    putValuesIntoBuffer(types, values, buffer);
    req.__set_values(buffer);
    req.__set_isAligned(true);
    TSStatus respStatus;
    try {
        client->insertRecord(respStatus, req);
        RpcUtils::verifySuccess(respStatus);
    } catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertRecords(const vector<string> &deviceIds,
                            const vector<int64_t> &times,
                            const vector<vector<string>> &measurementsList,
                            const vector<vector<string>> &valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }
    TSInsertStringRecordsReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPaths(deviceIds);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    request.__set_valuesList(valuesList);
    request.__set_isAligned(false);

    try {
        TSStatus respStatus;
        client->insertStringRecords(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertRecords(const vector<string> &deviceIds,
                            const vector<int64_t> &times,
                            const vector<vector<string>> &measurementsList,
                            const vector<vector<TSDataType::TSDataType>> &typesList,
                            const vector<vector<char *>> &valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }
    TSInsertRecordsReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPaths(deviceIds);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    vector<string> bufferList;
    for (size_t i = 0; i < valuesList.size(); i++) {
        string buffer;
        putValuesIntoBuffer(typesList[i], valuesList[i], buffer);
        bufferList.push_back(buffer);
    }
    request.__set_valuesList(bufferList);
    request.__set_isAligned(false);

    try {
        TSStatus respStatus;
        client->insertRecords(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    } catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedRecords(const vector<string> &deviceIds,
                                   const vector<int64_t> &times,
                                   const vector<vector<string>> &measurementsList,
                                   const vector<vector<string>> &valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }
    TSInsertStringRecordsReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPaths(deviceIds);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    request.__set_valuesList(valuesList);
    request.__set_isAligned(true);

    try {
        TSStatus respStatus;
        client->insertStringRecords(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedRecords(const vector<string> &deviceIds,
                                   const vector<int64_t> &times,
                                   const vector<vector<string>> &measurementsList,
                                   const vector<vector<TSDataType::TSDataType>> &typesList,
                                   const vector<vector<char *>> &valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }
    TSInsertRecordsReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPaths(deviceIds);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    vector<string> bufferList;
    for (size_t i = 0; i < valuesList.size(); i++) {
        string buffer;
        putValuesIntoBuffer(typesList[i], valuesList[i], buffer);
        bufferList.push_back(buffer);
    }
    request.__set_valuesList(bufferList);
    request.__set_isAligned(true);

    try {
        TSStatus respStatus;
        client->insertRecords(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    } catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertRecordsOfOneDevice(const string &deviceId,
                                       vector<int64_t> &times,
                                       vector<vector<string>> &measurementsList,
                                       vector<vector<TSDataType::TSDataType>> &typesList,
                                       vector<vector<char *>> &valuesList) {
    insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
}

void Session::insertRecordsOfOneDevice(const string &deviceId,
                                       vector<int64_t> &times,
                                       vector<vector<string>> &measurementsList,
                                       vector<vector<TSDataType::TSDataType>> &typesList,
                                       vector<vector<char *>> &valuesList,
                                       bool sorted) {

    if (sorted) {
        if (!checkSorted(times)) {
            throw BatchExecutionException("Times in InsertOneDeviceRecords are not in ascending order");
        }
    } else {
        int *index = new int[times.size()];
        for (size_t i = 0; i < times.size(); i++) {
            index[i] = i;
        }

        this->sortIndexByTimestamp(index, times, times.size());
        times = sortList(times, index, times.size());
        measurementsList = sortList(measurementsList, index, times.size());
        typesList = sortList(typesList, index, times.size());
        valuesList = sortList(valuesList, index, times.size());
        delete[] index;
    }
    TSInsertRecordsOfOneDeviceReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPath(deviceId);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    vector<string> bufferList;
    for (size_t i = 0; i < valuesList.size(); i++) {
        string buffer;
        putValuesIntoBuffer(typesList[i], valuesList[i], buffer);
        bufferList.push_back(buffer);
    }
    request.__set_valuesList(bufferList);
    request.__set_isAligned(false);

    try {
        TSStatus respStatus;
        client->insertRecordsOfOneDevice(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    } catch (const exception &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedRecordsOfOneDevice(const string &deviceId,
                                              vector<int64_t> &times,
                                              vector<vector<string>> &measurementsList,
                                              vector<vector<TSDataType::TSDataType>> &typesList,
                                              vector<vector<char *>> &valuesList) {
    insertAlignedRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
}

void Session::insertAlignedRecordsOfOneDevice(const string &deviceId,
                                              vector<int64_t> &times,
                                              vector<vector<string>> &measurementsList,
                                              vector<vector<TSDataType::TSDataType>> &typesList,
                                              vector<vector<char *>> &valuesList,
                                              bool sorted) {

    if (sorted) {
        if (!checkSorted(times)) {
            throw BatchExecutionException("Times in InsertOneDeviceRecords are not in ascending order");
        }
    } else {
        int *index = new int[times.size()];
        for (size_t i = 0; i < times.size(); i++) {
            index[i] = i;
        }

        this->sortIndexByTimestamp(index, times, times.size());
        times = sortList(times, index, times.size());
        measurementsList = sortList(measurementsList, index, times.size());
        typesList = sortList(typesList, index, times.size());
        valuesList = sortList(valuesList, index, times.size());
        delete[] index;
    }
    TSInsertRecordsOfOneDeviceReq request;
    request.__set_sessionId(sessionId);
    request.__set_prefixPath(deviceId);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    vector<string> bufferList;
    for (size_t i = 0; i < valuesList.size(); i++) {
        string buffer;
        putValuesIntoBuffer(typesList[i], valuesList[i], buffer);
        bufferList.push_back(buffer);
    }
    request.__set_valuesList(bufferList);
    request.__set_isAligned(true);

    try {
        TSStatus respStatus;
        client->insertRecordsOfOneDevice(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    } catch (const exception &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertTablet(Tablet &tablet) {
    try {
        insertTablet(tablet, false);
    }
    catch (const exception &e) {
        cout << e.what() << endl;
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablet(Tablet &tablet, bool sorted) {
    if (sorted) {
        if (!checkSorted(tablet)) {
            throw BatchExecutionException("Times in Tablet are not in ascending order");
        }
    } else {
        sortTablet(tablet);
    }

    TSInsertTabletReq request;
    request.__set_sessionId(sessionId);
    request.prefixPath = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema: tablet.schemas) {
        request.measurements.push_back(schema.first);
        request.types.push_back(schema.second);
    }
    request.__set_timestamps(SessionUtils::getTime(tablet));
    request.__set_values(SessionUtils::getValue(tablet));
    request.__set_size(tablet.rowSize);
    request.__set_isAligned(tablet.isAligned);

    try {
        TSStatus respStatus;
        client->insertTablet(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedTablet(Tablet &tablet) {
    insertAlignedTablet(tablet, false);
}

void Session::insertAlignedTablet(Tablet &tablet, bool sorted) {
    tablet.setAligned(true);
    try {
        insertTablet(tablet, sorted);
    }
    catch (const exception &e) {
        cout << e.what() << endl;
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet *> &tablets) {
    try {
        insertTablets(tablets, false);
    }
    catch (const exception &e) {
        cout << e.what() << endl;
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet *> &tablets, bool sorted) {
    TSInsertTabletsReq request;
    request.__set_sessionId(sessionId);
    if (tablets.empty()) {
        throw BatchExecutionException("No tablet is inserting!");
    }
    auto beginIter = tablets.begin();
    bool isFirstTabletAligned = ((*beginIter).second)->isAligned;
    for (const auto &item: tablets) {
        if (isFirstTabletAligned != item.second->isAligned) {
            throw BatchExecutionException("The tablets should be all aligned or non-aligned!");
        }
        if (sorted) {
            if (!checkSorted(*(item.second))) {
                throw BatchExecutionException("Times in Tablet are not in ascending order");
            }
        } else {
            sortTablet(*(item.second));
        }
        request.prefixPaths.push_back(item.second->deviceId);
        vector<string> measurements;
        vector<int> dataTypes;
        for (pair<string, TSDataType::TSDataType> schema: item.second->schemas) {
            measurements.push_back(schema.first);
            dataTypes.push_back(schema.second);
        }
        request.measurementsList.push_back(measurements);
        request.typesList.push_back(dataTypes);
        request.timestampsList.push_back(SessionUtils::getTime(*(item.second)));
        request.valuesList.push_back(SessionUtils::getValue(*(item.second)));
        request.sizeList.push_back(item.second->rowSize);
    }
    request.__set_isAligned(isFirstTabletAligned);
    try {
        TSStatus respStatus;
        client->insertTablets(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::insertAlignedTablets(unordered_map<string, Tablet *> &tablets) {
    insertAlignedTablets(tablets, false);
}

void Session::insertAlignedTablets(unordered_map<string, Tablet *> &tablets, bool sorted) {
    for (auto iter = tablets.begin(); iter != tablets.end(); iter++) {
        iter->second->setAligned(true);
    }
    try {
        insertTablets(tablets, sorted);
    }
    catch (const exception &e) {
        cout << e.what() << endl;
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::testInsertRecord(const string &deviceId, int64_t time, const vector<string> &measurements,
                               const vector<string> &values) {
    shared_ptr<TSInsertStringRecordReq> req(new TSInsertStringRecordReq());
    req->__set_sessionId(sessionId);
    req->__set_prefixPath(deviceId);
    req->__set_timestamp(time);
    req->__set_measurements(measurements);
    req->__set_values(values);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->insertStringRecord(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::testInsertTablet(const Tablet &tablet) {
    shared_ptr<TSInsertTabletReq> request(new TSInsertTabletReq());
    request->__set_sessionId(sessionId);
    request->prefixPath = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema: tablet.schemas) {
        request->measurements.push_back(schema.first);
        request->types.push_back(schema.second);
    }
    request->__set_timestamps(SessionUtils::getTime(tablet));
    request->__set_values(SessionUtils::getValue(tablet));
    request->__set_size(tablet.rowSize);

    try {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->testInsertTablet(*resp, *request);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::testInsertRecords(const vector<string> &deviceIds,
                                const vector<int64_t> &times,
                                const vector<vector<string>> &measurementsList,
                                const vector<vector<string>> &valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error error("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(error);
    }
    shared_ptr<TSInsertStringRecordsReq> request(new TSInsertStringRecordsReq());
    request->__set_sessionId(sessionId);
    request->__set_prefixPaths(deviceIds);
    request->__set_timestamps(times);
    request->__set_measurementsList(measurementsList);
    request->__set_valuesList(valuesList);

    try {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->insertStringRecords(*resp, *request);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteTimeseries(const string &path) {
    vector<string> paths;
    paths.push_back(path);
    deleteTimeseries(paths);
}

void Session::deleteTimeseries(const vector<string> &paths) {
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->deleteTimeseries(*resp, sessionId, paths);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteData(const string &path, int64_t time) {
    vector<string> paths;
    paths.push_back(path);
    deleteData(paths, time);
}

void Session::deleteData(const vector<string> &deviceId, int64_t time) {
    shared_ptr<TSDeleteDataReq> req(new TSDeleteDataReq());
    req->__set_sessionId(sessionId);
    req->__set_paths(deviceId);
    req->__set_endTime(time);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->deleteData(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (exception &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::setStorageGroup(const string &storageGroupId) {
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->setStorageGroup(*resp, sessionId, storageGroupId);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::deleteStorageGroup(const string &storageGroup) {
    vector<string> storageGroups;
    storageGroups.push_back(storageGroup);
    deleteStorageGroups(storageGroups);
}

void Session::deleteStorageGroups(const vector<string> &storageGroups) {
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->deleteStorageGroups(*resp, sessionId, storageGroups);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createTimeseries(const string &path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor) {
    try {
        createTimeseries(path, dataType, encoding, compressor, nullptr, nullptr, nullptr, "");
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

// TODO:
void Session::createTimeseries(const string &path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor,
                               map<string, string> *props,
                               map<string, string> *tags,
                               map<string, string> *attributes,
                               const string &measurementAlias) {
    shared_ptr<TSCreateTimeseriesReq> req(new TSCreateTimeseriesReq());
    req->__set_sessionId(sessionId);
    req->__set_path(path);
    req->__set_dataType(dataType);
    req->__set_encoding(encoding);
    req->__set_compressor(compressor);
    if (props != nullptr) {
        req->__set_props(*props);
    }

    if (tags != nullptr) {
        req->__set_tags(*tags);
    }
    if (attributes != nullptr) {
        req->__set_attributes(*attributes);
    }
    if (!measurementAlias.empty()) {
        req->__set_measurementAlias(measurementAlias);
    }

    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->createTimeseries(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createMultiTimeseries(const vector<string> &paths,
                                    const vector<TSDataType::TSDataType> &dataTypes,
                                    const vector<TSEncoding::TSEncoding> &encodings,
                                    const vector<CompressionType::CompressionType> &compressors,
                                    vector<map<string, string>> *propsList,
                                    vector<map<string, string>> *tagsList,
                                    vector<map<string, string>> *attributesList,
                                    vector<string> *measurementAliasList) {
    shared_ptr<TSCreateMultiTimeseriesReq> request(new TSCreateMultiTimeseriesReq());
    request->__set_sessionId(sessionId);
    request->__set_paths(paths);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request->__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request->__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request->__set_compressors(compressorsOrdinal);

    if (propsList != nullptr) {
        request->__set_propsList(*propsList);
    }

    if (tagsList != nullptr) {
        request->__set_tagsList(*tagsList);
    }
    if (attributesList != nullptr) {
        request->__set_attributesList(*attributesList);
    }
    if (measurementAliasList != nullptr) {
        request->__set_measurementAliasList(*measurementAliasList);
    }

    try {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->createMultiTimeseries(*resp, *request);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createAlignedTimeseries(const std::string &deviceId,
                                      const std::vector<std::string> &measurements,
                                      const std::vector<TSDataType::TSDataType> &dataTypes,
                                      const std::vector<TSEncoding::TSEncoding> &encodings,
                                      const std::vector<CompressionType::CompressionType> &compressors) {
    shared_ptr<TSCreateAlignedTimeseriesReq> request(new TSCreateAlignedTimeseriesReq());
    request->__set_sessionId(sessionId);
    request->__set_prefixPath(deviceId);
    request->__set_measurements(measurements);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request->__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request->__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request->__set_compressors(compressorsOrdinal);

    try {
        shared_ptr<TSStatus> resp(new TSStatus());
        client->createAlignedTimeseries(*resp, *request);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

bool Session::checkTimeseriesExists(const string &path) {
    try {
        std::unique_ptr<SessionDataSet> dataset = executeQueryStatement("SHOW TIMESERIES " + path);
        bool isExisted = dataset->hasNext();
        dataset->closeOperationHandle();
        return isExisted;
    }
    catch (exception &e) {
        std::cout << e.what() << std::endl;
        throw IoTDBConnectionException(e.what());
    }
}

string Session::getTimeZone() {
    if (!zoneId.empty()) {
        return zoneId;
    }
    shared_ptr<TSGetTimeZoneResp> resp(new TSGetTimeZoneResp());
    try {
        client->getTimeZone(*resp, sessionId);
        RpcUtils::verifySuccess(resp->status);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->timeZone;
}

void Session::setTimeZone(const string &zoneId) {
    shared_ptr<TSSetTimeZoneReq> req(new TSSetTimeZoneReq());
    req->__set_sessionId(sessionId);
    req->__set_timeZone(zoneId);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->setTimeZone(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    RpcUtils::verifySuccess(*resp);
    this->zoneId = zoneId;
}

unique_ptr<SessionDataSet> Session::executeQueryStatement(const string &sql) {
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionId(sessionId);
    req->__set_statementId(statementId);
    req->__set_statement(sql);
    req->__set_fetchSize(fetchSize);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try {
        client->executeStatement(*resp, *req);
        RpcUtils::verifySuccess(resp->status);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    shared_ptr<TSQueryDataSet> queryDataSet(new TSQueryDataSet(resp->queryDataSet));
    return unique_ptr<SessionDataSet>(new SessionDataSet(
            sql, resp->columns, resp->dataTypeList, resp->columnNameIndexMap, resp->ignoreTimeStamp, resp->queryId,
            statementId, client, sessionId, queryDataSet));
}

void Session::executeNonQueryStatement(const string &sql) {
    shared_ptr<TSExecuteStatementReq> req(new TSExecuteStatementReq());
    req->__set_sessionId(sessionId);
    req->__set_statementId(statementId);
    req->__set_statement(sql);
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    try {
        client->executeUpdateStatement(*resp, *req);
        RpcUtils::verifySuccess(resp->status);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::createSchemaTemplate(const Template &templ) {
    shared_ptr<TSCreateSchemaTemplateReq> req(new TSCreateSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(templ.getName());
    req->__set_serializedTemplate(templ.serialize());
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->createSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::setSchemaTemplate(const string &template_name, const string &prefix_path) {
    shared_ptr<TSSetSchemaTemplateReq> req(new TSSetSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_templateName(template_name);
    req->__set_prefixPath(prefix_path);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->setSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::unsetSchemaTemplate(const string &prefix_path, const string &template_name) {
    shared_ptr<TSUnsetSchemaTemplateReq> req(new TSUnsetSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_templateName(template_name);
    req->__set_prefixPath(prefix_path);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->unsetSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::addAlignedMeasurementsInTemplate(const string &template_name, const vector<std::string> &measurements,
                                               const vector<TSDataType::TSDataType> &dataTypes,
                                               const vector<TSEncoding::TSEncoding> &encodings,
                                               const vector<CompressionType::CompressionType> &compressors) {
    shared_ptr<TSAppendSchemaTemplateReq> req(new TSAppendSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurements(measurements);
    req->__set_isAligned(true);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req->__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req->__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req->__set_compressors(compressorsOrdinal);

    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->appendSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::addAlignedMeasurementsInTemplate(const string &template_name, const string &measurement,
                                               TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                                               CompressionType::CompressionType compressor) {
    vector<std::string> measurements(1, measurement);
    vector<TSDataType::TSDataType> dataTypes(1, dataType);
    vector<TSEncoding::TSEncoding> encodings(1, encoding);
    vector<CompressionType::CompressionType> compressors(1, compressor);
    addAlignedMeasurementsInTemplate(template_name, measurements, dataTypes, encodings, compressors);
}

void Session::addUnalignedMeasurementsInTemplate(const string &template_name, const vector<std::string> &measurements,
                                                 const vector<TSDataType::TSDataType> &dataTypes,
                                                 const vector<TSEncoding::TSEncoding> &encodings,
                                                 const vector<CompressionType::CompressionType> &compressors) {
    shared_ptr<TSAppendSchemaTemplateReq> req(new TSAppendSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurements(measurements);
    req->__set_isAligned(false);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req->__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req->__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req->__set_compressors(compressorsOrdinal);

    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->appendSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

void Session::addUnalignedMeasurementsInTemplate(const string &template_name, const string &measurement,
                                                 TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                                                 CompressionType::CompressionType compressor) {
    vector<std::string> measurements(1, measurement);
    vector<TSDataType::TSDataType> dataTypes(1, dataType);
    vector<TSEncoding::TSEncoding> encodings(1, encoding);
    vector<CompressionType::CompressionType> compressors(1, compressor);
    addUnalignedMeasurementsInTemplate(template_name, measurements, dataTypes, encodings, compressors);
}

void Session::deleteNodeInTemplate(const string &template_name, const string &path) {
    shared_ptr<TSPruneSchemaTemplateReq> req(new TSPruneSchemaTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_path(path);
    shared_ptr<TSStatus> resp(new TSStatus());
    try {
        client->pruneSchemaTemplate(*resp, *req);
        RpcUtils::verifySuccess(*resp);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
}

int Session::countMeasurementsInTemplate(const string &template_name) {
    shared_ptr<TSQueryTemplateReq> req(new TSQueryTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_queryType(TemplateQueryType::COUNT_MEASUREMENTS);
    shared_ptr<TSQueryTemplateResp> resp(new TSQueryTemplateResp());
    try {
        client->querySchemaTemplate(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->count;
}

bool Session::isMeasurementInTemplate(const string &template_name, const string &path) {
    shared_ptr<TSQueryTemplateReq> req(new TSQueryTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurement(path);
    req->__set_queryType(TemplateQueryType::IS_MEASUREMENT);
    shared_ptr<TSQueryTemplateResp> resp(new TSQueryTemplateResp());
    try {
        client->querySchemaTemplate(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->result;
}

bool Session::isPathExistInTemplate(const string &template_name, const string &path) {
    shared_ptr<TSQueryTemplateReq> req(new TSQueryTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurement(path);
    req->__set_queryType(TemplateQueryType::PATH_EXIST);
    shared_ptr<TSQueryTemplateResp> resp(new TSQueryTemplateResp());
    try {
        client->querySchemaTemplate(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->result;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string &template_name) {
    shared_ptr<TSQueryTemplateReq> req(new TSQueryTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurement("");
    req->__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    shared_ptr<TSQueryTemplateResp> resp(new TSQueryTemplateResp());
    try {
        client->querySchemaTemplate(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->measurements;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string &template_name, const string &pattern) {
    shared_ptr<TSQueryTemplateReq> req(new TSQueryTemplateReq());
    req->__set_sessionId(sessionId);
    req->__set_name(template_name);
    req->__set_measurement(pattern);
    req->__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    shared_ptr<TSQueryTemplateResp> resp(new TSQueryTemplateResp());
    try {
        client->querySchemaTemplate(*resp, *req);
    }
    catch (IoTDBConnectionException &e) {
        cout << e.what() << endl;
        throw IoTDBConnectionException(e.what());
    }
    return resp->measurements;
}
