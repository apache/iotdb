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
#include <time.h>
#include <future>
#include "NodesSupplier.h"

using namespace std;

/**
* Timeout of query can be set by users.
* A negative number means using the default configuration of server.
* And value 0 will disable the function of query timeout.
*/
static const int64_t QUERY_TIMEOUT_MS = -1;

LogLevelType LOG_LEVEL = LEVEL_DEBUG;

TSDataType::TSDataType getTSDataTypeFromString(const string &str) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, TIMESTAMP, DATE, NULLTYPE
    if (str == "BOOLEAN") return TSDataType::BOOLEAN;
    else if (str == "INT32" || str == "DATE") return TSDataType::INT32;
    else if (str == "INT64" || str == "TIMESTAMP") return TSDataType::INT64;
    else if (str == "FLOAT") return TSDataType::FLOAT;
    else if (str == "DOUBLE") return TSDataType::DOUBLE;
    else if (str == "TEXT" || str == "STRING" || str == "BLOB") return TSDataType::TEXT;
    else if (str == "NULLTYPE") return TSDataType::NULLTYPE;
    return TSDataType::INVALID_DATATYPE;
}

void Tablet::createColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        TSDataType::TSDataType dataType = schemas[i].second;
        switch (dataType) {
            case TSDataType::BOOLEAN:
                values[i] = new bool[maxRowNumber];
                break;
            case TSDataType::INT32:
                values[i] = new int[maxRowNumber];
                break;
            case TSDataType::INT64:
                values[i] = new int64_t[maxRowNumber];
                break;
            case TSDataType::FLOAT:
                values[i] = new float[maxRowNumber];
                break;
            case TSDataType::DOUBLE:
                values[i] = new double[maxRowNumber];
                break;
            case TSDataType::TEXT:
                values[i] = new string[maxRowNumber];
                break;
            default:
                throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
    }
}

void Tablet::deleteColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        if (values[i]) continue;
        TSDataType::TSDataType dataType = schemas[i].second;
        switch (dataType) {
            case TSDataType::BOOLEAN: {
                bool* valueBuf = (bool*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::INT32: {
                int* valueBuf = (int*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::INT64: {
                int64_t* valueBuf = (int64_t*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::FLOAT: {
                float* valueBuf = (float*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::DOUBLE: {
                double* valueBuf = (double*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::TEXT: {
                string* valueBuf = (string*)(values[i]);
                delete[] valueBuf;
                break;
            }
            default:
                throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
        values[i] = nullptr;
    }
}

void Tablet::reset() {
    rowSize = 0;
    for (size_t i = 0; i < schemas.size(); i++) {
        bitMaps[i].reset();
    }
}

size_t Tablet::getTimeBytesSize() {
    return rowSize * 8;
}

size_t Tablet::getValueByteSize() {
    size_t valueOccupation = 0;
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
            case TSDataType::TEXT: {
                valueOccupation += rowSize * 4;
                string* valueBuf = (string*)(values[i]);
                for (size_t j = 0; j < rowSize; j++) {
                    valueOccupation += valueBuf[j].size();
                }
                break;
            }
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

std::shared_ptr<storage::IDeviceID> Tablet::getDeviceID(int row) {
    std::vector<std::string> id_array(idColumnIndexes.size() + 1);
    size_t idArrayIdx = 0;
    id_array[idArrayIdx++] = this->deviceId;
    for (auto idColumnIndex : idColumnIndexes) {
        void *strPtr = getValue(idColumnIndex, row, TSDataType::TEXT);
        if (!strPtr) {
            throw std::runtime_error("Unsupported data type: " + std::to_string(TSDataType::TEXT));
        }
        id_array[idArrayIdx++] = *static_cast<std::string*>(strPtr);
    }
    return std::make_shared<storage::StringArrayDeviceID>(id_array);
}

string SessionUtils::getTime(const Tablet &tablet) {
    MyStringBuffer timeBuffer;
    unsigned int n = 8u * tablet.rowSize;
    if (n > timeBuffer.str.capacity()) {
        timeBuffer.reserve(n);
    }

    for (size_t i = 0; i < tablet.rowSize; i++) {
        timeBuffer.putInt64(tablet.timestamps[i]);
    }
    return timeBuffer.str;
}

string SessionUtils::getValue(const Tablet &tablet) {
    MyStringBuffer valueBuffer;
    unsigned int n = 8u * tablet.schemas.size() * tablet.rowSize;
    if (n > valueBuffer.str.capacity()) {
        valueBuffer.reserve(n);
    }

    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        TSDataType::TSDataType dataType = tablet.schemas[i].second;
        const BitMap& bitMap = tablet.bitMaps[i];
        switch (dataType) { 
            case TSDataType::BOOLEAN: {
                bool* valueBuf = (bool*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putBool(valueBuf[index]);
                    }
                    else {
                        valueBuffer.putBool(false);
                    }
                }
                break;
            }
            case TSDataType::INT32: {
                int* valueBuf = (int*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putInt(valueBuf[index]);
                    }
                    else {
                        valueBuffer.putInt((numeric_limits<int32_t>::min)());
                    }
                }
                break;
            }
            case TSDataType::INT64: {
                int64_t* valueBuf = (int64_t*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putInt64(valueBuf[index]);
                    }
                    else {
                        valueBuffer.putInt64((numeric_limits<int64_t>::min)());
                    }
                }
                break;
            }
            case TSDataType::FLOAT: {
                float* valueBuf = (float*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putFloat(valueBuf[index]);
                    }
                    else {
                        valueBuffer.putFloat((numeric_limits<float>::min)());
                    }
                }
                break;
            }
            case TSDataType::DOUBLE: {
                double* valueBuf = (double*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putDouble(valueBuf[index]);
                    }
                    else {
                        valueBuffer.putDouble((numeric_limits<double>::min)());
                    }
                }
                break;
            }
            case TSDataType::TEXT: {
                string* valueBuf = (string*)(tablet.values[i]);
                for (size_t index = 0; index < tablet.rowSize; index++) {
                    if (!bitMap.isMarked(index)) {
                        valueBuffer.putString(valueBuf[index]);
                    } else {
                        valueBuffer.putString("");
                    }
                }
                break;
            }
            default:
                throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
    }
    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        const BitMap& bitMap = tablet.bitMaps[i];
        bool columnHasNull = !bitMap.isAllUnmarked();
        valueBuffer.putChar(columnHasNull ? (char) 1 : (char) 0);
        if (columnHasNull) {
            const vector<char>& bytes = bitMap.getByteArray();
            for (size_t index = 0; index < tablet.rowSize / 8 + 1; index++) {
                valueBuffer.putChar(bytes[index]);
            }
        }
    }
    return valueBuffer.str;
}

bool SessionUtils::isTabletContainsSingleDevice(Tablet tablet) {
    if (tablet.rowSize == 1) {
        return true;
    }
    auto firstDeviceId = tablet.getDeviceID(0);
    for (int i = 1; i < tablet.rowSize; ++i) {
        if (*firstDeviceId != *tablet.getDeviceID(i)) {
            return false;
        }
    }
    return true;
}

int SessionDataSet::getFetchSize() {
    return fetchSize;
}

void SessionDataSet::setFetchSize(int fetchSize) {
    this->fetchSize = fetchSize;
}

vector<string> SessionDataSet::getColumnNames() { return this->columnNameList; }

vector<string> SessionDataSet::getColumnTypeList() { return this->columnTypeList; }

bool SessionDataSet::hasNext() {
    if (hasCachedRecord) {
        return true;
    }
    if (!tsQueryDataSetTimeBuffer.hasRemaining()) {
        TSFetchResultsReq req;
        req.__set_sessionId(sessionId);
        req.__set_statement(sql);
        req.__set_fetchSize(fetchSize);
        req.__set_queryId(queryId);
        req.__set_isAlign(true);
        req.__set_timeout(-1);
        try {
            TSFetchResultsResp resp;
            client->fetchResults(resp, req);
            RpcUtils::verifySuccess(resp.status);

            if (!resp.hasResultSet) {
                return false;
            } else {
                TSQueryDataSet *tsQueryDataSet = &(resp.queryDataSet);
                tsQueryDataSetTimeBuffer.str = tsQueryDataSet->time;
                tsQueryDataSetTimeBuffer.pos = 0;

                valueBuffers.clear();
                bitmapBuffers.clear();

                for (size_t i = columnFieldStartIndex; i < columnNameList.size(); i++) {
                    if (duplicateLocation.find(i) != duplicateLocation.end()) {
                        continue;
                    }
                    std::string name = columnNameList[i];
                    int valueIndex = columnMap[name];
                    valueBuffers.emplace_back(new MyStringBuffer(tsQueryDataSet->valueList[valueIndex]));
                    bitmapBuffers.emplace_back(new MyStringBuffer(tsQueryDataSet->bitmapList[valueIndex]));
                }
                rowsIndex = 0;
            }
        } catch (const TTransportException &e) {
            log_debug(e.what());
            throw IoTDBConnectionException(e.what());
        } catch (const IoTDBException &e) {
            log_debug(e.what());
            throw;
        } catch (exception &e) {
            throw IoTDBException(string("Cannot fetch result from server: ") + e.what());
        }
    }

    constructOneRow();
    hasCachedRecord = true;
    return true;
}

void SessionDataSet::constructOneRow() {
    vector<Field> outFields;
    int loc = 0;
    for (size_t i = columnFieldStartIndex; i < columnNameList.size(); i++) {
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
                TSDataType::TSDataType dataType = getTSDataTypeFromString(columnTypeList[i]);
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
                        int64_t longValue = valueBuffer->getInt64();
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
                                string("Data type ") + columnTypeList[i] + " is not supported.");
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
        rowRecord = RowRecord(tsQueryDataSetTimeBuffer.getInt64(), outFields);
    } else {
        tsQueryDataSetTimeBuffer.getInt64();
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

void SessionDataSet::closeOperationHandle(bool forceClose) {
    if ((!forceClose) && (!operationIsOpen)) {
        return;
    }
    operationIsOpen = false;

    TSCloseOperationReq closeReq;
    closeReq.__set_sessionId(sessionId);
    closeReq.__set_statementId(statementId);
    closeReq.__set_queryId(queryId);
    TSStatus tsStatus;
    try {
        client->closeOperation(tsStatus, closeReq);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
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
    try {
        close();
    } catch (const exception &e) {
        log_debug(e.what());
    }
}

void Session::removeBrokenSessionConnection(shared_ptr<SessionConnection> sessionConnection) {
    if (enableRedirection) {
        this->endPointToSessionConnection.erase(sessionConnection->getEndPoint());
    }

    auto it1 = deviceIdToEndpoint.begin();
    while (it1 != deviceIdToEndpoint.end()) {
        if (it1->second == sessionConnection->getEndPoint()) {
            it1 = deviceIdToEndpoint.erase(it1);
        } else {
            ++it1;
        }
    }

    auto it2 = tableModelDeviceIdToEndpoint.begin();
    while (it2 != tableModelDeviceIdToEndpoint.end()) {
        if (it2->second == sessionConnection->getEndPoint()) {
            it2 = tableModelDeviceIdToEndpoint.erase(it2);
        } else {
            ++it2;
        }
    }
}

/**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
bool Session::checkSorted(const Tablet &tablet) {
    for (size_t i = 1; i < tablet.rowSize; i++) {
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

template<typename T>
std::vector<T> sortList(const std::vector<T>& valueList, const int* index, int indexLength) {
    std::vector<T> sortedValues(valueList.size());
    for (int i = 0; i < indexLength; i++) {
        sortedValues[i] = valueList[index[i]];
    }
    return sortedValues;
}

template<typename T>
void sortValuesList(T* valueList, const int* index, size_t indexLength) {
    T* sortedValues = new T[indexLength];
    for (int i = 0; i < indexLength; i++) {
        sortedValues[i] = valueList[index[i]];
    }
    for (int i = 0; i < indexLength; i++) {
        valueList[i] = sortedValues[i];
    }
    delete[] sortedValues;
}

void Session::sortTablet(Tablet& tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    int *index = new int[tablet.rowSize];
    for (size_t i = 0; i < tablet.rowSize; i++) {
        index[i] = i;
    }

    sortIndexByTimestamp(index, tablet.timestamps, tablet.rowSize);
    tablet.timestamps = sortList(tablet.timestamps, index, tablet.rowSize);
    for (size_t i = 0; i < tablet.schemas.size(); i++) {
        TSDataType::TSDataType dataType = tablet.schemas[i].second;
        switch (dataType) {
            case TSDataType::BOOLEAN: {
                sortValuesList((bool*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            case TSDataType::INT32: {
                sortValuesList((int*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            case TSDataType::INT64: {
                sortValuesList((int64_t*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            case TSDataType::FLOAT: {
                sortValuesList((float*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            case TSDataType::DOUBLE: {
                sortValuesList((double*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            case TSDataType::TEXT: {
                sortValuesList((string*)(tablet.values[i]), index, tablet.rowSize);
                break;
            }
            default:
                throw UnSupportedDataTypeException(string("Data type ") + to_string(dataType) + " is not supported.");
        }
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
                int32_t len = (uint32_t) strlen(values[i]);
                appendValues(buf, (char *) (&len), sizeof(uint32_t));
                // no need to change the byte order of string value
                buf.append(values[i], len);
                break;
            }
            case TSDataType::NULLTYPE:
                break;
            default:
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
        case Version::V_1_0:
            return "V_1_0";
        default:
            return "V_0_12";
    }
}

void Session::initZoneId() {
    if (!zoneId.empty()) {
        return;
    }

    time_t ts = 0;
    struct tm tmv;
#if defined(_WIN64) || defined (WIN32) || defined (_WIN32)
    localtime_s(&tmv, &ts);
#else
    localtime_r(&ts, &tmv);
#endif

    char zoneStr[32];
    strftime(zoneStr, sizeof(zoneStr), "%z", &tmv);
    zoneId = zoneStr;
}

void Session::initNodesSupplier() {
    std::vector<TEndPoint> endPoints;
    TEndPoint endPoint;
    endPoint.__set_ip(host);
    endPoint.__set_port(rpcPort);
    endPoints.emplace_back(endPoint);
    if (enableAutoFetch) {
        nodesSupplier = NodesSupplier::create(endPoints, username, password);
    } else {
        nodesSupplier = make_shared<StaticNodesSupplier>(endPoints);
    }
}

void Session::initDefaultSessionConnection() {
    defaultEndPoint.__set_ip(host);
    defaultEndPoint.__set_port(rpcPort);
    defaultSessionConnection = make_shared<SessionConnection>(this, defaultEndPoint, zoneId, nodesSupplier, fetchSize, 60, 500,
            sqlDialect, database);
}

void Session::insertStringRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
    vector<vector<string>> measurementsList, vector<vector<string>> valuesList, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertStringRecordsReq> recordsGroup;
    for (int i = 0; i < deviceIds.size(); i++) {
        auto connection = getSessionConnection(deviceIds[i]);
        TSInsertStringRecordsReq request;
        request.__set_sessionId(connection->sessionId);
        request.__set_prefixPaths(deviceIds);
        request.__set_timestamps(times);
        request.__set_measurementsList(measurementsList);
        request.__set_valuesList(valuesList);
        request.__set_isAligned(isAligned);
        recordsGroup.insert(make_pair(connection, request));
    }
    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertStringRecordsReq&)> consumer =
    [](const std::shared_ptr<SessionConnection>& c, const TSInsertStringRecordsReq& r) {
        c->insertStringRecords(r);
    };
    if (recordsGroup.size() == 1) {
        insertOnce(recordsGroup, consumer);
    } else {
        insertByGroup(recordsGroup, consumer);
    }
}

void Session::insertRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
    vector<vector<string>> measurementsList, const vector<vector<TSDataType::TSDataType>> &typesList,
                            vector<vector<char*>> valuesList, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertRecordsReq> recordsGroup;
    for (int i = 0; i < deviceIds.size(); i++) {
        auto connection = getSessionConnection(deviceIds[i]);
        TSInsertRecordsReq request;
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
        recordsGroup.insert(make_pair(connection, request));
    }
    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertRecordsReq&)> consumer =
    [](const std::shared_ptr<SessionConnection>& c, const TSInsertRecordsReq& r) {
        c->insertRecords(r);
    };
    if (recordsGroup.size() == 1) {
        insertOnce(recordsGroup, consumer);
    } else {
        insertByGroup(recordsGroup, consumer);
    }
}

void Session::insertTabletsWithLeaderCache(unordered_map<string, Tablet*> tablets, bool sorted, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertTabletsReq> tabletGroup;
    if (tablets.empty()) {
        throw BatchExecutionException("No tablet is inserting!");
    }
    auto beginIter = tablets.begin();
    bool isFirstTabletAligned = ((*beginIter).second)->isAligned;
    for (const auto &item: tablets) {
        TSInsertTabletsReq request;
        if (isFirstTabletAligned != item.second->isAligned) {
            throw BatchExecutionException("The tablets should be all aligned or non-aligned!");
        }
        if (!checkSorted(*(item.second))) {
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
        request.timestampsList.push_back(move(SessionUtils::getTime(*(item.second))));
        request.valuesList.push_back(move(SessionUtils::getValue(*(item.second))));
        request.sizeList.push_back(item.second->rowSize);
        request.__set_isAligned(item.second->isAligned);
        auto connection = getSessionConnection(item.first);
        tabletGroup.insert(make_pair(connection, request));
    }

    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertTabletsReq&)> consumer =
    [](const std::shared_ptr<SessionConnection>& c, const TSInsertTabletsReq& r) {
        c->insertTablets(r);
    };
    if (tabletGroup.size() == 1) {
        insertOnce(tabletGroup, consumer);
    } else {
        insertByGroup(tabletGroup, consumer);
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

    try {
        initDefaultSessionConnection();
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    zoneId = defaultSessionConnection->zoneId;

    if (enableRedirection) {
        endPointToSessionConnection.insert(make_pair(defaultEndPoint, defaultSessionConnection));
    }

    isClosed = false;
}


void Session::close() {
    if (isClosed) {
        return;
    }
    isClosed = true;
}


void Session::insertRecord(const string &deviceId, int64_t time,
                           const vector<string> &measurements,
                           const vector<string> &values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(false);
    try {
        getSessionConnection(deviceId)->insertStringRecord(req);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertStringRecord(req);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
    }
}

void Session::insertRecord(const string &deviceId, int64_t time,
                           const vector<string> &measurements,
                           const vector<TSDataType::TSDataType> &types,
                           const vector<char *> &values) {
    TSInsertRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    string buffer;
    putValuesIntoBuffer(types, values, buffer);
    req.__set_values(buffer);
    req.__set_isAligned(false);
    try {
        getSessionConnection(deviceId)->insertRecord(req);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertRecord(req);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
    }
}

void Session::insertAlignedRecord(const string &deviceId, int64_t time,
                                  const vector<string> &measurements,
                                  const vector<string> &values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(true);
    try {
        getSessionConnection(deviceId)->insertStringRecord(req);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertStringRecord(req);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
    }
}

void Session::insertAlignedRecord(const string &deviceId, int64_t time,
                                  const vector<string> &measurements,
                                  const vector<TSDataType::TSDataType> &types,
                                  const vector<char *> &values) {
    TSInsertRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    string buffer;
    putValuesIntoBuffer(types, values, buffer);
    req.__set_values(buffer);
    req.__set_isAligned(false);
    try {
        getSessionConnection(deviceId)->insertRecord(req);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertRecord(req);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
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

    if (enableRedirection) {
        insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, false);
    } else {
        TSInsertStringRecordsReq request;
        request.__set_prefixPaths(deviceIds);
        request.__set_timestamps(times);
        request.__set_measurementsList(measurementsList);
        request.__set_valuesList(valuesList);
        request.__set_isAligned(false);
        try {
            defaultSessionConnection->insertStringRecords(request);
        } catch (RedirectException& e) {}
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

    if (enableRedirection) {
        insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList, false);
    } else {
        TSInsertRecordsReq request;
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
            defaultSessionConnection->insertRecords(request);
        } catch (RedirectException& e) {}
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

    if (enableRedirection) {
        insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, true);
    } else {
        TSInsertStringRecordsReq request;
        request.__set_prefixPaths(deviceIds);
        request.__set_timestamps(times);
        request.__set_measurementsList(measurementsList);
        request.__set_valuesList(valuesList);
        request.__set_isAligned(true);
        try {
            defaultSessionConnection->insertStringRecords(request);
        } catch (RedirectException& e) {}
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

    if (enableRedirection) {
        insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList, false);
    } else {
        TSInsertRecordsReq request;
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
            defaultSessionConnection->insertRecords(request);
        } catch (RedirectException& e) {}
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
    if (!checkSorted(times)) {
        int *index = new int[times.size()];
        for (size_t i = 0; i < times.size(); i++) {
            index[i] = (int)i;
        }

        sortIndexByTimestamp(index, times, (int)(times.size()));
        times = sortList(times, index, (int)(times.size()));
        measurementsList = sortList(measurementsList, index, (int)(times.size()));
        typesList = sortList(typesList, index, (int)(times.size()));
        valuesList = sortList(valuesList, index, (int)(times.size()));
        delete[] index;
    }
    TSInsertRecordsOfOneDeviceReq request;
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
    TSStatus respStatus;
    try {
        getSessionConnection(deviceId)->insertRecordsOfOneDevice(request);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertRecordsOfOneDevice(request);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
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
    if (!checkSorted(times)) {
        int *index = new int[times.size()];
        for (size_t i = 0; i < times.size(); i++) {
            index[i] = (int)i;
        }

        sortIndexByTimestamp(index, times, (int)(times.size()));
        times = sortList(times, index, (int)(times.size()));
        measurementsList = sortList(measurementsList, index, (int)(times.size()));
        typesList = sortList(typesList, index, (int)(times.size()));
        valuesList = sortList(valuesList, index, (int)(times.size()));
        delete[] index;
    }
    TSInsertRecordsOfOneDeviceReq request;
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
    TSStatus respStatus;
    try {
        getSessionConnection(deviceId)->insertRecordsOfOneDevice(request);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertRecordsOfOneDevice(request);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
    }
}

void Session::insertTablet(Tablet &tablet) {
    try {
        insertTablet(tablet, false);
    }
    catch (const exception &e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::buildInsertTabletReq(TSInsertTabletReq &request, Tablet &tablet, bool sorted) {
    if ((!sorted) && !checkSorted(tablet)) {
        sortTablet(tablet);
    }

    request.prefixPath = tablet.deviceId;

    request.measurements.reserve(tablet.schemas.size());
    request.types.reserve(tablet.schemas.size());
    for (pair<string, TSDataType::TSDataType> schema: tablet.schemas) {
        request.measurements.push_back(schema.first);
        request.types.push_back(schema.second);
    }

    request.values = move(SessionUtils::getValue(tablet));
    request.timestamps = move(SessionUtils::getTime(tablet));
    request.__set_size(tablet.rowSize);
    request.__set_isAligned(tablet.isAligned);
}

void Session::insertTablet(TSInsertTabletReq request) {
    auto deviceId = request.prefixPath;
    try {
        getSessionConnection(deviceId)->insertTablet(request);
    } catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException &e) {
        if (enableRedirection && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection->insertTablet(request);
            } catch (RedirectException& e) {
            }
        } else {
            throw e;
        }
    }
}

void Session::insertTablet(Tablet &tablet, bool sorted) {
    TSInsertTabletReq request;
    buildInsertTabletReq(request, tablet, sorted);
    insertTablet(request);
}

void Session::insertRelationalTablet(Tablet &tablet, bool sorted) {
    std::unordered_map<std::shared_ptr<SessionConnection>, Tablet> relationalTabletGroup;
    if (tableModelDeviceIdToEndpoint.empty()) {
        relationalTabletGroup.insert(make_pair(defaultSessionConnection, tablet));
    } else if (SessionUtils::isTabletContainsSingleDevice(tablet)) {
        relationalTabletGroup.insert(make_pair(getSessionConnection(tablet.getDeviceID(0)), tablet));
    } else {
        for (int i = 0; i < tablet.rowSize; i++) {
            auto iDeviceID = tablet.getDeviceID(i);
            std::shared_ptr<SessionConnection> connection = getSessionConnection(iDeviceID);

            auto it = relationalTabletGroup.find(connection);
            if (it == relationalTabletGroup.end()) {
                Tablet newTablet(tablet.deviceId, tablet.schemas, tablet.columnTypes);
                it = relationalTabletGroup.insert(std::make_pair(connection, newTablet)).first;
            }

            Tablet& currentTablet = it->second;
            currentTablet.values[currentTablet.rowSize] = tablet.values[i];
            currentTablet.addTimestamp(currentTablet.rowSize, tablet.timestamps[i]);
        }
    }
    if (relationalTabletGroup.size() == 1) {
        insertRelationalTabletOnce(relationalTabletGroup, sorted);
    } else {
        insertRelationalTabletByGroup(relationalTabletGroup, sorted);
    }
}

void Session::insertRelationalTablet(Tablet &tablet) {
    insertRelationalTablet(tablet, false);
}

void Session::insertRelationalTabletOnce(const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>&
    relationalTabletGroup, bool sorted) {
    auto iter = relationalTabletGroup.begin();
    auto connection = iter->first;
    auto tablet = iter->second;
    TSInsertTabletReq request;
    buildInsertTabletReq(request, tablet, sorted);
    request.__set_writeToTable(true);
    std::vector<int8_t> columnCategories;
    for (auto &category: tablet.columnTypes) {
        columnCategories.push_back(static_cast<int8_t>(category));
    }
    request.__set_columnCategories(columnCategories);
    try {
        TSStatus respStatus;
        connection->getSessionClient()->insertTablet(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }  catch (RedirectException& e) {
        auto endPointList = e.endPointList;
        for (int i = 0; i < endPointList.size(); i++) {
            auto deviceID = tablet.getDeviceID(i);
            handleRedirection(deviceID, endPointList[i]);
        }
    } catch (const IoTDBConnectionException &e) {
        if (endPointToSessionConnection.size() > 1) {
            removeBrokenSessionConnection(connection);
            try {
                TSStatus respStatus;
                defaultSessionConnection->getSessionClient()->insertTablet(respStatus, request);
                RpcUtils::verifySuccess(respStatus);
            } catch (RedirectException& e) {
            }
        } else {
            throw IoTDBConnectionException(e.what());
        }
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::insertRelationalTabletByGroup(const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>&
    relationalTabletGroup, bool sorted) {
    // Create a vector to store future objects for asynchronous operations
    std::vector<std::future<void>> futures;

    for (auto iter = relationalTabletGroup.begin(); iter != relationalTabletGroup.end(); iter++) {
        auto connection = iter->first;
        auto tablet = iter->second;

        // Launch asynchronous task for each tablet insertion
        futures.emplace_back(std::async(std::launch::async, [=]() mutable {
            TSInsertTabletReq request;
            buildInsertTabletReq(request, tablet, sorted);
            request.__set_writeToTable(true);

            std::vector<int8_t> columnCategories;
            for (auto &category: tablet.columnTypes) {
                columnCategories.push_back(static_cast<int8_t>(category));
            }
            request.__set_columnCategories(columnCategories);

            try {
                TSStatus respStatus;
                connection->getSessionClient()->insertTablet(respStatus, request);
                RpcUtils::verifySuccess(respStatus);
            } catch (const TTransportException &e) {
                log_debug(e.what());
                throw IoTDBConnectionException(e.what());
            } catch (const IoTDBException &e) {
                log_debug(e.what());
                throw;
            } catch (const exception &e) {
                log_debug(e.what());
                throw IoTDBException(e.what());
            }
        }));
    }

    for (auto &f : futures) {
        f.get();
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
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet *> &tablets) {
    try {
        insertTablets(tablets, false);
    }
    catch (const exception &e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet *> &tablets, bool sorted) {
    TSInsertTabletsReq request;
    if (tablets.empty()) {
        throw BatchExecutionException("No tablet is inserting!");
    }
    auto beginIter = tablets.begin();
    bool isFirstTabletAligned = ((*beginIter).second)->isAligned;
    for (const auto &item: tablets) {
        if (isFirstTabletAligned != item.second->isAligned) {
            throw BatchExecutionException("The tablets should be all aligned or non-aligned!");
        }
        if (!checkSorted(*(item.second))) {
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
        request.timestampsList.push_back(move(SessionUtils::getTime(*(item.second))));
        request.valuesList.push_back(move(SessionUtils::getValue(*(item.second))));
        request.sizeList.push_back(item.second->rowSize);
    }
    request.__set_isAligned(isFirstTabletAligned);
    try {
        TSStatus respStatus;
        defaultSessionConnection->getSessionClient()->insertTablets(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}


void Session::insertAlignedTablets(unordered_map<string, Tablet *> &tablets, bool sorted) {
    for (auto iter = tablets.begin(); iter != tablets.end(); iter++) {
        iter->second->setAligned(true);
    }
    try {
        insertTablets(tablets, sorted);
    }
    catch (const exception &e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::testInsertRecord(const string &deviceId, int64_t time, const vector<string> &measurements,
                               const vector<string> &values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->testInsertStringRecord(req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::testInsertTablet(const Tablet &tablet) {
    TSInsertTabletReq request;
    request.prefixPath = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema: tablet.schemas) {
        request.measurements.push_back(schema.first);
        request.types.push_back(schema.second);
    }
    request.__set_timestamps(move(SessionUtils::getTime(tablet)));
    request.__set_values(move(SessionUtils::getValue(tablet)));
    request.__set_size(tablet.rowSize);
    try {
        TSStatus tsStatus;
        defaultSessionConnection->testInsertTablet(request);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
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
    TSInsertStringRecordsReq request;
    request.__set_prefixPaths(deviceIds);
    request.__set_timestamps(times);
    request.__set_measurementsList(measurementsList);
    request.__set_valuesList(valuesList);

    try {
        TSStatus tsStatus;
        defaultSessionConnection->getSessionClient()->insertStringRecords(tsStatus, request);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::deleteTimeseries(const string &path) {
    vector<string> paths;
    paths.push_back(path);
    deleteTimeseries(paths);
}

void Session::deleteTimeseries(const vector<string> &paths) {
    TSStatus tsStatus;

    try {
        defaultSessionConnection->getSessionClient()->deleteTimeseries(tsStatus,
            defaultSessionConnection->sessionId, paths);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::deleteData(const string &path, int64_t endTime) {
    vector<string> paths;
    paths.push_back(path);
    deleteData(paths, LONG_LONG_MIN, endTime);
}

void Session::deleteData(const vector<string> &paths, int64_t endTime) {
    deleteData(paths, LONG_LONG_MIN, endTime);
}

void Session::deleteData(const vector<string> &paths, int64_t startTime, int64_t endTime) {
    TSDeleteDataReq req;
    req.__set_paths(paths);
    req.__set_startTime(startTime);
    req.__set_endTime(endTime);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->deleteData(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::setStorageGroup(const string &storageGroupId) {
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->setStorageGroup(tsStatus, defaultSessionConnection->sessionId, storageGroupId);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::deleteStorageGroup(const string &storageGroup) {
    vector<string> storageGroups;
    storageGroups.push_back(storageGroup);
    deleteStorageGroups(storageGroups);
}

void Session::deleteStorageGroups(const vector<string> &storageGroups) {
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->deleteStorageGroups(tsStatus, defaultSessionConnection->sessionId, storageGroups);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::createDatabase(const string &database) {
    this->setStorageGroup(database);
}

void Session::deleteDatabase(const string &database) {
    this->deleteStorageGroups(vector<string>{database});
}

void Session::deleteDatabases(const vector<string> &databases) {
    this->deleteStorageGroups(databases);
}

void Session::createTimeseries(const string &path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor) {
    try {
        createTimeseries(path, dataType, encoding, compressor, nullptr, nullptr, nullptr, "");
    }
    catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::createTimeseries(const string &path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor,
                               map<string, string> *props,
                               map<string, string> *tags,
                               map<string, string> *attributes,
                               const string &measurementAlias) {
    TSCreateTimeseriesReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_path(path);
    req.__set_dataType(dataType);
    req.__set_encoding(encoding);
    req.__set_compressor(compressor);
    if (props != nullptr) {
        req.__set_props(*props);
    }

    if (tags != nullptr) {
        req.__set_tags(*tags);
    }
    if (attributes != nullptr) {
        req.__set_attributes(*attributes);
    }
    if (!measurementAlias.empty()) {
        req.__set_measurementAlias(measurementAlias);
    }

    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->createTimeseries(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
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
    TSCreateMultiTimeseriesReq request;
    request.__set_sessionId(defaultSessionConnection->sessionId);
    request.__set_paths(paths);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request.__set_compressors(compressorsOrdinal);

    if (propsList != nullptr) {
        request.__set_propsList(*propsList);
    }

    if (tagsList != nullptr) {
        request.__set_tagsList(*tagsList);
    }
    if (attributesList != nullptr) {
        request.__set_attributesList(*attributesList);
    }
    if (measurementAliasList != nullptr) {
        request.__set_measurementAliasList(*measurementAliasList);
    }

    try {
        TSStatus tsStatus;
        defaultSessionConnection->getSessionClient()->createMultiTimeseries(tsStatus, request);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::createAlignedTimeseries(const std::string &deviceId,
                                      const std::vector<std::string> &measurements,
                                      const std::vector<TSDataType::TSDataType> &dataTypes,
                                      const std::vector<TSEncoding::TSEncoding> &encodings,
                                      const std::vector<CompressionType::CompressionType> &compressors) {
    TSCreateAlignedTimeseriesReq request;
    request.__set_sessionId(defaultSessionConnection->sessionId);
    request.__set_prefixPath(deviceId);
    request.__set_measurements(measurements);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request.__set_compressors(compressorsOrdinal);

    try {
        TSStatus tsStatus;
        defaultSessionConnection->getSessionClient()->createAlignedTimeseries(tsStatus, request);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

bool Session::checkTimeseriesExists(const string &path) {
    try {
        std::unique_ptr<SessionDataSet> dataset = executeQueryStatement("SHOW TIMESERIES " + path);
        bool isExisted = dataset->hasNext();
        dataset->closeOperationHandle();
        return isExisted;
    }
    catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

shared_ptr<SessionConnection> Session::getQuerySessionConnection() {
    auto endPoint = nodesSupplier->getQueryEndPoint();
    if (!endPoint.has_value() || endPointToSessionConnection.empty()) {
        return defaultSessionConnection;
    }

    auto it = endPointToSessionConnection.find(endPoint.value());
    if (it != endPointToSessionConnection.end()) {
        return it->second;
    }

    shared_ptr<SessionConnection> newConnection;
    try {
        newConnection = make_shared<SessionConnection>(this, endPoint.value(), zoneId, nodesSupplier,
        fetchSize, 60, 500, sqlDialect, database);
        endPointToSessionConnection.emplace(endPoint.value(), newConnection);
        return newConnection;
    } catch (exception &e) {
        log_debug("Session::getQuerySessionConnection() exception: " + e.what());
        return newConnection;
    }
}

shared_ptr<SessionConnection> Session::getSessionConnection(std::string deviceId) {
    if (!enableRedirection ||
            deviceIdToEndpoint.find(deviceId) == deviceIdToEndpoint.end() ||
            endPointToSessionConnection.find(deviceIdToEndpoint[deviceId]) == endPointToSessionConnection.end()) {
        return defaultSessionConnection;
    }
    return endPointToSessionConnection.find(deviceIdToEndpoint[deviceId])->second;
}

shared_ptr<SessionConnection> Session::getSessionConnection(std::shared_ptr<storage::IDeviceID> deviceId) {
    if (!enableRedirection ||
        tableModelDeviceIdToEndpoint.find(deviceId) == tableModelDeviceIdToEndpoint.end() ||
        endPointToSessionConnection.find(tableModelDeviceIdToEndpoint[deviceId]) == endPointToSessionConnection.end()) {
        return defaultSessionConnection;
        }
    return endPointToSessionConnection.find(tableModelDeviceIdToEndpoint[deviceId])->second;
}

string Session::getTimeZone() {
    if (!zoneId.empty()) {
        return zoneId;
    }
    TSGetTimeZoneResp resp;
    try {
        defaultSessionConnection->getSessionClient()->getTimeZone(resp, defaultSessionConnection->sessionId);
        RpcUtils::verifySuccess(resp.status);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.timeZone;
}

void Session::setTimeZone(const string &zoneId) {
    TSSetTimeZoneReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_timeZone(zoneId);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->setTimeZone(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
        this->zoneId = zoneId;
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

unique_ptr<SessionDataSet> Session::executeQueryStatement(const string &sql) {
    return executeQueryStatementMayRedirect(sql, QUERY_TIMEOUT_MS);
}

unique_ptr<SessionDataSet> Session::executeQueryStatement(const string &sql, int64_t timeoutInMs) {
    return executeQueryStatementMayRedirect(sql, timeoutInMs);
}

void Session::handleQueryRedirection(TEndPoint endPoint) {
    if (!enableRedirection) return;
    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    } else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId, nodesSupplier,
                    fetchSize, 60, 500, sqlDialect, database);

            endPointToSessionConnection.emplace(endPoint, newConnection);
        } catch (exception &e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    defaultSessionConnection = newConnection;
}

void Session::handleRedirection(const std::string& deviceId, TEndPoint endPoint) {
    if (!enableRedirection) return;
    if (endPoint.ip == "127.0.0.1") return;
    deviceIdToEndpoint[deviceId] = endPoint;

    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    } else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId, nodesSupplier,
                    fetchSize, 60, 500, sqlDialect, database);
            endPointToSessionConnection.emplace(endPoint, newConnection);
        } catch (exception &e) {
            deviceIdToEndpoint.erase(deviceId);
            throw IoTDBConnectionException(e.what());
        }
    }
}

void Session::handleRedirection(const std::shared_ptr<storage::IDeviceID>& deviceId, TEndPoint endPoint) {
    if (!enableRedirection) return;
    if (endPoint.ip == "127.0.0.1") return;
    tableModelDeviceIdToEndpoint[deviceId] = endPoint;

    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    } else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId, nodesSupplier,
                    fetchSize, 60, 500, sqlDialect, database);
            endPointToSessionConnection.emplace(endPoint, newConnection);
        } catch (exception &e) {
            tableModelDeviceIdToEndpoint.erase(deviceId);
            throw IoTDBConnectionException(e.what());
        }
    }
}

std::unique_ptr<SessionDataSet> Session::executeQueryStatementMayRedirect(const std::string &sql, int64_t timeoutInMs) {
    auto sessionConnection = getQuerySessionConnection();
    if (!sessionConnection) {
        log_warn("Session connection not found");
        return nullptr;
    }
    try {
        return sessionConnection->executeQueryStatement(sql, timeoutInMs);
    } catch (RedirectException& e) {
        log_warn("Session connection redirect exception: " + e.what());
        handleQueryRedirection(e.endPoint);
        try {
            return defaultSessionConnection->executeQueryStatement(sql, timeoutInMs);
        } catch (exception& e) {
            log_error("Exception while executing redirected query statement: %s", e.what());
            throw ExecutionException(e.what());
        }
    } catch (exception& e) {
        log_error("Exception while executing query statement: %s", e.what());
        throw e;
    }

}

void Session::executeNonQueryStatement(const string &sql) {
    try {
        defaultSessionConnection->executeNonQueryStatement(sql);
    } catch (const exception &e) {
        throw IoTDBException(e.what());
    }
}

unique_ptr<SessionDataSet> Session::executeRawDataQuery(const vector<string> &paths, int64_t startTime, int64_t endTime) {
    return defaultSessionConnection->executeRawDataQuery(paths, startTime, endTime);
}


unique_ptr<SessionDataSet> Session::executeLastDataQuery(const vector<string> &paths) {
    return executeLastDataQuery(paths, LONG_LONG_MIN);
}
unique_ptr<SessionDataSet> Session::executeLastDataQuery(const vector<string> &paths, int64_t lastTime) {
    return defaultSessionConnection->executeLastDataQuery(paths, lastTime);
}

void Session::createSchemaTemplate(const Template &templ) {
    TSCreateSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(templ.getName());
    req.__set_serializedTemplate(templ.serialize());
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->createSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::setSchemaTemplate(const string &template_name, const string &prefix_path) {
    TSSetSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_templateName(template_name);
    req.__set_prefixPath(prefix_path);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->setSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::unsetSchemaTemplate(const string &prefix_path, const string &template_name) {
    TSUnsetSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_templateName(template_name);
    req.__set_prefixPath(prefix_path);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->unsetSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::addAlignedMeasurementsInTemplate(const string &template_name, const vector<std::string> &measurements,
                                               const vector<TSDataType::TSDataType> &dataTypes,
                                               const vector<TSEncoding::TSEncoding> &encodings,
                                               const vector<CompressionType::CompressionType> &compressors) {
    TSAppendSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurements(measurements);
    req.__set_isAligned(true);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req.__set_compressors(compressorsOrdinal);

    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->appendSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
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
    TSAppendSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurements(measurements);
    req.__set_isAligned(false);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType: dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding: encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor: compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req.__set_compressors(compressorsOrdinal);

    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->appendSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
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
    TSPruneSchemaTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_path(path);
    TSStatus tsStatus;
    try {
        defaultSessionConnection->getSessionClient()->pruneSchemaTemplate(tsStatus, req);
        RpcUtils::verifySuccess(tsStatus);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException &e) {
        log_debug(e.what());
        throw;
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

int Session::countMeasurementsInTemplate(const string &template_name) {
    TSQueryTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_queryType(TemplateQueryType::COUNT_MEASUREMENTS);
    TSQueryTemplateResp resp;
    try {
        defaultSessionConnection->getSessionClient()->querySchemaTemplate(resp, req);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.count;
}

bool Session::isMeasurementInTemplate(const string &template_name, const string &path) {
    TSQueryTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurement(path);
    req.__set_queryType(TemplateQueryType::IS_MEASUREMENT);
    TSQueryTemplateResp resp;
    try {
        defaultSessionConnection->getSessionClient()->querySchemaTemplate(resp, req);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.result;
}

bool Session::isPathExistInTemplate(const string &template_name, const string &path) {
    TSQueryTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurement(path);
    req.__set_queryType(TemplateQueryType::PATH_EXIST);
    TSQueryTemplateResp resp;
    try {
        defaultSessionConnection->getSessionClient()->querySchemaTemplate(resp, req);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.result;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string &template_name) {
    TSQueryTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurement("");
    req.__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    TSQueryTemplateResp resp;
    try {
        defaultSessionConnection->getSessionClient()->querySchemaTemplate(resp, req);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.measurements;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string &template_name, const string &pattern) {
    TSQueryTemplateReq req;
    req.__set_sessionId(defaultSessionConnection->sessionId);
    req.__set_name(template_name);
    req.__set_measurement(pattern);
    req.__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    TSQueryTemplateResp resp;
    try {
        defaultSessionConnection->getSessionClient()->querySchemaTemplate(resp, req);
    } catch (const TTransportException &e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const exception &e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    return resp.measurements;
}

bool Session::checkTemplateExists(const string& template_name) {
    try {
        std::unique_ptr<SessionDataSet> dataset = executeQueryStatement("SHOW NODES IN DEVICE TEMPLATE " + template_name);
        bool isExisted = dataset->hasNext();
        dataset->closeOperationHandle();
        return isExisted;
    }
    catch (const exception &e) {
        if ( strstr(e.what(), "does not exist") != NULL ) {
            return false;
        }
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}
