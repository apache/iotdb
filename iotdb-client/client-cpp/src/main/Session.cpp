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
#include <unordered_set>
#include "NodesSupplier.h"
#include "SessionDataSet.h"

using namespace std;

/**
* Timeout of query can be set by users.
* A negative number means using the default configuration of server.
* And value 0 will disable the function of query timeout.
*/
static const int64_t QUERY_TIMEOUT_MS = -1;

LogLevelType LOG_LEVEL = LEVEL_DEBUG;

TSDataType::TSDataType getTSDataTypeFromString(const string& str) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, BLOB, TIMESTAMP, DATE, NULLTYPE
    if (str == "BOOLEAN") {
        return TSDataType::BOOLEAN;
    } else if (str == "INT32") {
        return TSDataType::INT32;
    } else if (str == "INT64") {
        return TSDataType::INT64;
    } else if (str == "FLOAT") {
        return TSDataType::FLOAT;
    } else if (str == "DOUBLE") {
        return TSDataType::DOUBLE;
    } else if (str == "TEXT") {
        return TSDataType::TEXT;
    } else if (str == "TIMESTAMP") {
        return TSDataType::TIMESTAMP;
    } else if (str == "DATE") {
        return TSDataType::DATE;
    } else if (str == "BLOB") {
        return TSDataType::BLOB;
    } else if (str == "STRING") {
        return TSDataType::STRING;
    } else if (str == "OBJECT") {
        return TSDataType::OBJECT;
    }
    return TSDataType::UNKNOWN;
}

void Tablet::createColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        TSDataType::TSDataType dataType = schemas[i].second;
        switch (dataType) {
        case TSDataType::BOOLEAN:
            values[i] = new bool[maxRowNumber];
            break;
        case TSDataType::DATE:
            values[i] = new boost::gregorian::date[maxRowNumber];
            break;
        case TSDataType::INT32:
            values[i] = new int[maxRowNumber];
            break;
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64:
            values[i] = new int64_t[maxRowNumber];
            break;
        case TSDataType::FLOAT:
            values[i] = new float[maxRowNumber];
            break;
        case TSDataType::DOUBLE:
            values[i] = new double[maxRowNumber];
            break;
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
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
        if (!values[i]) continue;
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
        case TSDataType::DATE: {
            boost::gregorian::date* valueBuf = (boost::gregorian::date*)(values[i]);
            delete[] valueBuf;
            break;
        }
        case TSDataType::TIMESTAMP:
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
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
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

void Tablet::deepCopyTabletColValue(void* const* srcPtr, void** destPtr, TSDataType::TSDataType type, int maxRowNumber) {
    void *src = *srcPtr;
    switch (type) {
    case TSDataType::BOOLEAN:
        *destPtr = new bool[maxRowNumber];
        memcpy(*destPtr, src, maxRowNumber * sizeof(bool));
        break;
    case TSDataType::INT32:
        *destPtr = new int32_t[maxRowNumber];
        memcpy(*destPtr, src, maxRowNumber * sizeof(int32_t));
        break;
    case TSDataType::INT64:
    case TSDataType::TIMESTAMP:
        *destPtr = new int64_t[maxRowNumber];
        memcpy(*destPtr, src, maxRowNumber * sizeof(int64_t));
        break;
    case TSDataType::FLOAT:
        *destPtr = new float[maxRowNumber];
        memcpy(*destPtr, src, maxRowNumber * sizeof(float));
        break;
    case TSDataType::DOUBLE:
        *destPtr = new double[maxRowNumber];
        memcpy(*destPtr, src, maxRowNumber * sizeof(double));
        break;
    case TSDataType::DATE: {
        *destPtr = new boost::gregorian::date[maxRowNumber];
        boost::gregorian::date* srcDate = static_cast<boost::gregorian::date*>(src);
        boost::gregorian::date* destDate = static_cast<boost::gregorian::date*>(*destPtr);
        for (size_t j = 0; j < maxRowNumber; ++j) {
            destDate[j] = srcDate[j];
        }
        break;
    }
    case TSDataType::STRING:
    case TSDataType::TEXT:
    case TSDataType::OBJECT:
    case TSDataType::BLOB: {
        *destPtr = new std::string[maxRowNumber];
        std::string* srcStr = static_cast<std::string*>(src);
        std::string* destStr = static_cast<std::string*>(*destPtr);
        for (size_t j = 0; j < maxRowNumber; ++j) {
            destStr[j] = srcStr[j];
        }
        break;
    }
    default:
        break;
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
        case TSDataType::DATE:
            valueOccupation += rowSize * 4;
            break;
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64:
            valueOccupation += rowSize * 8;
            break;
        case TSDataType::FLOAT:
            valueOccupation += rowSize * 4;
            break;
        case TSDataType::DOUBLE:
            valueOccupation += rowSize * 8;
            break;
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
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
        void* strPtr = getValue(idColumnIndex, row, TSDataType::TEXT);
        id_array[idArrayIdx++] = *static_cast<std::string*>(strPtr);
    }
    return std::make_shared<storage::StringArrayDeviceID>(id_array);
}

string SessionUtils::getTime(const Tablet& tablet) {
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

string SessionUtils::getValue(const Tablet& tablet) {
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
        case TSDataType::DATE: {
            boost::gregorian::date* valueBuf = (boost::gregorian::date*)(tablet.values[i]);
            for (size_t index = 0; index < tablet.rowSize; index++) {
                if (!bitMap.isMarked(index)) {
                    valueBuffer.putDate(valueBuf[index]);
                }
                else {
                    valueBuffer.putInt(EMPTY_DATE_INT);
                }
            }
            break;
        }
        case TSDataType::TIMESTAMP:
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
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
        case TSDataType::TEXT: {
            string* valueBuf = (string*)(tablet.values[i]);
            for (size_t index = 0; index < tablet.rowSize; index++) {
                if (!bitMap.isMarked(index)) {
                    valueBuffer.putString(valueBuf[index]);
                }
                else {
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
        valueBuffer.putChar(columnHasNull ? (char)1 : (char)0);
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

    for (const auto& child : children_) {
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
            for (const auto& child : cur_node_ptr->getChildren()) {
                stack.push(make_pair(fullPath, child.second));
            }
        }
        else {
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
    }
    catch (const exception& e) {
        log_debug(e.what());
    }
}

void Session::removeBrokenSessionConnection(shared_ptr<SessionConnection> sessionConnection) {
    if (enableRedirection_) {
        this->endPointToSessionConnection.erase(sessionConnection->getEndPoint());
    }

    auto it1 = deviceIdToEndpoint.begin();
    while (it1 != deviceIdToEndpoint.end()) {
        if (it1->second == sessionConnection->getEndPoint()) {
            it1 = deviceIdToEndpoint.erase(it1);
        }
        else {
            ++it1;
        }
    }

    auto it2 = tableModelDeviceIdToEndpoint.begin();
    while (it2 != tableModelDeviceIdToEndpoint.end()) {
        if (it2->second == sessionConnection->getEndPoint()) {
            it2 = tableModelDeviceIdToEndpoint.erase(it2);
        }
        else {
            ++it2;
        }
    }
}

/**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
bool Session::checkSorted(const Tablet& tablet) {
    for (size_t i = 1; i < tablet.rowSize; i++) {
        if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
            return false;
        }
    }
    return true;
}

bool Session::checkSorted(const vector<int64_t>& times) {
    for (size_t i = 1; i < times.size(); i++) {
        if (times[i] < times[i - 1]) {
            return false;
        }
    }
    return true;
}

template <typename T>
std::vector<T> sortList(const std::vector<T>& valueList, const int* index, int indexLength) {
    std::vector<T> sortedValues(valueList.size());
    for (int i = 0; i < indexLength; i++) {
        sortedValues[i] = valueList[index[i]];
    }
    return sortedValues;
}

template <typename T>
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
    int* index = new int[tablet.rowSize];
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
        case TSDataType::DATE: {
            sortValuesList((boost::gregorian::date*)(tablet.values[i]), index, tablet.rowSize);
            break;
        }
        case TSDataType::TIMESTAMP:
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
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
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

void Session::sortIndexByTimestamp(int* index, std::vector<int64_t>& timestamps, int length) {
    if (length <= 1) {
        return;
    }

    TsCompare tsCompareObj(timestamps);
    std::sort(&index[0], &index[length], tsCompareObj);
}

/**
 * Append value into buffer in Big Endian order to comply with IoTDB server
 */
void Session::appendValues(string& buffer, const char* value, int size) {
    static bool hasCheckedEndianFlag = false;
    static bool localCpuIsBigEndian = false;
    if (!hasCheckedEndianFlag) {
        hasCheckedEndianFlag = true;
        int chk = 0x0201; //used to distinguish CPU's type (BigEndian or LittleEndian)
        localCpuIsBigEndian = (0x01 != *(char*)(&chk));
    }

    if (localCpuIsBigEndian) {
        buffer.append(value, size);
    }
    else {
        for (int i = size - 1; i >= 0; i--) {
            buffer.append(value + i, 1);
        }
    }
}

void
Session::putValuesIntoBuffer(const vector<TSDataType::TSDataType>& types, const vector<char*>& values, string& buf) {
    int32_t date;
    for (size_t i = 0; i < values.size(); i++) {
        int8_t typeNum = getDataTypeNumber(types[i]);
        buf.append((char*)(&typeNum), sizeof(int8_t));
        switch (types[i]) {
        case TSDataType::BOOLEAN:
            buf.append(values[i], 1);
            break;
        case TSDataType::INT32:
            appendValues(buf, values[i], sizeof(int32_t));
            break;
        case TSDataType::DATE:
            date = parseDateExpressionToInt(*(boost::gregorian::date*)values[i]);
            appendValues(buf, (char*)&date, sizeof(int32_t));
            break;
        case TSDataType::TIMESTAMP:
        case TSDataType::INT64:
            appendValues(buf, values[i], sizeof(int64_t));
            break;
        case TSDataType::FLOAT:
            appendValues(buf, values[i], sizeof(float));
            break;
        case TSDataType::DOUBLE:
            appendValues(buf, values[i], sizeof(double));
            break;
        case TSDataType::STRING:
        case TSDataType::BLOB:
        case TSDataType::OBJECT:
        case TSDataType::TEXT: {
            int32_t len = (uint32_t)strlen(values[i]);
            appendValues(buf, (char*)(&len), sizeof(uint32_t));
            // no need to change the byte order of string value
            buf.append(values[i], len);
            break;
        }
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
    case TSDataType::TIMESTAMP:
        return 8;
    case TSDataType::DATE:
        return 9;
    case TSDataType::BLOB:
        return 10;
    case TSDataType::STRING:
        return 11;
    case TSDataType::OBJECT:
        return 12;
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
    if (!zoneId_.empty()) {
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
    zoneId_ = zoneStr;
}

void Session::initNodesSupplier(const std::vector<std::string>& nodeUrls) {
    std::vector<TEndPoint> endPoints;
    std::unordered_set<std::string> uniqueEndpoints;

    if (nodeUrls.empty() && host_.empty()) {
        throw IoTDBException("No available nodes");
    }

    // Process provided node URLs
    if (!nodeUrls.empty()) {
        for (auto& url : nodeUrls) {
            try {
                TEndPoint endPoint = UrlUtils::parseTEndPointIpv4AndIpv6Url(url);
                if (endPoint.port == 0) continue; // Skip invalid endpoints

                std::string endpointKey = endPoint.ip + ":" + std::to_string(endPoint.port);
                if (uniqueEndpoints.find(endpointKey) == uniqueEndpoints.end()) {
                    endPoints.emplace_back(std::move(endPoint));
                    uniqueEndpoints.insert(std::move(endpointKey));
                }
            } catch (...) {
                continue; // Skip malformed URLs
            }
        }
    }

    // Fallback to local endpoint if no valid endpoints found
    if (endPoints.empty()) {
        if (host_.empty() || rpcPort_ == 0) {
            throw IoTDBException("No valid endpoints available");
        }
        TEndPoint endPoint;
        endPoint.__set_ip(host_);
        endPoint.__set_port(rpcPort_);
        endPoints.emplace_back(std::move(endPoint));
    }

    if (enableAutoFetch_) {
        nodesSupplier_ = NodesSupplier::create(endPoints, username_, password_);
    }
    else {
        nodesSupplier_ = make_shared<StaticNodesSupplier>(endPoints);
    }
}

void Session::initDefaultSessionConnection() {
    // Try all endpoints from supplier until a connection is established.
    auto endpoints = nodesSupplier_->getEndPointList();
    bool connected = false;

    for (const auto& endpoint : endpoints) {
        try {
            host_ = endpoint.ip;
            rpcPort_ = endpoint.port;

            defaultEndPoint_.__set_ip(host_);
            defaultEndPoint_.__set_port(rpcPort_);

            defaultSessionConnection_ = std::make_shared<SessionConnection>(
                this, defaultEndPoint_, zoneId_, nodesSupplier_, fetchSize_,
                3,
                500, connectTimeoutMs_,
                sqlDialect_, database_);

            connected = true;
            break;
        } catch (const IoTDBException& e) {
            log_debug(e.what());
            throw;
        } catch (const std::exception& e) {
            log_warn(e.what());
        }
    }

    if (!connected) {
        throw std::runtime_error("No available node to establish SessionConnection.");
    }
}

void Session::insertStringRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
                                                 vector<vector<string>> measurementsList,
                                                 vector<vector<string>> valuesList, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertStringRecordsReq> recordsGroup;
    for (int i = 0; i < deviceIds.size(); i++) {
        auto connection = getSessionConnection(deviceIds[i]);
        if (recordsGroup.find(connection) == recordsGroup.end()) {
            TSInsertStringRecordsReq request;
            std::vector<std::string> emptyPrefixPaths;
            std::vector<std::vector<std::string>> emptyMeasurementsList;
            vector<vector<string>> emptyValuesList;
            std::vector<int64_t> emptyTimestamps;
            request.__set_isAligned(isAligned);
            request.__set_prefixPaths(emptyPrefixPaths);
            request.__set_timestamps(emptyTimestamps);
            request.__set_measurementsList(emptyMeasurementsList);
            request.__set_valuesList(emptyValuesList);
            recordsGroup.insert(make_pair(connection, request));
        }
        TSInsertStringRecordsReq& existingReq = recordsGroup[connection];
        existingReq.prefixPaths.emplace_back(deviceIds[i]);
        existingReq.timestamps.emplace_back(times[i]);
        existingReq.measurementsList.emplace_back(measurementsList[i]);
        existingReq.valuesList.emplace_back(valuesList[i]);
    }
    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertStringRecordsReq&)> consumer =
        [](const std::shared_ptr<SessionConnection>& c, const TSInsertStringRecordsReq& r) {
        c->insertStringRecords(r);
    };
    if (recordsGroup.size() == 1) {
        insertOnce(recordsGroup, consumer);
    }
    else {
        insertByGroup(recordsGroup, consumer);
    }
}

void Session::insertRecordsWithLeaderCache(vector<string> deviceIds, vector<int64_t> times,
                                           vector<vector<string>> measurementsList,
                                           const vector<vector<TSDataType::TSDataType>>& typesList,
                                           vector<vector<char*>> valuesList, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertRecordsReq> recordsGroup;
    for (int i = 0; i < deviceIds.size(); i++) {
        auto connection = getSessionConnection(deviceIds[i]);
        if (recordsGroup.find(connection) == recordsGroup.end()) {
            TSInsertRecordsReq request;
            std::vector<std::string> emptyPrefixPaths;
            std::vector<std::vector<std::string>> emptyMeasurementsList;
            std::vector<std::string> emptyValuesList;
            std::vector<int64_t> emptyTimestamps;
            request.__set_isAligned(isAligned);
            request.__set_prefixPaths(emptyPrefixPaths);
            request.__set_timestamps(emptyTimestamps);
            request.__set_measurementsList(emptyMeasurementsList);
            request.__set_valuesList(emptyValuesList);
            recordsGroup.insert(make_pair(connection, request));
        }
        TSInsertRecordsReq& existingReq = recordsGroup[connection];
        existingReq.prefixPaths.emplace_back(deviceIds[i]);
        existingReq.timestamps.emplace_back(times[i]);
        existingReq.measurementsList.emplace_back(measurementsList[i]);
        vector<string> bufferList;
        string buffer;
        putValuesIntoBuffer(typesList[i], valuesList[i], buffer);
        existingReq.valuesList.emplace_back(buffer);
        recordsGroup[connection] = existingReq;
    }
    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertRecordsReq&)> consumer =
        [](const std::shared_ptr<SessionConnection>& c, const TSInsertRecordsReq& r) {
        c->insertRecords(r);
    };
    if (recordsGroup.size() == 1) {
        insertOnce(recordsGroup, consumer);
    }
    else {
        insertByGroup(recordsGroup, consumer);
    }
}

void Session::insertTabletsWithLeaderCache(unordered_map<string, Tablet*> tablets, bool sorted, bool isAligned) {
    std::unordered_map<std::shared_ptr<SessionConnection>, TSInsertTabletsReq> tabletsGroup;
    if (tablets.empty()) {
        throw BatchExecutionException("No tablet is inserting!");
    }
    for (const auto& item : tablets) {
        if (isAligned != item.second->isAligned) {
            throw BatchExecutionException("The tablets should be all aligned or non-aligned!");
        }
        if (!checkSorted(*(item.second))) {
            sortTablet(*(item.second));
        }
        auto deviceId = item.first;
        auto tablet = item.second;
        auto connection = getSessionConnection(deviceId);
        auto it = tabletsGroup.find(connection);
        if (it == tabletsGroup.end()) {
            TSInsertTabletsReq request;
            tabletsGroup[connection] = request;
        }
        TSInsertTabletsReq& existingReq = tabletsGroup[connection];
        existingReq.prefixPaths.emplace_back(tablet->deviceId);
        existingReq.timestampsList.emplace_back(move(SessionUtils::getTime(*tablet)));
        existingReq.valuesList.emplace_back(move(SessionUtils::getValue(*tablet)));
        existingReq.sizeList.emplace_back(tablet->rowSize);
        vector<int> dataTypes;
        vector<string> measurements;
        for (pair<string, TSDataType::TSDataType> schema : tablet->schemas) {
            measurements.push_back(schema.first);
            dataTypes.push_back(schema.second);
        }
        existingReq.measurementsList.emplace_back(measurements);
        existingReq.typesList.emplace_back(dataTypes);
    }

    std::function<void(std::shared_ptr<SessionConnection>, const TSInsertTabletsReq&)> consumer =
        [](const std::shared_ptr<SessionConnection>& c, const TSInsertTabletsReq& r) {
        c->insertTablets(r);
    };
    if (tabletsGroup.size() == 1) {
        insertOnce(tabletsGroup, consumer);
    }
    else {
        insertByGroup(tabletsGroup, consumer);
    }
}

void Session::open() {
    open(false, DEFAULT_TIMEOUT_MS);
}

void Session::open(bool enableRPCCompression) {
    open(enableRPCCompression, DEFAULT_TIMEOUT_MS);
}

void Session::open(bool enableRPCCompression, int connectionTimeoutInMs) {
    if (!isClosed_) {
        return;
    }

    try {
        initDefaultSessionConnection();
    }
    catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
    zoneId_ = defaultSessionConnection_->zoneId;

    if (enableRedirection_) {
        endPointToSessionConnection.insert(make_pair(defaultEndPoint_, defaultSessionConnection_));
    }

    isClosed_ = false;
}


void Session::close() {
    if (isClosed_) {
        return;
    }
    isClosed_ = true;
}


void Session::insertRecord(const string& deviceId, int64_t time,
                           const vector<string>& measurements,
                           const vector<string>& values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(false);
    try {
        getSessionConnection(deviceId)->insertStringRecord(req);
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertStringRecord(req);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertRecord(const string& deviceId, int64_t time,
                           const vector<string>& measurements,
                           const vector<TSDataType::TSDataType>& types,
                           const vector<char*>& values) {
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
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertRecord(req);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertAlignedRecord(const string& deviceId, int64_t time,
                                  const vector<string>& measurements,
                                  const vector<string>& values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    req.__set_isAligned(true);
    try {
        getSessionConnection(deviceId)->insertStringRecord(req);
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertStringRecord(req);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertAlignedRecord(const string& deviceId, int64_t time,
                                  const vector<string>& measurements,
                                  const vector<TSDataType::TSDataType>& types,
                                  const vector<char*>& values) {
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
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertRecord(req);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertRecords(const vector<string>& deviceIds,
                            const vector<int64_t>& times,
                            const vector<vector<string>>& measurementsList,
                            const vector<vector<string>>& valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }

    if (enableRedirection_) {
        insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, false);
    }
    else {
        TSInsertStringRecordsReq request;
        request.__set_prefixPaths(deviceIds);
        request.__set_timestamps(times);
        request.__set_measurementsList(measurementsList);
        request.__set_valuesList(valuesList);
        request.__set_isAligned(false);
        try {
            defaultSessionConnection_->insertStringRecords(request);
        }
        catch (RedirectException& e) {
        }
    }
}

void Session::insertRecords(const vector<string>& deviceIds,
                            const vector<int64_t>& times,
                            const vector<vector<string>>& measurementsList,
                            const vector<vector<TSDataType::TSDataType>>& typesList,
                            const vector<vector<char*>>& valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }

    if (enableRedirection_) {
        insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList, false);
    }
    else {
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
            defaultSessionConnection_->insertRecords(request);
        }
        catch (RedirectException& e) {
        }
    }
}

void Session::insertAlignedRecords(const vector<string>& deviceIds,
                                   const vector<int64_t>& times,
                                   const vector<vector<string>>& measurementsList,
                                   const vector<vector<string>>& valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }

    if (enableRedirection_) {
        insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, true);
    }
    else {
        TSInsertStringRecordsReq request;
        request.__set_prefixPaths(deviceIds);
        request.__set_timestamps(times);
        request.__set_measurementsList(measurementsList);
        request.__set_valuesList(valuesList);
        request.__set_isAligned(true);
        try {
            defaultSessionConnection_->insertStringRecords(request);
        }
        catch (RedirectException& e) {
        }
    }
}

void Session::insertAlignedRecords(const vector<string>& deviceIds,
                                   const vector<int64_t>& times,
                                   const vector<vector<string>>& measurementsList,
                                   const vector<vector<TSDataType::TSDataType>>& typesList,
                                   const vector<vector<char*>>& valuesList) {
    size_t len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
        logic_error e("deviceIds, times, measurementsList and valuesList's size should be equal");
        throw exception(e);
    }

    if (enableRedirection_) {
        insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList, true);
    }
    else {
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
            defaultSessionConnection_->insertRecords(request);
        }
        catch (RedirectException& e) {
        }
    }
}

void Session::insertRecordsOfOneDevice(const string& deviceId,
                                       vector<int64_t>& times,
                                       vector<vector<string>>& measurementsList,
                                       vector<vector<TSDataType::TSDataType>>& typesList,
                                       vector<vector<char*>>& valuesList) {
    insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
}

void Session::insertRecordsOfOneDevice(const string& deviceId,
                                       vector<int64_t>& times,
                                       vector<vector<string>>& measurementsList,
                                       vector<vector<TSDataType::TSDataType>>& typesList,
                                       vector<vector<char*>>& valuesList,
                                       bool sorted) {
    if (!checkSorted(times)) {
        int* index = new int[times.size()];
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
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertRecordsOfOneDevice(request);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertAlignedRecordsOfOneDevice(const string& deviceId,
                                              vector<int64_t>& times,
                                              vector<vector<string>>& measurementsList,
                                              vector<vector<TSDataType::TSDataType>>& typesList,
                                              vector<vector<char*>>& valuesList) {
    insertAlignedRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
}

void Session::insertAlignedRecordsOfOneDevice(const string& deviceId,
                                              vector<int64_t>& times,
                                              vector<vector<string>>& measurementsList,
                                              vector<vector<TSDataType::TSDataType>>& typesList,
                                              vector<vector<char*>>& valuesList,
                                              bool sorted) {
    if (!checkSorted(times)) {
        int* index = new int[times.size()];
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
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertRecordsOfOneDevice(request);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertTablet(Tablet& tablet) {
    try {
        insertTablet(tablet, false);
    }
    catch (const exception& e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::buildInsertTabletReq(TSInsertTabletReq& request, Tablet& tablet, bool sorted) {
    if ((!sorted) && !checkSorted(tablet)) {
        sortTablet(tablet);
    }

    request.__set_prefixPath(tablet.deviceId);

    std::vector<std::string> reqMeasurements;
    reqMeasurements.reserve(tablet.schemas.size());
    std::vector<int32_t> types;
    types.reserve(tablet.schemas.size());
    for (pair<string, TSDataType::TSDataType> schema : tablet.schemas) {
        reqMeasurements.push_back(schema.first);
        types.push_back(schema.second);
    }
    request.__set_measurements(reqMeasurements);
    request.__set_types(types);
    request.__set_values(SessionUtils::getValue(tablet));
    request.__set_timestamps(SessionUtils::getTime(tablet));
    request.__set_size(tablet.rowSize);
    request.__set_isAligned(tablet.isAligned);
}

void Session::insertTablet(TSInsertTabletReq request) {
    auto deviceId = request.prefixPath;
    try {
        getSessionConnection(deviceId)->insertTablet(request);
    }
    catch (RedirectException& e) {
        handleRedirection(deviceId, e.endPoint);
    } catch (const IoTDBConnectionException& e) {
        if (enableRedirection_ && deviceIdToEndpoint.find(deviceId) != deviceIdToEndpoint.end()) {
            deviceIdToEndpoint.erase(deviceId);
            try {
                defaultSessionConnection_->insertTablet(request);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw e;
        }
    }
}

void Session::insertTablet(Tablet& tablet, bool sorted) {
    TSInsertTabletReq request;
    buildInsertTabletReq(request, tablet, sorted);
    insertTablet(request);
}

void Session::insertRelationalTablet(Tablet& tablet, bool sorted) {
    std::unordered_map<std::shared_ptr<SessionConnection>, Tablet> relationalTabletGroup;
    if (tableModelDeviceIdToEndpoint.empty()) {
        relationalTabletGroup.insert(make_pair(defaultSessionConnection_, tablet));
    }
    else if (SessionUtils::isTabletContainsSingleDevice(tablet)) {
        relationalTabletGroup.insert(make_pair(getSessionConnection(tablet.getDeviceID(0)), tablet));
    }
    else {
        for (int row = 0; row < tablet.rowSize; row++) {
            auto iDeviceID = tablet.getDeviceID(row);
            std::shared_ptr<SessionConnection> connection = getSessionConnection(iDeviceID);

            auto it = relationalTabletGroup.find(connection);
            if (it == relationalTabletGroup.end()) {
                Tablet newTablet(tablet.deviceId, tablet.schemas, tablet.columnTypes, tablet.rowSize);
                it = relationalTabletGroup.insert(std::make_pair(connection, newTablet)).first;
            }

            Tablet& currentTablet = it->second;
            int rowIndex = currentTablet.rowSize++;
            currentTablet.timestamps[rowIndex] = tablet.timestamps[row];
            for (int col = 0; col < tablet.schemas.size(); col++) {
                switch (tablet.schemas[col].second) {
                case TSDataType::BOOLEAN:
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(bool*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                case TSDataType::INT32:
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(int32_t*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                case TSDataType::INT64:
                case TSDataType::TIMESTAMP:
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(int64_t*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                case TSDataType::FLOAT:
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(float*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                case TSDataType::DOUBLE:
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(double*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                case TSDataType::DATE: {
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(boost::gregorian::date*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                }
                case TSDataType::STRING:
                case TSDataType::TEXT:
                case TSDataType::OBJECT:
                case TSDataType::BLOB: {
                    currentTablet.addValue(tablet.schemas[col].first, rowIndex,
                        *(string*)tablet.getValue(col, row, tablet.schemas[col].second));
                    break;
                }
                default:
                    break;
                }

            }
        }
    }
    if (relationalTabletGroup.size() == 1) {
        insertRelationalTabletOnce(relationalTabletGroup, sorted);
    }
    else {
        insertRelationalTabletByGroup(relationalTabletGroup, sorted);
    }
}

void Session::insertRelationalTablet(Tablet& tablet) {
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
    for (auto& category : tablet.columnTypes) {
        columnCategories.push_back(static_cast<int8_t>(category));
    }
    request.__set_columnCategories(columnCategories);
    try {
        TSStatus respStatus;
        connection->getSessionClient()->insertTablet(respStatus, request);
        RpcUtils::verifySuccess(respStatus);
    }
    catch (RedirectException& e) {
        auto endPointList = e.endPointList;
        for (int i = 0; i < endPointList.size(); i++) {
            auto deviceID = tablet.getDeviceID(i);
            handleRedirection(deviceID, endPointList[i]);
        }
    } catch (const IoTDBConnectionException& e) {
        if (endPointToSessionConnection.size() > 1) {
            removeBrokenSessionConnection(connection);
            try {
                TSStatus respStatus;
                defaultSessionConnection_->getSessionClient()->insertTablet(respStatus, request);
                RpcUtils::verifySuccess(respStatus);
            }
            catch (RedirectException& e) {
            }
        }
        else {
            throw IoTDBConnectionException(e.what());
        }
    } catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (const exception& e) {
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
            for (auto& category : tablet.columnTypes) {
                columnCategories.push_back(static_cast<int8_t>(category));
            }
            request.__set_columnCategories(columnCategories);

            try {
                TSStatus respStatus;
                connection->getSessionClient()->insertTablet(respStatus, request);
                RpcUtils::verifySuccess(respStatus);
            }
            catch (const TTransportException& e) {
                log_debug(e.what());
                throw IoTDBConnectionException(e.what());
            } catch (const IoTDBException& e) {
                log_debug(e.what());
                throw;
            } catch (const exception& e) {
                log_debug(e.what());
                throw IoTDBException(e.what());
            }
        }));
    }

    for (auto& f : futures) {
        f.get();
    }
}

void Session::insertAlignedTablet(Tablet& tablet) {
    insertAlignedTablet(tablet, false);
}

void Session::insertAlignedTablet(Tablet& tablet, bool sorted) {
    tablet.setAligned(true);
    try {
        insertTablet(tablet, sorted);
    }
    catch (const exception& e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet*>& tablets) {
    try {
        insertTablets(tablets, false);
    }
    catch (const exception& e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::insertTablets(unordered_map<string, Tablet*>& tablets, bool sorted) {
    if (tablets.empty()) {
        throw BatchExecutionException("No tablet is inserting!");
    }
    auto beginIter = tablets.begin();
    bool isAligned = ((*beginIter).second)->isAligned;
    if (enableRedirection_) {
        insertTabletsWithLeaderCache(tablets, sorted, isAligned);
    }
    else {
        TSInsertTabletsReq request;
        for (const auto& item : tablets) {
            if (isAligned != item.second->isAligned) {
                throw BatchExecutionException("The tablets should be all aligned or non-aligned!");
            }
            if (!checkSorted(*(item.second))) {
                sortTablet(*(item.second));
            }
            request.prefixPaths.push_back(item.second->deviceId);
            vector<string> measurements;
            vector<int> dataTypes;
            for (pair<string, TSDataType::TSDataType> schema : item.second->schemas) {
                measurements.push_back(schema.first);
                dataTypes.push_back(schema.second);
            }
            request.measurementsList.push_back(measurements);
            request.typesList.push_back(dataTypes);
            request.timestampsList.push_back(move(SessionUtils::getTime(*(item.second))));
            request.valuesList.push_back(move(SessionUtils::getValue(*(item.second))));
            request.sizeList.push_back(item.second->rowSize);
        }
        request.__set_isAligned(isAligned);
        try {
            TSStatus respStatus;
            defaultSessionConnection_->insertTablets(request);
            RpcUtils::verifySuccess(respStatus);
        }
        catch (RedirectException& e) {
        }
    }
}


void Session::insertAlignedTablets(unordered_map<string, Tablet*>& tablets, bool sorted) {
    for (auto iter = tablets.begin(); iter != tablets.end(); iter++) {
        iter->second->setAligned(true);
    }
    try {
        insertTablets(tablets, sorted);
    }
    catch (const exception& e) {
        log_debug(e.what());
        logic_error error(e.what());
        throw exception(error);
    }
}

void Session::testInsertRecord(const string& deviceId, int64_t time, const vector<string>& measurements,
                               const vector<string>& values) {
    TSInsertStringRecordReq req;
    req.__set_prefixPath(deviceId);
    req.__set_timestamp(time);
    req.__set_measurements(measurements);
    req.__set_values(values);
    TSStatus tsStatus;
    try {
        defaultSessionConnection_->testInsertStringRecord(req);
        RpcUtils::verifySuccess(tsStatus);
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::testInsertTablet(const Tablet& tablet) {
    TSInsertTabletReq request;
    request.prefixPath = tablet.deviceId;
    for (pair<string, TSDataType::TSDataType> schema : tablet.schemas) {
        request.measurements.push_back(schema.first);
        request.types.push_back(schema.second);
    }
    request.__set_timestamps(move(SessionUtils::getTime(tablet)));
    request.__set_values(move(SessionUtils::getValue(tablet)));
    request.__set_size(tablet.rowSize);
    try {
        TSStatus tsStatus;
        defaultSessionConnection_->testInsertTablet(request);
        RpcUtils::verifySuccess(tsStatus);
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::testInsertRecords(const vector<string>& deviceIds,
                                const vector<int64_t>& times,
                                const vector<vector<string>>& measurementsList,
                                const vector<vector<string>>& valuesList) {
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
        defaultSessionConnection_->getSessionClient()->insertStringRecords(tsStatus, request);
        RpcUtils::verifySuccess(tsStatus);
    }
    catch (const TTransportException& e) {
        log_debug(e.what());
        throw IoTDBConnectionException(e.what());
    } catch (const IoTDBException& e) {
        log_debug(e.what());
        throw;
    } catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::deleteTimeseries(const string& path) {
    vector<string> paths;
    paths.push_back(path);
    deleteTimeseries(paths);
}

void Session::deleteTimeseries(const vector<string>& paths) {
    defaultSessionConnection_->deleteTimeseries(paths);
}

void Session::deleteData(const string& path, int64_t endTime) {
    vector<string> paths;
    paths.push_back(path);
    deleteData(paths, LONG_LONG_MIN, endTime);
}

void Session::deleteData(const vector<string>& paths, int64_t endTime) {
    deleteData(paths, LONG_LONG_MIN, endTime);
}

void Session::deleteData(const vector<string>& paths, int64_t startTime, int64_t endTime) {
    TSDeleteDataReq req;
    req.__set_paths(paths);
    req.__set_startTime(startTime);
    req.__set_endTime(endTime);
    defaultSessionConnection_->deleteData(req);
}

void Session::setStorageGroup(const string& storageGroupId) {
    defaultSessionConnection_->setStorageGroup(storageGroupId);
}

void Session::deleteStorageGroup(const string& storageGroup) {
    vector<string> storageGroups;
    storageGroups.push_back(storageGroup);
    deleteStorageGroups(storageGroups);
}

void Session::deleteStorageGroups(const vector<string>& storageGroups) {
    defaultSessionConnection_->deleteStorageGroups(storageGroups);
}

void Session::createDatabase(const string& database) {
    this->setStorageGroup(database);
}

void Session::deleteDatabase(const string& database) {
    this->deleteStorageGroups(vector<string>{database});
}

void Session::deleteDatabases(const vector<string>& databases) {
    this->deleteStorageGroups(databases);
}

void Session::createTimeseries(const string& path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor) {
    try {
        createTimeseries(path, dataType, encoding, compressor, nullptr, nullptr, nullptr, "");
    }
    catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

void Session::createTimeseries(const string& path,
                               TSDataType::TSDataType dataType,
                               TSEncoding::TSEncoding encoding,
                               CompressionType::CompressionType compressor,
                               map<string, string>* props,
                               map<string, string>* tags,
                               map<string, string>* attributes,
                               const string& measurementAlias) {
    TSCreateTimeseriesReq req;
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
    defaultSessionConnection_->createTimeseries(req);
}

void Session::createMultiTimeseries(const vector<string>& paths,
                                    const vector<TSDataType::TSDataType>& dataTypes,
                                    const vector<TSEncoding::TSEncoding>& encodings,
                                    const vector<CompressionType::CompressionType>& compressors,
                                    vector<map<string, string>>* propsList,
                                    vector<map<string, string>>* tagsList,
                                    vector<map<string, string>>* attributesList,
                                    vector<string>* measurementAliasList) {
    TSCreateMultiTimeseriesReq request;
    request.__set_paths(paths);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType : dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding : encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor : compressors) {
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

    defaultSessionConnection_->createMultiTimeseries(request);
}

void Session::createAlignedTimeseries(const std::string& deviceId,
                                      const std::vector<std::string>& measurements,
                                      const std::vector<TSDataType::TSDataType>& dataTypes,
                                      const std::vector<TSEncoding::TSEncoding>& encodings,
                                      const std::vector<CompressionType::CompressionType>& compressors) {
    TSCreateAlignedTimeseriesReq request;
    request.__set_prefixPath(deviceId);
    request.__set_measurements(measurements);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType : dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    request.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding : encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    request.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor : compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    request.__set_compressors(compressorsOrdinal);

    defaultSessionConnection_->createAlignedTimeseries(request);
}

bool Session::checkTimeseriesExists(const string& path) {
    try {
        std::unique_ptr<SessionDataSet> dataset = executeQueryStatement("SHOW TIMESERIES " + path);
        if (dataset == nullptr) {
            throw IoTDBException("executeQueryStatement failed");
        }
        bool isExisted = dataset->hasNext();
        dataset->closeOperationHandle();
        return isExisted;
    }
    catch (const exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}

shared_ptr<SessionConnection> Session::getQuerySessionConnection() {
    auto endPoint = nodesSupplier_->getQueryEndPoint();
    if (!endPoint.is_initialized() || endPointToSessionConnection.empty()) {
        return defaultSessionConnection_;
    }

    auto it = endPointToSessionConnection.find(endPoint.value());
    if (it != endPointToSessionConnection.end()) {
        return it->second;
    }

    shared_ptr<SessionConnection> newConnection;
    try {
        newConnection = make_shared<SessionConnection>(this, endPoint.value(), zoneId_, nodesSupplier_,
                                                       fetchSize_, 60, 500, connectTimeoutMs_, sqlDialect_, database_);
        endPointToSessionConnection.emplace(endPoint.value(), newConnection);
        return newConnection;
    }
    catch (exception& e) {
        log_debug("Session::getQuerySessionConnection() exception: " + e.what());
        return newConnection;
    }
}

shared_ptr<SessionConnection> Session::getSessionConnection(std::string deviceId) {
    if (!enableRedirection_ ||
        deviceIdToEndpoint.find(deviceId) == deviceIdToEndpoint.end() ||
        endPointToSessionConnection.find(deviceIdToEndpoint[deviceId]) == endPointToSessionConnection.end()) {
        return defaultSessionConnection_;
    }
    return endPointToSessionConnection.find(deviceIdToEndpoint[deviceId])->second;
}

shared_ptr<SessionConnection> Session::getSessionConnection(std::shared_ptr<storage::IDeviceID> deviceId) {
    if (!enableRedirection_ ||
        tableModelDeviceIdToEndpoint.find(deviceId) == tableModelDeviceIdToEndpoint.end() ||
        endPointToSessionConnection.find(tableModelDeviceIdToEndpoint[deviceId]) == endPointToSessionConnection.end()) {
        return defaultSessionConnection_;
    }
    return endPointToSessionConnection.find(tableModelDeviceIdToEndpoint[deviceId])->second;
}

string Session::getTimeZone() {
    auto ret = defaultSessionConnection_->getTimeZone();
    return ret.timeZone;
}

void Session::setTimeZone(const string& zoneId) {
    TSSetTimeZoneReq req;
    req.__set_sessionId(defaultSessionConnection_->sessionId);
    req.__set_timeZone(zoneId);
    defaultSessionConnection_->setTimeZone(req);
}

unique_ptr<SessionDataSet> Session::executeQueryStatement(const string& sql) {
    return executeQueryStatementMayRedirect(sql, QUERY_TIMEOUT_MS);
}

unique_ptr<SessionDataSet> Session::executeQueryStatement(const string& sql, int64_t timeoutInMs) {
    return executeQueryStatementMayRedirect(sql, timeoutInMs);
}

void Session::handleQueryRedirection(TEndPoint endPoint) {
    if (!enableRedirection_) return;
    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    }
    else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId_, nodesSupplier_,
                                                           fetchSize_, 60, 500, connectTimeoutMs_, sqlDialect_, database_);

            endPointToSessionConnection.emplace(endPoint, newConnection);
        }
        catch (exception& e) {
            throw IoTDBConnectionException(e.what());
        }
    }
    defaultSessionConnection_ = newConnection;
}

void Session::handleRedirection(const std::string& deviceId, TEndPoint endPoint) {
    if (!enableRedirection_) return;
    if (endPoint.ip == "127.0.0.1") return;
    deviceIdToEndpoint[deviceId] = endPoint;

    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    }
    else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId_, nodesSupplier_,
                                                           fetchSize_, 60, 500, 1000, sqlDialect_, database_);
            endPointToSessionConnection.emplace(endPoint, newConnection);
        }
        catch (exception& e) {
            deviceIdToEndpoint.erase(deviceId);
            throw IoTDBConnectionException(e.what());
        }
    }
}

void Session::handleRedirection(const std::shared_ptr<storage::IDeviceID>& deviceId, TEndPoint endPoint) {
    if (!enableRedirection_) return;
    if (endPoint.ip == "127.0.0.1") return;
    tableModelDeviceIdToEndpoint[deviceId] = endPoint;

    shared_ptr<SessionConnection> newConnection;
    auto it = endPointToSessionConnection.find(endPoint);
    if (it != endPointToSessionConnection.end()) {
        newConnection = it->second;
    }
    else {
        try {
            newConnection = make_shared<SessionConnection>(this, endPoint, zoneId_, nodesSupplier_,
                                                           fetchSize_, 3, 500, connectTimeoutMs_, sqlDialect_, database_);
            endPointToSessionConnection.emplace(endPoint, newConnection);
        }
        catch (exception& e) {
            tableModelDeviceIdToEndpoint.erase(deviceId);
            throw IoTDBConnectionException(e.what());
        }
    }
}

std::unique_ptr<SessionDataSet> Session::executeQueryStatementMayRedirect(const std::string& sql, int64_t timeoutInMs) {
    auto sessionConnection = getQuerySessionConnection();
    if (!sessionConnection) {
        log_warn("Session connection not found");
        return nullptr;
    }
    try {
        return sessionConnection->executeQueryStatement(sql, timeoutInMs);
    }
    catch (RedirectException& e) {
        log_warn("Session connection redirect exception: " + e.what());
        handleQueryRedirection(e.endPoint);
        try {
            return defaultSessionConnection_->executeQueryStatement(sql, timeoutInMs);
        }
        catch (exception& e) {
            log_error("Exception while executing redirected query statement: %s", e.what());
            throw ExecutionException(e.what());
        }
    } catch (exception& e) {
        log_error("Exception while executing query statement: %s", e.what());
        throw e;
    }
}

void Session::executeNonQueryStatement(const string& sql) {
    try {
        defaultSessionConnection_->executeNonQueryStatement(sql);
    }
    catch (const exception& e) {
        throw IoTDBException(e.what());
    }
}

unique_ptr<SessionDataSet>
Session::executeRawDataQuery(const vector<string>& paths, int64_t startTime, int64_t endTime) {
    return defaultSessionConnection_->executeRawDataQuery(paths, startTime, endTime);
}


unique_ptr<SessionDataSet> Session::executeLastDataQuery(const vector<string>& paths) {
    return executeLastDataQuery(paths, LONG_LONG_MIN);
}

unique_ptr<SessionDataSet> Session::executeLastDataQuery(const vector<string>& paths, int64_t lastTime) {
    return defaultSessionConnection_->executeLastDataQuery(paths, lastTime);
}

void Session::createSchemaTemplate(const Template& templ) {
    TSCreateSchemaTemplateReq req;
    req.__set_name(templ.getName());
    req.__set_serializedTemplate(templ.serialize());
    defaultSessionConnection_->createSchemaTemplate(req);
}

void Session::setSchemaTemplate(const string& template_name, const string& prefix_path) {
    TSSetSchemaTemplateReq req;
    req.__set_templateName(template_name);
    req.__set_prefixPath(prefix_path);
    defaultSessionConnection_->setSchemaTemplate(req);
}

void Session::unsetSchemaTemplate(const string& prefix_path, const string& template_name) {
    TSUnsetSchemaTemplateReq req;
    req.__set_templateName(template_name);
    req.__set_prefixPath(prefix_path);
    defaultSessionConnection_->unsetSchemaTemplate(req);
}

void Session::addAlignedMeasurementsInTemplate(const string& template_name, const vector<std::string>& measurements,
                                               const vector<TSDataType::TSDataType>& dataTypes,
                                               const vector<TSEncoding::TSEncoding>& encodings,
                                               const vector<CompressionType::CompressionType>& compressors) {
    TSAppendSchemaTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurements(measurements);
    req.__set_isAligned(true);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType : dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding : encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor : compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req.__set_compressors(compressorsOrdinal);

    defaultSessionConnection_->appendSchemaTemplate(req);
}

void Session::addAlignedMeasurementsInTemplate(const string& template_name, const string& measurement,
                                               TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                                               CompressionType::CompressionType compressor) {
    vector<std::string> measurements(1, measurement);
    vector<TSDataType::TSDataType> dataTypes(1, dataType);
    vector<TSEncoding::TSEncoding> encodings(1, encoding);
    vector<CompressionType::CompressionType> compressors(1, compressor);
    addAlignedMeasurementsInTemplate(template_name, measurements, dataTypes, encodings, compressors);
}

void Session::addUnalignedMeasurementsInTemplate(const string& template_name, const vector<std::string>& measurements,
                                                 const vector<TSDataType::TSDataType>& dataTypes,
                                                 const vector<TSEncoding::TSEncoding>& encodings,
                                                 const vector<CompressionType::CompressionType>& compressors) {
    TSAppendSchemaTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurements(measurements);
    req.__set_isAligned(false);

    vector<int> dataTypesOrdinal;
    dataTypesOrdinal.reserve(dataTypes.size());
    for (TSDataType::TSDataType dataType : dataTypes) {
        dataTypesOrdinal.push_back(dataType);
    }
    req.__set_dataTypes(dataTypesOrdinal);

    vector<int> encodingsOrdinal;
    encodingsOrdinal.reserve(encodings.size());
    for (TSEncoding::TSEncoding encoding : encodings) {
        encodingsOrdinal.push_back(encoding);
    }
    req.__set_encodings(encodingsOrdinal);

    vector<int> compressorsOrdinal;
    compressorsOrdinal.reserve(compressors.size());
    for (CompressionType::CompressionType compressor : compressors) {
        compressorsOrdinal.push_back(compressor);
    }
    req.__set_compressors(compressorsOrdinal);

    defaultSessionConnection_->appendSchemaTemplate(req);
}

void Session::addUnalignedMeasurementsInTemplate(const string& template_name, const string& measurement,
                                                 TSDataType::TSDataType dataType, TSEncoding::TSEncoding encoding,
                                                 CompressionType::CompressionType compressor) {
    vector<std::string> measurements(1, measurement);
    vector<TSDataType::TSDataType> dataTypes(1, dataType);
    vector<TSEncoding::TSEncoding> encodings(1, encoding);
    vector<CompressionType::CompressionType> compressors(1, compressor);
    addUnalignedMeasurementsInTemplate(template_name, measurements, dataTypes, encodings, compressors);
}

void Session::deleteNodeInTemplate(const string& template_name, const string& path) {
    TSPruneSchemaTemplateReq req;
    req.__set_name(template_name);
    req.__set_path(path);
    defaultSessionConnection_->pruneSchemaTemplate(req);
}

int Session::countMeasurementsInTemplate(const string& template_name) {
    TSQueryTemplateReq req;
    req.__set_name(template_name);
    req.__set_queryType(TemplateQueryType::COUNT_MEASUREMENTS);
    TSQueryTemplateResp resp = defaultSessionConnection_->querySchemaTemplate(req);
    return resp.count;
}

bool Session::isMeasurementInTemplate(const string& template_name, const string& path) {
    TSQueryTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurement(path);
    req.__set_queryType(TemplateQueryType::IS_MEASUREMENT);
    TSQueryTemplateResp resp = defaultSessionConnection_->querySchemaTemplate(req);
    return resp.result;
}

bool Session::isPathExistInTemplate(const string& template_name, const string& path) {
    TSQueryTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurement(path);
    req.__set_queryType(TemplateQueryType::PATH_EXIST);
    TSQueryTemplateResp resp = defaultSessionConnection_->querySchemaTemplate(req);
    return resp.result;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string& template_name) {
    TSQueryTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurement("");
    req.__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    TSQueryTemplateResp resp = defaultSessionConnection_->querySchemaTemplate(req);
    return resp.measurements;
}

std::vector<std::string> Session::showMeasurementsInTemplate(const string& template_name, const string& pattern) {
    TSQueryTemplateReq req;
    req.__set_name(template_name);
    req.__set_measurement(pattern);
    req.__set_queryType(TemplateQueryType::SHOW_MEASUREMENTS);
    TSQueryTemplateResp resp = defaultSessionConnection_->querySchemaTemplate(req);
    return resp.measurements;
}

bool Session::checkTemplateExists(const string& template_name) {
    try {
        std::unique_ptr<SessionDataSet> dataset = executeQueryStatement(
            "SHOW NODES IN DEVICE TEMPLATE " + template_name);
        bool isExisted = dataset->hasNext();
        dataset->closeOperationHandle();
        return isExisted;
    }
    catch (const exception& e) {
        if (strstr(e.what(), "does not exist") != NULL) {
            return false;
        }
        log_debug(e.what());
        throw IoTDBException(e.what());
    }
}
