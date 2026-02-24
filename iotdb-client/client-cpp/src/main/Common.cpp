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

#include "Common.h"
#include <boost/date_time/gregorian/gregorian.hpp>

int32_t parseDateExpressionToInt(const boost::gregorian::date& date) {
    if (date.is_not_a_date()) {
        throw IoTDBException("Date expression is null or empty.");
    }

    const int year = date.year();
    if (year < 1000 || year > 9999) {
        throw DateTimeParseException(
            "Year must be between 1000 and 9999.",
            boost::gregorian::to_iso_extended_string(date),
            0
        );
    }

    const int64_t result = static_cast<int64_t>(year) * 10000 +
        date.month() * 100 +
        date.day();
    if (result > INT32_MAX || result < INT32_MIN) {
        throw DateTimeParseException(
            "Date value overflow. ",
            boost::gregorian::to_iso_extended_string(date),
            0
        );
    }
    return static_cast<int32_t>(result);
}

boost::gregorian::date parseIntToDate(int32_t dateInt) {
    if (dateInt == EMPTY_DATE_INT) {
        return boost::gregorian::date(boost::date_time::not_a_date_time);
    }
    int year = dateInt / 10000;
    int month = (dateInt % 10000) / 100;
    int day = dateInt % 100;
    return boost::gregorian::date(year, month, day);
}

std::string getTimePrecision(int32_t timeFactor) {
    if (timeFactor >= 1000000) return "us";
    if (timeFactor >= 1000) return "ms";
    return "s";
}

std::string formatDatetime(const std::string& format, const std::string& precision,
                           int64_t timestamp, const std::string& zoneId) {
    // Simplified implementation - in real code you'd use proper timezone handling
    std::time_t time = static_cast<std::time_t>(timestamp);
    std::tm* tm = std::localtime(&time);
    char buffer[80];
    strftime(buffer, sizeof(buffer), format.c_str(), tm);
    return std::string(buffer);
}

std::tm convertToTimestamp(int64_t value, int32_t timeFactor) {
    std::time_t time = static_cast<std::time_t>(value / timeFactor);
    return *std::localtime(&time);
}

TSDataType::TSDataType getDataTypeByStr(const std::string& typeStr) {
    if (typeStr == "BOOLEAN") return TSDataType::BOOLEAN;
    if (typeStr == "INT32") return TSDataType::INT32;
    if (typeStr == "INT64") return TSDataType::INT64;
    if (typeStr == "FLOAT") return TSDataType::FLOAT;
    if (typeStr == "DOUBLE") return TSDataType::DOUBLE;
    if (typeStr == "TEXT") return TSDataType::TEXT;
    if (typeStr == "TIMESTAMP") return TSDataType::TIMESTAMP;
    if (typeStr == "DATE") return TSDataType::DATE;
    if (typeStr == "BLOB") return TSDataType::BLOB;
    if (typeStr == "STRING") return TSDataType::STRING;
    if (typeStr == "OBJECT") return TSDataType::OBJECT;
    return TSDataType::UNKNOWN;
}

std::tm int32ToDate(int32_t value) {
    // Convert days since epoch (1970-01-01) to tm struct
    std::time_t time = static_cast<std::time_t>(value) * 86400; // seconds per day
    return *std::localtime(&time);
}

void RpcUtils::verifySuccess(const TSStatus& status) {
    if (status.code == TSStatusCode::MULTIPLE_ERROR) {
        verifySuccess(status.subStatus);
        return;
    }
    if (status.code != TSStatusCode::SUCCESS_STATUS
        && status.code != TSStatusCode::REDIRECTION_RECOMMEND) {
        throw ExecutionException(to_string(status.code) + ": " + status.message, status);
    }
}

void RpcUtils::verifySuccessWithRedirection(const TSStatus& status) {
    verifySuccess(status);
    if (status.__isset.redirectNode) {
        throw RedirectException(to_string(status.code) + ": " + status.message, status.redirectNode);
    }
    if (status.__isset.subStatus) {
        auto statusSubStatus = status.subStatus;
        vector<TEndPoint> endPointList(statusSubStatus.size());
        int count = 0;
        for (TSStatus subStatus : statusSubStatus) {
            if (subStatus.__isset.redirectNode) {
                endPointList[count++] = subStatus.redirectNode;
            }
            else {
                TEndPoint endPoint;
                endPointList[count++] = endPoint;
            }
        }
        if (!endPointList.empty()) {
            throw RedirectException(to_string(status.code) + ": " + status.message, endPointList);
        }
    }
}

void RpcUtils::verifySuccessWithRedirectionForMultiDevices(const TSStatus& status, vector<string> devices) {
    verifySuccess(status);

    if (status.code == TSStatusCode::MULTIPLE_ERROR
        || status.code == TSStatusCode::REDIRECTION_RECOMMEND) {
        map<string, TEndPoint> deviceEndPointMap;
        vector<TSStatus> statusSubStatus;
        for (int i = 0; i < statusSubStatus.size(); i++) {
            TSStatus subStatus = statusSubStatus[i];
            if (subStatus.__isset.redirectNode) {
                deviceEndPointMap.insert(make_pair(devices[i], subStatus.redirectNode));
            }
        }
        throw RedirectException(to_string(status.code) + ": " + status.message, deviceEndPointMap);
    }

    if (status.__isset.redirectNode) {
        throw RedirectException(to_string(status.code) + ": " + status.message, status.redirectNode);
    }
    if (status.__isset.subStatus) {
        auto statusSubStatus = status.subStatus;
        vector<TEndPoint> endPointList(statusSubStatus.size());
        int count = 0;
        for (TSStatus subStatus : statusSubStatus) {
            if (subStatus.__isset.redirectNode) {
                endPointList[count++] = subStatus.redirectNode;
            }
            else {
                TEndPoint endPoint;
                endPointList[count++] = endPoint;
            }
        }
        if (!endPointList.empty()) {
            throw RedirectException(to_string(status.code) + ": " + status.message, endPointList);
        }
    }
}

void RpcUtils::verifySuccess(const vector<TSStatus>& statuses) {
    for (const TSStatus& status : statuses) {
        if (status.code != TSStatusCode::SUCCESS_STATUS) {
            throw BatchExecutionException(status.message, statuses);
        }
    }
}

TSStatus RpcUtils::getStatus(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status;
    status.__set_code(tsStatusCode);
    return status;
}

TSStatus RpcUtils::getStatus(int code, const string& message) {
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
RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, const string& message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
}

shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(const TSStatus& status) {
    shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
    TSStatus tsStatus(status);
    resp->__set_status(status);
    return resp;
}

shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp>
RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, const string& appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(const TSStatus& status) {
    shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
    TSStatus tsStatus(status);
    resp->__set_status(tsStatus);
    return resp;
}

MyStringBuffer::MyStringBuffer() : pos(0) {
    checkBigEndian();
}

MyStringBuffer::MyStringBuffer(const std::string& str) : str(str), pos(0) {
    checkBigEndian();
}

void MyStringBuffer::reserve(size_t n) {
    str.reserve(n);
}

void MyStringBuffer::clear() {
    str.clear();
    pos = 0;
}

bool MyStringBuffer::hasRemaining() {
    return pos < str.size();
}

int MyStringBuffer::getInt() {
    return *(int*)getOrderedByte(4);
}

boost::gregorian::date MyStringBuffer::getDate() {
    return parseIntToDate(getInt());
}

int64_t MyStringBuffer::getInt64() {
#ifdef ARCH32
    const char *buf_addr = getOrderedByte(8);
    if (reinterpret_cast<uint32_t>(buf_addr) % 4 == 0) {
        return *(int64_t *)buf_addr;
    } else {
        char tmp_buf[8];
        memcpy(tmp_buf, buf_addr, 8);
        return *(int64_t*)tmp_buf;
    }
#else
    return *(int64_t*)getOrderedByte(8);
#endif
}

float MyStringBuffer::getFloat() {
    return *(float*)getOrderedByte(4);
}

double MyStringBuffer::getDouble() {
#ifdef ARCH32
    const char *buf_addr = getOrderedByte(8);
    if (reinterpret_cast<uint32_t>(buf_addr) % 4 == 0) {
        return  *(double*)buf_addr;
    } else {
        char tmp_buf[8];
        memcpy(tmp_buf, buf_addr, 8);
        return *(double*)tmp_buf;
    }
#else
    return *(double*)getOrderedByte(8);
#endif
}

char MyStringBuffer::getChar() {
    return str[pos++];
}

bool MyStringBuffer::getBool() {
    return getChar() == 1;
}

std::string MyStringBuffer::getString() {
    size_t len = getInt();
    size_t tmpPos = pos;
    pos += len;
    return str.substr(tmpPos, len);
}

void MyStringBuffer::putInt(int ins) {
    putOrderedByte((char*)&ins, 4);
}

void MyStringBuffer::putDate(boost::gregorian::date date) {
    putInt(parseDateExpressionToInt(date));
}

void MyStringBuffer::putInt64(int64_t ins) {
    putOrderedByte((char*)&ins, 8);
}

void MyStringBuffer::putFloat(float ins) {
    putOrderedByte((char*)&ins, 4);
}

void MyStringBuffer::putDouble(double ins) {
    putOrderedByte((char*)&ins, 8);
}

void MyStringBuffer::putChar(char ins) {
    str += ins;
}

void MyStringBuffer::putBool(bool ins) {
    char tmp = ins ? 1 : 0;
    str += tmp;
}

void MyStringBuffer::putString(const std::string& ins) {
    putInt((int)(ins.size()));
    str += ins;
}

void MyStringBuffer::concat(const std::string& ins) {
    str.append(ins);
}

void MyStringBuffer::checkBigEndian() {
    static int chk = 0x0201; //used to distinguish CPU's type (BigEndian or LittleEndian)
    isBigEndian = (0x01 != *(char*)(&chk));
}

const char* MyStringBuffer::getOrderedByte(size_t len) {
    const char* p = nullptr;
    if (isBigEndian) {
        p = str.c_str() + pos;
    }
    else {
        const char* tmp = str.c_str();
        for (size_t i = pos; i < pos + len; i++) {
            numericBuf[pos + len - 1 - i] = tmp[i];
        }
        p = numericBuf;
    }
    pos += len;
    return p;
}

void MyStringBuffer::putOrderedByte(char* buf, int len) {
    if (isBigEndian) {
        str.assign(buf, len);
    }
    else {
        for (int i = len - 1; i > -1; i--) {
            str += buf[i];
        }
    }
}

BitMap::BitMap(size_t size) {
    resize(size);
}

void BitMap::resize(size_t size) {
    this->size = size;
    this->bits.resize((size >> 3) + 1); // equal to "size/8 + 1"
    reset();
}

bool BitMap::mark(size_t position) {
    if (position >= size)
        return false;

    bits[position >> 3] |= (char)1 << (position % 8);
    return true;
}

bool BitMap::unmark(size_t position) {
    if (position >= size)
        return false;

    bits[position >> 3] &= ~((char)1 << (position % 8));
    return true;
}

void BitMap::markAll() {
    std::fill(bits.begin(), bits.end(), (char)0XFF);
}

void BitMap::reset() {
    std::fill(bits.begin(), bits.end(), (char)0);
}

bool BitMap::isMarked(size_t position) const {
    if (position >= size)
        return false;

    return (bits[position >> 3] & ((char)1 << (position % 8))) != 0;
}

bool BitMap::isAllUnmarked() const {
    size_t j;
    for (j = 0; j < size >> 3; j++) {
        if (bits[j] != (char)0) {
            return false;
        }
    }
    for (j = 0; j < size % 8; j++) {
        if ((bits[size >> 3] & ((char)1 << j)) != 0) {
            return false;
        }
    }
    return true;
}

bool BitMap::isAllMarked() const {
    size_t j;
    for (j = 0; j < size >> 3; j++) {
        if (bits[j] != (char)0XFF) {
            return false;
        }
    }
    for (j = 0; j < size % 8; j++) {
        if ((bits[size >> 3] & ((char)1 << j)) == 0) {
            return false;
        }
    }
    return true;
}

const std::vector<char>& BitMap::getByteArray() const {
    return this->bits;
}

size_t BitMap::getSize() const {
    return this->size;
}

const std::string UrlUtils::PORT_SEPARATOR = ":";
const std::string UrlUtils::ABB_COLON = "[";

TEndPoint UrlUtils::parseTEndPointIpv4AndIpv6Url(const std::string& endPointUrl) {
    TEndPoint endPoint;

    // Return default TEndPoint if input is empty
    if (endPointUrl.empty()) {
        return endPoint;
    }

    size_t portSeparatorPos = endPointUrl.find_last_of(PORT_SEPARATOR);

    // If no port separator found, treat entire string as IP
    if (portSeparatorPos == std::string::npos) {
        endPoint.__set_ip(endPointUrl);
        return endPoint;
    }

    // Extract port part
    std::string portStr = endPointUrl.substr(portSeparatorPos + 1);

    // Extract IP part
    std::string ip = endPointUrl.substr(0, portSeparatorPos);

    // Handle IPv6 addresses with brackets
    if (ip.find(ABB_COLON) != std::string::npos) {
        // Remove surrounding square brackets for IPv6
        if (ip.size() >= 2 && ip.front() == '[' && ip.back() == ']') {
            ip = ip.substr(1, ip.size() - 2);
        }
    }

    try {
        int port = std::stoi(portStr);
        endPoint.__set_ip(ip);
        endPoint.__set_port(port);
    } catch (const std::exception& e) {
        endPoint.__set_ip(endPointUrl);
    }

    return endPoint;
}
