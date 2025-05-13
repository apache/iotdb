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
    if(date.is_not_a_date()) {
        throw IoTDBException("Date expression is null or empty.");
    }

    const int year = date.year();
    if(year < 1000 || year > 9999) {
        throw DateTimeParseException(
            "Year must be between 1000 and 9999.",
            boost::gregorian::to_iso_extended_string(date),
            0
        );
    }

    const int64_t result = static_cast<int64_t>(year) * 10000 +
                          date.month() * 100 +
                          date.day();
    if(result > INT32_MAX || result < INT32_MIN) {
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

void RpcUtils::verifySuccess(const TSStatus &status) {
    if (status.code == TSStatusCode::MULTIPLE_ERROR) {
        verifySuccess(status.subStatus);
        return;
    }
    if (status.code != TSStatusCode::SUCCESS_STATUS
        && status.code != TSStatusCode::REDIRECTION_RECOMMEND) {
        throw ExecutionException(to_string(status.code) + ": " + status.message, status);
    }
}

void RpcUtils::verifySuccessWithRedirection(const TSStatus &status) {
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
            } else {
                TEndPoint endPoint;
                endPointList[count++] = endPoint;
            }
        }
        if (!endPointList.empty()) {
            throw RedirectException(to_string(status.code) + ": " + status.message, endPointList);
        }
    }
}

void RpcUtils::verifySuccessWithRedirectionForMultiDevices(const TSStatus &status, vector<string> devices) {
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
            } else {
                TEndPoint endPoint;
                endPointList[count++] = endPoint;
            }
        }
        if (!endPointList.empty()) {
            throw RedirectException(to_string(status.code) + ": " + status.message, endPointList);
        }
    }
}

void RpcUtils::verifySuccess(const vector<TSStatus> &statuses) {
    for (const TSStatus &status: statuses) {
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
    resp->__set_status(status);
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