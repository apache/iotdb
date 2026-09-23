/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "RpcCommon.h"

#include <map>
#include <string>
#include <vector>

#include "ThriftConvert.h"
#include "client_types.h"
#include "common_types.h"

using namespace std;

RpcUtils::RpcUtils() {
  SUCCESS_STATUS = std::make_shared<TSStatus>();
  SUCCESS_STATUS->__set_code(TSStatusCode::SUCCESS_STATUS);
}

void RpcUtils::verifySuccess(const TSStatus& status) {
  if (status.code == TSStatusCode::MULTIPLE_ERROR) {
    verifySuccess(status.subStatus);
    return;
  }
  if (status.code != TSStatusCode::SUCCESS_STATUS &&
      status.code != TSStatusCode::REDIRECTION_RECOMMEND) {
    throw ExecutionException(to_string(status.code) + ": " + status.message,
                             statusFromThrift(status));
  }
}

void RpcUtils::verifySuccessWithRedirection(const TSStatus& status) {
  verifySuccess(status);
  if (status.__isset.redirectNode) {
    throw RedirectException(to_string(status.code) + ": " + status.message,
                            endpointFromThrift(status.redirectNode));
  }
  if (status.__isset.subStatus) {
    auto statusSubStatus = status.subStatus;
    vector<TEndPoint> endPointList(statusSubStatus.size());
    int count = 0;
    for (const TSStatus& subStatus : statusSubStatus) {
      if (subStatus.__isset.redirectNode) {
        endPointList[count++] = subStatus.redirectNode;
      } else {
        TEndPoint endPoint;
        endPointList[count++] = endPoint;
      }
    }
    if (!endPointList.empty()) {
      throw RedirectException(to_string(status.code) + ": " + status.message,
                              endpointListFromThrift(endPointList));
    }
  }
}

void RpcUtils::verifySuccessWithRedirectionForMultiDevices(const TSStatus& status,
                                                           vector<string> devices) {
  verifySuccess(status);

  if (status.code == TSStatusCode::MULTIPLE_ERROR ||
      status.code == TSStatusCode::REDIRECTION_RECOMMEND) {
    map<string, TEndPoint> deviceEndPointMap;
    const vector<TSStatus>& statusSubStatus = status.subStatus;
    for (size_t i = 0; i < statusSubStatus.size() && i < devices.size(); i++) {
      const TSStatus& subStatus = statusSubStatus[i];
      if (subStatus.__isset.redirectNode) {
        deviceEndPointMap.insert(make_pair(devices[i], subStatus.redirectNode));
      }
    }
    throw RedirectException(to_string(status.code) + ": " + status.message,
                            endpointMapFromThrift(deviceEndPointMap));
  }

  if (status.__isset.redirectNode) {
    throw RedirectException(to_string(status.code) + ": " + status.message,
                            endpointFromThrift(status.redirectNode));
  }
  if (status.__isset.subStatus) {
    auto statusSubStatus = status.subStatus;
    vector<TEndPoint> endPointList(statusSubStatus.size());
    int count = 0;
    for (const TSStatus& subStatus : statusSubStatus) {
      if (subStatus.__isset.redirectNode) {
        endPointList[count++] = subStatus.redirectNode;
      } else {
        TEndPoint endPoint;
        endPointList[count++] = endPoint;
      }
    }
    if (!endPointList.empty()) {
      throw RedirectException(to_string(status.code) + ": " + status.message,
                              endpointListFromThrift(endPointList));
    }
  }
}

void RpcUtils::verifySuccess(const vector<TSStatus>& statuses) {
  for (const TSStatus& status : statuses) {
    if (status.code != TSStatusCode::SUCCESS_STATUS) {
      vector<Status> publicStatuses;
      publicStatuses.reserve(statuses.size());
      for (const TSStatus& s : statuses) {
        publicStatuses.push_back(statusFromThrift(s));
      }
      throw BatchExecutionException(status.message, publicStatuses);
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

shared_ptr<TSExecuteStatementResp>
RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode) {
  TSStatus status = getStatus(tsStatusCode);
  return getTSExecuteStatementResp(status);
}

shared_ptr<TSExecuteStatementResp>
RpcUtils::getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode,
                                    const string& message) {
  TSStatus status = getStatus(tsStatusCode, message);
  return getTSExecuteStatementResp(status);
}

shared_ptr<TSExecuteStatementResp> RpcUtils::getTSExecuteStatementResp(const TSStatus& status) {
  shared_ptr<TSExecuteStatementResp> resp(new TSExecuteStatementResp());
  resp->__set_status(status);
  return resp;
}

shared_ptr<TSFetchResultsResp>
RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode) {
  TSStatus status = getStatus(tsStatusCode);
  return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp>
RpcUtils::getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode,
                                const string& appendMessage) {
  TSStatus status = getStatus(tsStatusCode, appendMessage);
  return getTSFetchResultsResp(status);
}

shared_ptr<TSFetchResultsResp> RpcUtils::getTSFetchResultsResp(const TSStatus& status) {
  shared_ptr<TSFetchResultsResp> resp(new TSFetchResultsResp());
  resp->__set_status(status);
  return resp;
}

const string UrlUtils::PORT_SEPARATOR = ":";
const string UrlUtils::ABB_COLON = "[";

TEndPoint UrlUtils::parseTEndPointIpv4AndIpv6Url(const string& endPointUrl) {
  TEndPoint endPoint;

  if (endPointUrl.empty()) {
    return endPoint;
  }

  size_t portSeparatorPos = endPointUrl.find_last_of(PORT_SEPARATOR);

  if (portSeparatorPos == string::npos) {
    endPoint.__set_ip(endPointUrl);
    return endPoint;
  }

  string portStr = endPointUrl.substr(portSeparatorPos + 1);
  string ip = endPointUrl.substr(0, portSeparatorPos);

  if (ip.find(ABB_COLON) != string::npos) {
    if (ip.size() >= 2 && ip.front() == '[' && ip.back() == ']') {
      ip = ip.substr(1, ip.size() - 2);
    }
  }

  try {
    int port = stoi(portStr);
    endPoint.__set_ip(ip);
    endPoint.__set_port(port);
  } catch (const exception&) {
    endPoint.__set_ip(endPointUrl);
  }

  return endPoint;
}

bool UrlUtils::isWildcardAddress(const string& host) {
  if (host.empty()) {
    return false;
  }

  const bool bracketed = host.size() >= 2 && host.front() == '[' && host.back() == ']';
  const size_t begin = bracketed ? 1 : 0;
  const size_t end = bracketed ? host.size() - 1 : host.size();
  if (end - begin == 7 && host.compare(begin, end - begin, "0.0.0.0") == 0) {
    return true;
  }

  bool hasColon = false;
  for (size_t index = begin; index < end; ++index) {
    if (host[index] == ':') {
      hasColon = true;
    } else if (host[index] != '0') {
      return false;
    }
  }
  return hasColon;
}
