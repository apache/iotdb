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
#ifndef IOTDB_RPC_COMMON_H
#define IOTDB_RPC_COMMON_H

#include <memory>
#include <string>
#include <vector>

#include "Common.h"

class TSStatus;
class TSExecuteStatementResp;
class TSFetchResultsResp;
class TEndPoint;

class RpcUtils {
public:
  std::shared_ptr<TSStatus> SUCCESS_STATUS;

  RpcUtils();

  static void verifySuccess(const TSStatus& status);

  static void verifySuccessWithRedirection(const TSStatus& status);

  static void verifySuccessWithRedirectionForMultiDevices(const TSStatus& status,
                                                          std::vector<std::string> devices);

  static void verifySuccess(const std::vector<TSStatus>& statuses);

  static TSStatus getStatus(TSStatusCode::TSStatusCode tsStatusCode);

  static TSStatus getStatus(int code, const std::string& message);

  static std::shared_ptr<TSExecuteStatementResp>
  getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode);

  static std::shared_ptr<TSExecuteStatementResp>
  getTSExecuteStatementResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string& message);

  static std::shared_ptr<TSExecuteStatementResp> getTSExecuteStatementResp(const TSStatus& status);

  static std::shared_ptr<TSFetchResultsResp>
  getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode);

  static std::shared_ptr<TSFetchResultsResp>
  getTSFetchResultsResp(TSStatusCode::TSStatusCode tsStatusCode, const std::string& appendMessage);

  static std::shared_ptr<TSFetchResultsResp> getTSFetchResultsResp(const TSStatus& status);
};

class UrlUtils {
private:
  static const std::string PORT_SEPARATOR;
  static const std::string ABB_COLON;

  UrlUtils() = delete;
  ~UrlUtils() = delete;

public:
  static TEndPoint parseTEndPointIpv4AndIpv6Url(const std::string& endPointUrl);

  static bool isWildcardAddress(const std::string& host);
};

#endif
