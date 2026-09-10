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

#include "ThriftConvert.h"

#include "common_types.h"

Status statusFromThrift(const TSStatus& tsStatus) {
  Status status;
  status.code = tsStatus.code;
  status.message = tsStatus.message;
  return status;
}

Endpoint endpointFromThrift(const TEndPoint& endPoint) {
  Endpoint endpoint;
  endpoint.host = endPoint.ip;
  endpoint.port = endPoint.port;
  return endpoint;
}

TEndPoint endpointToThrift(const Endpoint& endpoint) {
  TEndPoint endPoint;
  endPoint.__set_ip(endpoint.host);
  endPoint.__set_port(endpoint.port);
  return endPoint;
}

std::map<std::string, Endpoint>
endpointMapFromThrift(const std::map<std::string, TEndPoint>& deviceEndPointMap) {
  std::map<std::string, Endpoint> result;
  for (const auto& entry : deviceEndPointMap) {
    result.emplace(entry.first, endpointFromThrift(entry.second));
  }
  return result;
}

std::vector<Endpoint> endpointListFromThrift(const std::vector<TEndPoint>& endPointList) {
  std::vector<Endpoint> result;
  result.reserve(endPointList.size());
  for (const auto& endPoint : endPointList) {
    result.push_back(endpointFromThrift(endPoint));
  }
  return result;
}
