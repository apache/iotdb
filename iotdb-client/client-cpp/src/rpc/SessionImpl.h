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
#ifndef IOTDB_SESSION_IMPL_H
#define IOTDB_SESSION_IMPL_H

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "AbstractSessionBuilder.h"
#include "Common.h"
#include "DeviceID.h"
#include "Endpoint.h"
#include "NodesSupplier.h"
#include "Session.h"
#include "SessionConnection.h"
#include "ThriftConvert.h"
#include "client_types.h"
#include "common_types.h"

class Session::Impl {
public:
  std::string host_;
  int rpcPort_ = 6667;
  bool useSSL_ = false;
  std::string trustCertFilePath_;
  std::vector<std::string> nodeUrls_;
  std::string username_ = "root";
  std::string password_ = "root";
  const TSProtocolVersion::type protocolVersion_ = TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3;
  bool isClosed_ = true;
  std::string zoneId_;
  int fetchSize_ = iotdb::session::DEFAULT_FETCH_SIZE;
  static const int DEFAULT_TIMEOUT_MS = 0;
  int connectTimeoutMs_ = iotdb::session::DEFAULT_CONNECT_TIMEOUT_MS;
  Version::Version version = Version::V_1_0;
  std::string sqlDialect_ = "tree";
  std::string database_;
  bool enableAutoFetch_ = true;
  bool enableRedirection_ = true;
  std::shared_ptr<INodesSupplier> nodesSupplier_;
  std::shared_ptr<SessionConnection> defaultSessionConnection_;

  std::shared_ptr<SessionConnection> getDefaultSessionConnection() {
    if (isClosed_ || !defaultSessionConnection_) {
      throw IoTDBConnectionException("Session is not open, please invoke Session.open() first");
    }
    return defaultSessionConnection_;
  }

  TEndPoint defaultEndPoint_;

  struct TEndPointHash {
    size_t operator()(const TEndPoint& endpoint) const {
      return std::hash<std::string>()(endpoint.ip) ^ std::hash<int>()(endpoint.port);
    }
  };

  struct TEndPointEqual {
    bool operator()(const TEndPoint& lhs, const TEndPoint& rhs) const {
      return lhs.ip == rhs.ip && lhs.port == rhs.port;
    }
  };

  using EndPointSessionMap = std::unordered_map<TEndPoint, std::shared_ptr<SessionConnection>,
                                                TEndPointHash, TEndPointEqual>;
  EndPointSessionMap endPointToSessionConnection;
  std::unordered_map<std::string, TEndPoint> deviceIdToEndpoint;
  std::unordered_map<std::shared_ptr<storage::IDeviceID>, TEndPoint> tableModelDeviceIdToEndpoint;

  void removeBrokenSessionConnection(std::shared_ptr<SessionConnection> sessionConnection);

  static bool checkSorted(const Tablet& tablet);
  static bool checkSorted(const std::vector<int64_t>& times);
  static void sortTablet(Tablet& tablet);
  static void sortIndexByTimestamp(int* index, std::vector<int64_t>& timestamps, int length);

  void appendValues(std::string& buffer, const char* value, int size);
  void putValuesIntoBuffer(const std::vector<TSDataType::TSDataType>& types,
                           const std::vector<char*>& values, std::string& buf);
  int8_t getDataTypeNumber(TSDataType::TSDataType type);

  struct TsCompare {
    std::vector<int64_t>& timestamps;
    explicit TsCompare(std::vector<int64_t>& inTimestamps) : timestamps(inTimestamps) {}
    bool operator()(int i, int j) {
      return timestamps[i] < timestamps[j];
    }
  };

  std::string getVersionString(Version::Version version);

  void initZoneId();
  void initNodesSupplier(const std::vector<std::string>& nodeUrls = std::vector<std::string>());
  void initDefaultSessionConnection();

  template <typename T, typename InsertConsumer>
  void insertByGroup(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                     InsertConsumer insertConsumer);

  template <typename T, typename InsertConsumer>
  void insertOnce(std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
                  InsertConsumer insertConsumer);

  void insertStringRecordsWithLeaderCache(std::vector<std::string> deviceIds,
                                          std::vector<int64_t> times,
                                          std::vector<std::vector<std::string>> measurementsList,
                                          std::vector<std::vector<std::string>> valuesList,
                                          bool isAligned);

  void
  insertRecordsWithLeaderCache(std::vector<std::string> deviceIds, std::vector<int64_t> times,
                               std::vector<std::vector<std::string>> measurementsList,
                               const std::vector<std::vector<TSDataType::TSDataType>>& typesList,
                               std::vector<std::vector<char*>> valuesList, bool isAligned);

  void insertTabletsWithLeaderCache(std::unordered_map<std::string, Tablet*>& tablets, bool sorted,
                                    bool isAligned);

  std::shared_ptr<SessionConnection> getQuerySessionConnection();
  std::shared_ptr<SessionConnection> getSessionConnection(std::string deviceId);
  std::shared_ptr<SessionConnection>
  getSessionConnection(std::shared_ptr<storage::IDeviceID> deviceId);

  void handleQueryRedirection(TEndPoint endPoint);
  void handleRedirection(const std::string& deviceId, TEndPoint endPoint);
  void handleRedirection(const std::shared_ptr<storage::IDeviceID>& deviceId, TEndPoint endPoint);

  static void buildInsertTabletReq(TSInsertTabletReq& request, Tablet& tablet, bool sorted);
  void insertTablet(TSInsertTabletReq request);
  void insertRelationalTabletOnce(
      const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>& relationalTabletGroup,
      bool sorted);
  void insertRelationalTabletByGroup(
      const std::unordered_map<std::shared_ptr<SessionConnection>, Tablet>& relationalTabletGroup,
      bool sorted);
};

template <typename T, typename InsertConsumer>
void Session::Impl::insertByGroup(
    std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
    InsertConsumer insertConsumer) {
  std::vector<std::future<void>> futures;

  for (auto& entry : insertGroup) {
    auto connection = entry.first;
    auto& req = entry.second;
    futures.emplace_back(std::async(std::launch::async, [=, &req]() mutable {
      try {
        insertConsumer(connection, req);
      } catch (const RedirectException& e) {
        for (const auto& deviceEndPoint : e.deviceEndPointMap) {
          handleRedirection(deviceEndPoint.first, endpointToThrift(deviceEndPoint.second));
        }
      } catch (const IoTDBConnectionException&) {
        if (endPointToSessionConnection.size() > 1) {
          removeBrokenSessionConnection(connection);
          try {
            insertConsumer(getDefaultSessionConnection(), req);
          } catch (const RedirectException&) {
          }
        } else {
          throw;
        }
      } catch (const std::exception& e) {
        log_debug(e.what());
        throw IoTDBException(e.what());
      }
    }));
  }

  std::string errorMessages;
  for (auto& f : futures) {
    try {
      f.get();
    } catch (const IoTDBConnectionException&) {
      throw;
    } catch (const std::exception& e) {
      if (!errorMessages.empty()) {
        errorMessages += ";";
      }
      errorMessages += e.what();
    }
  }

  if (!errorMessages.empty()) {
    throw StatementExecutionException(errorMessages);
  }
}

template <typename T, typename InsertConsumer>
void Session::Impl::insertOnce(
    std::unordered_map<std::shared_ptr<SessionConnection>, T>& insertGroup,
    InsertConsumer insertConsumer) {
  auto connection = insertGroup.begin()->first;
  auto req = insertGroup.begin()->second;
  try {
    insertConsumer(connection, req);
  } catch (const RedirectException& e) {
    for (const auto& deviceEndPoint : e.deviceEndPointMap) {
      handleRedirection(deviceEndPoint.first, endpointToThrift(deviceEndPoint.second));
    }
  } catch (const IoTDBConnectionException&) {
    if (endPointToSessionConnection.size() > 1) {
      removeBrokenSessionConnection(connection);
      try {
        insertConsumer(getDefaultSessionConnection(), req);
      } catch (const RedirectException&) {
      }
    } else {
      throw;
    }
  } catch (const std::exception& e) {
    log_debug(e.what());
    throw IoTDBException(e.what());
  }
}

#endif
