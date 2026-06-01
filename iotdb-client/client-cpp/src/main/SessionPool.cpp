/*
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

#include "SessionPool.h"

void PooledSession::reset() {
  if (session_ && pool_ != nullptr) {
    pool_->putBack(session_, broken_);
  }
  pool_ = nullptr;
  session_ = nullptr;
  broken_ = false;
}

SessionPool::SessionPool(std::string host, int rpcPort, std::string username, std::string password,
                         size_t maxSize)
    : host_(std::move(host)), rpcPort_(rpcPort), username_(std::move(username)),
      password_(std::move(password)), maxSize_(maxSize) {
  if (maxSize_ == 0) {
    throw IoTDBException("SessionPool maxSize must be greater than 0.");
  }
}

SessionPool::SessionPool(std::vector<std::string> nodeUrls, std::string username,
                         std::string password, size_t maxSize)
    : rpcPort_(AbstractSessionBuilder::DEFAULT_RPC_PORT), nodeUrls_(std::move(nodeUrls)),
      username_(std::move(username)), password_(std::move(password)), maxSize_(maxSize) {
  if (maxSize_ == 0) {
    throw IoTDBException("SessionPool maxSize must be greater than 0.");
  }
}

SessionPool::~SessionPool() {
  try {
    close();
  } catch (const std::exception& e) {
    log_debug(std::string("SessionPool::~SessionPool(), ") + e.what());
  }
}

SessionPool& SessionPool::setFetchSize(int fetchSize) {
  fetchSize_ = fetchSize;
  return *this;
}

SessionPool& SessionPool::setZoneId(std::string zoneId) {
  zoneId_ = std::move(zoneId);
  return *this;
}

SessionPool& SessionPool::setSqlDialect(std::string sqlDialect) {
  sqlDialect_ = std::move(sqlDialect);
  return *this;
}

SessionPool& SessionPool::setDatabase(std::string database) {
  database_ = std::move(database);
  return *this;
}

SessionPool& SessionPool::setEnableRedirection(bool enable) {
  enableRedirection_ = enable;
  return *this;
}

SessionPool& SessionPool::setEnableAutoFetch(bool enable) {
  enableAutoFetch_ = enable;
  return *this;
}

SessionPool& SessionPool::setEnableRPCCompression(bool enable) {
  enableRPCCompression_ = enable;
  return *this;
}

SessionPool& SessionPool::setConnectTimeoutMs(int connectTimeoutMs) {
  connectTimeoutMs_ = connectTimeoutMs;
  return *this;
}

SessionPool& SessionPool::setWaitToGetSessionTimeoutMs(int64_t timeoutMs) {
  waitTimeoutMs_ = timeoutMs;
  return *this;
}

SessionPool& SessionPool::setUseSSL(bool useSSL) {
  useSSL_ = useSSL;
  return *this;
}

SessionPool& SessionPool::setTrustCertFilePath(std::string path) {
  trustCertFilePath_ = std::move(path);
  return *this;
}

std::shared_ptr<Session> SessionPool::constructNewSession() {
  AbstractSessionBuilder builder;
  builder.host = host_;
  builder.rpcPort = rpcPort_;
  builder.nodeUrls = nodeUrls_;
  builder.username = username_;
  builder.password = password_;
  builder.zoneId = zoneId_;
  builder.fetchSize = fetchSize_;
  builder.sqlDialect = sqlDialect_;
  builder.database = database_;
  builder.enableAutoFetch = enableAutoFetch_;
  builder.enableRedirections = enableRedirection_;
  builder.enableRPCCompression = enableRPCCompression_;
  builder.connectTimeoutMs = connectTimeoutMs_;
  builder.useSSL = useSSL_;
  builder.trustCertFilePath = trustCertFilePath_;

  auto session = std::make_shared<Session>(&builder);
  session->open(enableRPCCompression_, connectTimeoutMs_);
  return session;
}

std::shared_ptr<Session> SessionPool::acquire(int64_t timeoutMs) {
  const int64_t effectiveTimeout = timeoutMs > 0 ? timeoutMs : waitTimeoutMs_;
  std::unique_lock<std::mutex> lock(mutex_);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(effectiveTimeout);

  while (true) {
    if (closed_) {
      throw IoTDBException("SessionPool is closed.");
    }
    if (!idleQueue_.empty()) {
      auto session = idleQueue_.front();
      idleQueue_.pop_front();
      return session;
    }
    if (size_ < maxSize_) {
      // Reserve a slot, then build the connection outside the lock so other
      // borrowers are not blocked by network/handshake latency.
      ++size_;
      lock.unlock();
      std::shared_ptr<Session> session;
      try {
        session = constructNewSession();
      } catch (...) {
        lock.lock();
        --size_;
        cv_.notify_one();
        throw;
      }
      return session;
    }

    // Pool exhausted: wait for a Session to be returned.
    if (effectiveTimeout <= 0) {
      cv_.wait(lock);
    } else {
      if (cv_.wait_until(lock, deadline) == std::cv_status::timeout && idleQueue_.empty() &&
          size_ >= maxSize_ && !closed_) {
        throw IoTDBException("Wait to get session timeout in SessionPool, maxSize=" +
                             std::to_string(maxSize_) + ", waitTimeoutMs=" +
                             std::to_string(effectiveTimeout) + ".");
      }
    }
  }
}

void SessionPool::putBack(const std::shared_ptr<Session>& session, bool broken) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (broken || closed_) {
    // Drop the Session and free its slot so a healthy replacement can be created
    // on demand. The caller (PooledSession::reset) still holds the last reference
    // and tears the connection down after we return, i.e. outside this lock.
    --size_;
  } else {
    idleQueue_.push_back(session);
  }
  cv_.notify_one();
}

PooledSession SessionPool::getSession() {
  return getSession(waitTimeoutMs_);
}

PooledSession SessionPool::getSession(int64_t timeoutMs) {
  return PooledSession(this, acquire(timeoutMs));
}

void SessionPool::insertTablet(Tablet& tablet, bool sorted) {
  execute([&](Session& s) { s.insertTablet(tablet, sorted); });
}

void SessionPool::insertAlignedTablet(Tablet& tablet, bool sorted) {
  execute([&](Session& s) { s.insertAlignedTablet(tablet, sorted); });
}

void SessionPool::insertTablets(std::unordered_map<std::string, Tablet*>& tablets, bool sorted) {
  execute([&](Session& s) { s.insertTablets(tablets, sorted); });
}

void SessionPool::insertRecord(const std::string& deviceId, int64_t time,
                               const std::vector<std::string>& measurements,
                               const std::vector<std::string>& values) {
  execute([&](Session& s) { s.insertRecord(deviceId, time, measurements, values); });
}

void SessionPool::insertRecords(const std::vector<std::string>& deviceIds,
                                const std::vector<int64_t>& times,
                                const std::vector<std::vector<std::string>>& measurementsList,
                                const std::vector<std::vector<std::string>>& valuesList) {
  execute([&](Session& s) { s.insertRecords(deviceIds, times, measurementsList, valuesList); });
}

void SessionPool::executeNonQueryStatement(const std::string& sql) {
  execute([&](Session& s) { s.executeNonQueryStatement(sql); });
}

PooledSessionDataSet SessionPool::executeQueryStatement(const std::string& sql) {
  PooledSession lease = getSession();
  try {
    auto dataSet = lease->executeQueryStatement(sql);
    return PooledSessionDataSet(std::move(lease), std::move(dataSet));
  } catch (const IoTDBConnectionException&) {
    lease.markBroken();
    throw;
  }
}

PooledSessionDataSet SessionPool::executeQueryStatement(const std::string& sql,
                                                        int64_t timeoutInMs) {
  PooledSession lease = getSession();
  try {
    auto dataSet = lease->executeQueryStatement(sql, timeoutInMs);
    return PooledSessionDataSet(std::move(lease), std::move(dataSet));
  } catch (const IoTDBConnectionException&) {
    lease.markBroken();
    throw;
  }
}

void SessionPool::close() {
  std::deque<std::shared_ptr<Session>> toClose;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return;
    }
    closed_ = true;
    toClose.swap(idleQueue_);
    size_ -= toClose.size();
  }
  cv_.notify_all();
  // Sessions destructed here (outside the lock) close their connections.
  toClose.clear();
}

size_t SessionPool::activeCount() {
  std::lock_guard<std::mutex> lock(mutex_);
  return size_ - idleQueue_.size();
}
