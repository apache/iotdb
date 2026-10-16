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

#ifndef IOTDB_SESSIONPOOL_H
#define IOTDB_SESSIONPOOL_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "Session.h"

/*
 * A thread-safe pool of opened Session objects.
 *
 * A Session is NOT safe to use from multiple threads concurrently. SessionPool
 * solves this by lending each Session to exactly one borrower at a time and
 * reclaiming it afterwards, so many application threads can share a bounded set
 * of physical connections without external locking.
 *
 * Two usage styles are supported:
 *
 *   1. RAII lease (recommended for arbitrary calls):
 *        {
 *          PooledSession s = pool.getSession();   // blocks up to the timeout
 *          s->insertTablet(tablet);               // call any Session method
 *        }                                        // automatically returned
 * here
 *
 *   2. Convenience wrappers / generic execute() (recommended for hot paths):
 *        pool.insertTablet(tablet);
 *        pool.execute([&](Session& s) { s.insertRecord(...); });
 *
 * Both styles evict a Session from the pool (instead of recycling it) when the
 * operation throws IoTDBConnectionException, so a dead connection is never
 * handed to the next borrower; a fresh one is created lazily on demand.
 *
 * Lifetime: a PooledSession returns its Session to the owning SessionPool when
 * destroyed, so every PooledSession must not outlive the pool it came from.
 */
class SessionPool;

class PooledSession {
public:
  PooledSession() noexcept
      : pool_(nullptr), session_(nullptr), broken_(false) {}

  PooledSession(SessionPool *pool, std::shared_ptr<Session> session)
      : pool_(pool), session_(std::move(session)), broken_(false) {}

  // Non-copyable: a leased Session is owned by exactly one borrower.
  PooledSession(const PooledSession &) = delete;
  PooledSession &operator=(const PooledSession &) = delete;

  PooledSession(PooledSession &&other) noexcept
      : pool_(other.pool_), session_(std::move(other.session_)),
        broken_(other.broken_) {
    other.pool_ = nullptr;
    other.session_ = nullptr;
    other.broken_ = false;
  }

  PooledSession &operator=(PooledSession &&other) noexcept {
    if (this != &other) {
      reset();
      pool_ = other.pool_;
      session_ = std::move(other.session_);
      broken_ = other.broken_;
      other.pool_ = nullptr;
      other.session_ = nullptr;
      other.broken_ = false;
    }
    return *this;
  }

  ~PooledSession() { reset(); }

  Session *operator->() const { return session_.get(); }

  Session &operator*() const { return *session_; }

  explicit operator bool() const { return static_cast<bool>(session_); }

  // Mark the underlying connection as unusable so it is discarded (not
  // recycled) when this lease is returned. Call this if you caught a connection
  // error.
  void markBroken() { broken_ = true; }

  // Eagerly return the Session to the pool before scope exit.
  void release() { reset(); }

private:
  void reset();

  SessionPool *pool_;
  std::shared_ptr<Session> session_;
  bool broken_;
};

/*
 * Couples a query result set with the pooled Session that produced it.
 *
 * A SessionDataSet lazily fetches further result blocks over its Session's
 * connection, so that Session must stay exclusively leased until iteration is
 * finished. This wrapper holds the lease for exactly that long and returns the
 * Session to the pool when destroyed.
 */
class PooledSessionDataSet {
public:
  PooledSessionDataSet(PooledSession session,
                       std::unique_ptr<SessionDataSet> dataSet)
      : session_(std::move(session)), dataSet_(std::move(dataSet)) {}

  PooledSessionDataSet(const PooledSessionDataSet &) = delete;
  PooledSessionDataSet &operator=(const PooledSessionDataSet &) = delete;
  PooledSessionDataSet(PooledSessionDataSet &&) noexcept = default;
  PooledSessionDataSet &operator=(PooledSessionDataSet &&) noexcept = default;

  SessionDataSet *operator->() const { return dataSet_.get(); }
  SessionDataSet &operator*() const { return *dataSet_; }

private:
  PooledSession session_;
  std::unique_ptr<SessionDataSet> dataSet_;
};

class SessionPool {
public:
  static constexpr size_t DEFAULT_MAX_SIZE = 5;
  static constexpr int64_t DEFAULT_WAIT_TIMEOUT_MS = 60 * 1000;

  // Single-host constructor.
  SessionPool(std::string host, int rpcPort, std::string username,
              std::string password, size_t maxSize = DEFAULT_MAX_SIZE);

  ~SessionPool();

  // Non-copyable, non-movable: the pool owns mutex/condition state.
  SessionPool(const SessionPool &) = delete;
  SessionPool &operator=(const SessionPool &) = delete;

  // ---- configuration (apply before the first getSession()) ----
  SessionPool &setFetchSize(int fetchSize);
  SessionPool &setZoneId(std::string zoneId);
  SessionPool &setSqlDialect(std::string sqlDialect);
  SessionPool &setDatabase(std::string database);
  SessionPool &setEnableRedirection(bool enable);
  SessionPool &setEnableAutoFetch(bool enable);
  SessionPool &setEnableRPCCompression(bool enable);
  SessionPool &setConnectTimeoutMs(int connectTimeoutMs);
  SessionPool &setWaitToGetSessionTimeoutMs(int64_t timeoutMs);

  // Borrow a Session. Blocks until one is free or a new one can be created,
  // up to timeoutMs (<= 0 means use the pool default). Throws IoTDBException on
  // timeout or when the pool is closed.
  PooledSession getSession();
  PooledSession getSession(int64_t timeoutMs);

  // Generic helper: borrow a Session, run func(Session&), return/evict it, and
  // forward the result. Evicts the Session on IoTDBConnectionException.
  template <typename Func>
  auto execute(Func &&func) -> decltype(func(std::declval<Session &>()));

  // ---- convenience wrappers for common operations (with eviction on failure)
  // ----
  void insertTablet(Tablet &tablet, bool sorted = false);
  void insertAlignedTablet(Tablet &tablet, bool sorted = false);
  void insertTablets(std::unordered_map<std::string, Tablet *> &tablets,
                     bool sorted = false);
  void insertRecord(const std::string &deviceId, int64_t time,
                    const std::vector<std::string> &measurements,
                    const std::vector<std::string> &values);
  void
  insertRecords(const std::vector<std::string> &deviceIds,
                const std::vector<int64_t> &times,
                const std::vector<std::vector<std::string>> &measurementsList,
                const std::vector<std::vector<std::string>> &valuesList);
  void executeNonQueryStatement(const std::string &sql);
  // The returned wrapper keeps the underlying Session leased until it is
  // destroyed, so it is safe to iterate the result set across multiple fetches.
  PooledSessionDataSet executeQueryStatement(const std::string &sql);
  PooledSessionDataSet executeQueryStatement(const std::string &sql,
                                             int64_t timeoutInMs);

  // Close the pool: idle Sessions are closed immediately, in-use Sessions are
  // closed when they are returned. Idempotent.
  void close();

  // ---- observability ----
  size_t getMaxSize() const { return maxSize_; }
  // Number of Sessions currently borrowed.
  size_t activeCount();

private:
  friend class PooledSession;

  std::shared_ptr<Session> constructNewSession();
  std::shared_ptr<Session> acquire(int64_t timeoutMs);
  void putBack(const std::shared_ptr<Session> &session, bool broken);

  // connection parameters
  std::string host_;
  int rpcPort_;
  std::string username_;
  std::string password_;
  std::string zoneId_;
  int fetchSize_ = 10000;
  std::string sqlDialect_ = "tree";
  std::string database_;
  bool enableRedirection_ = true;
  bool enableAutoFetch_ = true;
  bool enableRPCCompression_ = false;
  int connectTimeoutMs_ = 3000;

  // pool sizing / waiting policy
  size_t maxSize_;
  int64_t waitTimeoutMs_ = DEFAULT_WAIT_TIMEOUT_MS;

  // pool state, guarded by mutex_
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::shared_ptr<Session>> idleQueue_;
  size_t size_ = 0; // total live Sessions (idle + borrowed)
  bool closed_ = false;
};

template <typename Func>
auto SessionPool::execute(Func &&func)
    -> decltype(func(std::declval<Session &>())) {
  PooledSession lease = getSession();
  try {
    return func(*lease);
  } catch (const IoTDBConnectionException &) {
    lease.markBroken();
    throw;
  }
}

/*
 * Fluent builder for SessionPool, mirroring SessionBuilder /
 * TableSessionBuilder.
 *
 *   auto pool = SessionPoolBuilder()
 *                   .host("127.0.0.1")->rpcPort(6667)
 *                   ->username("root")->password("root")
 *                   ->maxSize(10)->build();
 */
class SessionPoolBuilder : public AbstractSessionBuilder {
public:
  SessionPoolBuilder *host(const std::string &v) {
    AbstractSessionBuilder::host = v;
    return this;
  }
  SessionPoolBuilder *rpcPort(int v) {
    AbstractSessionBuilder::rpcPort = v;
    return this;
  }
  SessionPoolBuilder *username(const std::string &v) {
    AbstractSessionBuilder::username = v;
    return this;
  }
  SessionPoolBuilder *password(const std::string &v) {
    AbstractSessionBuilder::password = v;
    return this;
  }
  SessionPoolBuilder *zoneId(const std::string &v) {
    AbstractSessionBuilder::zoneId = v;
    return this;
  }
  SessionPoolBuilder *fetchSize(int v) {
    AbstractSessionBuilder::fetchSize = v;
    return this;
  }
  SessionPoolBuilder *database(const std::string &v) {
    AbstractSessionBuilder::database = v;
    return this;
  }
  SessionPoolBuilder *enableAutoFetch(bool v) {
    AbstractSessionBuilder::enableAutoFetch = v;
    return this;
  }
  SessionPoolBuilder *enableRedirections(bool v) {
    AbstractSessionBuilder::enableRedirections = v;
    return this;
  }
  SessionPoolBuilder *enableRPCCompression(bool v) {
    AbstractSessionBuilder::enableRPCCompression = v;
    return this;
  }
  SessionPoolBuilder *connectTimeoutMs(int v) {
    connectTimeoutMs_ = v;
    return this;
  }
  SessionPoolBuilder *maxSize(size_t v) {
    maxSize_ = v;
    return this;
  }
  SessionPoolBuilder *waitToGetSessionTimeoutMs(int64_t v) {
    waitTimeoutMs_ = v;
    return this;
  }
  SessionPoolBuilder *sqlDialect(const std::string &v) {
    AbstractSessionBuilder::sqlDialect = v;
    return this;
  }

  std::shared_ptr<SessionPool> build() {
    auto pool = std::make_shared<SessionPool>(
        AbstractSessionBuilder::host, AbstractSessionBuilder::rpcPort,
        AbstractSessionBuilder::username, AbstractSessionBuilder::password,
        maxSize_);
    pool->setFetchSize(AbstractSessionBuilder::fetchSize)
        .setZoneId(AbstractSessionBuilder::zoneId)
        .setSqlDialect(AbstractSessionBuilder::sqlDialect)
        .setDatabase(AbstractSessionBuilder::database)
        .setEnableRedirection(AbstractSessionBuilder::enableRedirections)
        .setEnableAutoFetch(AbstractSessionBuilder::enableAutoFetch)
        .setEnableRPCCompression(AbstractSessionBuilder::enableRPCCompression)
        .setConnectTimeoutMs(connectTimeoutMs_)
        .setWaitToGetSessionTimeoutMs(waitTimeoutMs_);
    return pool;
  }

private:
  size_t maxSize_ = SessionPool::DEFAULT_MAX_SIZE;
  int64_t waitTimeoutMs_ = SessionPool::DEFAULT_WAIT_TIMEOUT_MS;
  int connectTimeoutMs_ = 3000;
};

#endif // IOTDB_SESSIONPOOL_H
