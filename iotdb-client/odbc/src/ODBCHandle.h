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
//
// Created by christoferdutz on 05.11.2024.
//

#ifndef ODBCHANDLE_H
#define ODBCHANDLE_H

#include "Pch.h"
#include "DiagnosticManager.h"
#include <mutex>
#include <unordered_map>

class ODBCHandle : public DiagnosticManager {
  // Common fields for all handles
protected:
  int handleType; // e.g., SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT
  bool isFreed;

public:
  explicit ODBCHandle(int type) : handleType(type), isFreed(false) {
    std::lock_guard<std::mutex> lock(registryMutex());
    registry()[this] = type;
  }

  ~ODBCHandle() {
    std::lock_guard<std::mutex> lock(registryMutex());
    registry().erase(this);
  }

  ODBCHandle(const ODBCHandle&) = delete;
  ODBCHandle& operator=(const ODBCHandle&) = delete;

  // SQLDisconnect drops child handles. A later cleanup call must reject their
  // old addresses without dereferencing already released storage.
  static bool isLiveHandle(SQLHANDLE handle, int type) {
    std::lock_guard<std::mutex> lock(registryMutex());
    auto entry = registry().find(handle);
    return entry != registry().end() && entry->second == type;
  }

  int getHandleType() const {
    return handleType;
  }

  void markAsFreed() {
    isFreed = true;
  }
  bool isHandleFreed() const {
    return isFreed;
  }

private:
  static std::mutex& registryMutex() {
    static std::mutex mutex;
    return mutex;
  }
  static std::unordered_map<SQLHANDLE, int>& registry() {
    static std::unordered_map<SQLHANDLE, int> handles;
    return handles;
  }
};

#endif //ODBCHANDLE_H
