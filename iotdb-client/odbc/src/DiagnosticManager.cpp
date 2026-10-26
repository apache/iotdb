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
#include "DiagnosticManager.h"

#include <utility>

DiagnosticRecord::DiagnosticRecord(std::string state, const std::string& msg, int error)
    : sqlState(std::move(state)), message(msg), nativeError(error) {}

DiagnosticManager::DiagnosticManager() = default;

DiagnosticManager::~DiagnosticManager() = default;

void DiagnosticManager::addDiagnostic(const std::string& sqlState, const std::string& message,
                                      int nativeError) {
  std::lock_guard<std::mutex> lock(mutex);
  diagnosticRecords.emplace_back(sqlState, message, nativeError);
}

bool DiagnosticManager::getDiagnostic(const int index, std::string& sqlState, std::string& message,
                                      int& nativeError) {
  std::lock_guard<std::mutex> lock(mutex);
  if (index <= 0 || index > static_cast<int>(diagnosticRecords.size())) {
    return false;
  }
  const DiagnosticRecord& record = diagnosticRecords[index - 1];
  sqlState = record.sqlState;
  message = record.message;
  nativeError = record.nativeError;
  return true;
}

void DiagnosticManager::clearDiagnostics() {
  std::lock_guard<std::mutex> lock(mutex);
  diagnosticRecords.clear();
}

size_t DiagnosticManager::diagnosticCount() {
  std::lock_guard<std::mutex> lock(mutex);
  return diagnosticRecords.size();
}

void DiagnosticManager::consumeFirstDiagnostic() {
  std::lock_guard<std::mutex> lock(mutex);
  if (!diagnosticRecords.empty())
    diagnosticRecords.pop_front();
}
