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
#ifndef DIAGNOSTIC_MANAGER_H
#define DIAGNOSTIC_MANAGER_H

#include "Pch.h"

// Class representing a single diagnostic record
class DiagnosticRecord {
public:
  std::string sqlState;
  std::string message;
  int nativeError;

  DiagnosticRecord(std::string state, const std::string& msg, int error = 0);
};

// Class managing a queue of diagnostic records
class DiagnosticManager {

  std::deque<DiagnosticRecord> diagnosticRecords;
  std::mutex mutex;

public:
  DiagnosticManager();
  ~DiagnosticManager();

  // Adds a new diagnostic record
  void addDiagnostic(const std::string& sqlState, const std::string& message, int nativeError = 0);

  // Retrieves a diagnostic record by index (1-based)
  bool getDiagnostic(int index, std::string& sqlState, std::string& message, int& nativeError);

  // Clears all diagnostic records
  void clearDiagnostics();
  void consumeFirstDiagnostic();

  // Returns the count of diagnostic records
  size_t diagnosticCount();
};

#endif // DIAGNOSTIC_MANAGER_H
