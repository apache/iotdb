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
#ifndef ODBCSESSIONRESULTSET_H
#define ODBCSESSIONRESULTSET_H

#include "ODBCResultSet.h"
#include "SessionDataSet.h"
#include <unordered_map>

class ODBCSessionResultSet : public ODBCResultSet {
public:
  ODBCSessionResultSet(StatementHandle* stmtHandle);
  ~ODBCSessionResultSet();

  // Implement Session data loading
  virtual void loadData(std::unique_ptr<SessionDataSet> sessionDataSet) override;
  virtual void loadData(const nlohmann::json& jsonData) override {
    throw std::runtime_error("ODBCSessionResultSet: loadData with json is not supported");
  }
  bool loadDataNextBatch();

  // Session-specific functionality
  bool loadDataInternal();
  bool loadDataMetaData();

private:
  // Streaming support for Session API
  std::unique_ptr<SessionDataSet> sessionDataSet;            // SessionDataSet for large result sets
  std::vector<TSDataType::TSDataType> TSDataTypeColumnTypes; // Column types
};

#endif // ODBCSESSIONRESULTSET_H
