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
#ifndef ODBCRESTRESULTSET_H
#define ODBCRESTRESULTSET_H

#include "ODBCResultSet.h"

class ODBCRestResultSet : public ODBCResultSet {
public:
  ODBCRestResultSet(StatementHandle* stmtHandle);
  ~ODBCRestResultSet();

  // Implement REST data loading
  virtual void loadData(const nlohmann::json& jsonData) override;
  virtual void loadData(std::unique_ptr<SessionDataSet> sessionDataSet) override {
    throw std::runtime_error("ODBCRestResultSet: loadData with SessionDataSet is not supported");
  }

private:
  // Helper methods for parsing different model types
  void parseJsonResponseTree(const nlohmann::json& jsonResponse);
  void parseJsonResponseTable(const nlohmann::json& jsonResponse);
};

#endif // ODBCRESTRESULTSET_H
