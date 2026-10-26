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
#include "ODBCRestResultSet.h"

// Project includes
#include "Log.h"

// Standard includes
#include <string>
#include <vector>
#include <sstream>
#include "StatementHandle.h"

ODBCRestResultSet::ODBCRestResultSet(StatementHandle* stmtHandle) : ODBCResultSet(stmtHandle) {
  // Initialize member variables
}

ODBCRestResultSet::~ODBCRestResultSet() {
  // Destructor - shared_ptr will handle cleanup automatically
}

// Implement REST data loading
void ODBCRestResultSet::loadData(const nlohmann::json& jsonData) {
  logMessage(stmt->getConnection(), "ODBCRestResultSet::loadData: Starting", LOG_LEVEL_TRACE);
  ConnectionHandle* cnct = stmt->getConnection();

  // Clear existing data
  columnNames.clear();
  columnTypes.clear();
  data.clear();
  numRows = 0;
  numColumns = 0;

  if (stmt->getConnection()->isTableModel) {
    logMessage(cnct, "ODBCRestResultSet::loadData: Parsing JSON response for table model",
               LOG_LEVEL_TRACE);
    parseJsonResponseTable(jsonData);
  } else {
    logMessage(cnct, "ODBCRestResultSet::loadData: Parsing JSON response for tree model",
               LOG_LEVEL_TRACE);
    parseJsonResponseTree(jsonData);
  }

  logMessage(cnct, "ODBCRestResultSet::loadData: Exiting", LOG_LEVEL_DEBUG);
}

namespace {
ODBCField jsonField(const nlohmann::json& value, const std::string& type) {
  if (value.is_null())
    return ODBCField::null();
  if (type == "DATE") {
    if (value.is_number_integer())
      return ODBCField(parseIntToDate(value.get<int32_t>()));
    const auto text = value.get<std::string>();
    if (text.size() != 10 || text[4] != '-' || text[7] != '-')
      throw std::runtime_error("Invalid REST DATE value");
    return ODBCField(IoTDBDate(std::stoi(text.substr(0, 4)), std::stoi(text.substr(5, 2)),
                               std::stoi(text.substr(8, 2))));
  }
  if (type == "INT32")
    return ODBCField(value.get<int32_t>());
  if (type == "FLOAT")
    return ODBCField(value.get<float>());
  ODBCField field;
  if (value.is_string())
    field = ODBCField(value.get<std::string>());
  else if (value.is_boolean())
    field = ODBCField(value.get<bool>());
  else if (value.is_number_integer())
    field = ODBCField(value.get<int64_t>());
  else if (value.is_number_float())
    field = ODBCField(value.get<double>());
  else
    throw std::runtime_error("Unsupported REST result value");
  if (type == "BLOB")
    field.setDataType(TSDataType::BLOB);
  else if (type == "TIMESTAMP")
    field.setDataType(TSDataType::TIMESTAMP);
  else if (type == "TEXT")
    field.setDataType(TSDataType::TEXT);
  return field;
}
} // namespace

void ODBCRestResultSet::parseJsonResponseTree(const nlohmann::json& response) {
  isMetaData = !response.contains("expressions") || response["expressions"].is_null() ||
               response["expressions"].empty();
  const char* names = isMetaData ? "column_names" : "expressions";
  if (response.contains(names) && response[names].is_array())
    columnNames = response[names].get<std::vector<std::string>>();
  if (response.contains("data_types") && response["data_types"].is_array())
    columnTypes = response["data_types"].get<std::vector<std::string>>();
  if (!isMetaData) {
    columnNames.insert(columnNames.begin(), "Time");
    columnTypes.insert(columnTypes.begin(), "TIMESTAMP");
  }
  numColumns = static_cast<int>(columnNames.size());
  const auto& values = response.at("values");
  numRows = isMetaData ? (values.empty() ? 0 : static_cast<int>(values.at(0).size()))
                       : static_cast<int>(response.at("timestamps").size());
  for (int row = 0; row < numRows; ++row) {
    std::vector<ODBCField> fields;
    for (int col = 0; col < numColumns; ++col) {
      const auto& value = !isMetaData && col == 0 ? response.at("timestamps").at(row)
                                                  : values.at(col - (isMetaData ? 0 : 1)).at(row);
      fields.push_back(jsonField(value, col < columnTypes.size() ? columnTypes[col] : ""));
    }
    data.push_back(std::move(fields));
  }
}

void ODBCRestResultSet::parseJsonResponseTable(const nlohmann::json& response) {
  isMetaData = false;
  if (response.contains("column_names") && response["column_names"].is_array())
    columnNames = response["column_names"].get<std::vector<std::string>>();
  if (response.contains("data_types") && response["data_types"].is_array())
    columnTypes = response["data_types"].get<std::vector<std::string>>();
  numColumns = static_cast<int>(columnNames.size());
  for (const auto& row : response.at("values")) {
    std::vector<ODBCField> fields;
    for (size_t col = 0; col < row.size(); ++col)
      fields.push_back(jsonField(row[col], col < columnTypes.size() ? columnTypes[col] : ""));
    data.push_back(std::move(fields));
  }
  numRows = static_cast<int>(data.size());
}
