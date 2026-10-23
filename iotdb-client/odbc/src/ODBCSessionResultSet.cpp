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
#include "ODBCSessionResultSet.h"

// Project includes
#include "Common.h"
#include "Log.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"
#include "driver.h"
#include "SessionDataSet.h"
#include "StatementHandle.h"
#include <cmath>

ODBCSessionResultSet::ODBCSessionResultSet(StatementHandle* stmtHandle)
    : ODBCResultSet(stmtHandle) {
  // Initialize member variables - sessionDataSet is automatically initialized as nullptr
}

ODBCSessionResultSet::~ODBCSessionResultSet() {
  if (sessionDataSet) {
    logMessage(stmt->getConnection(),
               "ODBCSessionResultSet::~ODBCSessionResultSet: Closing session data set "
               "(closeOperationHandle()). WARNING: This is an abnormal operation, should be closed "
               "in loadDataInternal()",
               LOG_LEVEL_WARN);
    try {
      sessionDataSet->closeOperationHandle();
    } catch (...) {
      // Destruction may follow a failed connection; do not terminate the host process.
    }
  }
  // Destructor - unique_ptr will handle cleanup automatically
}

// Implement Session data loading
void ODBCSessionResultSet::loadData(std::unique_ptr<SessionDataSet> _sessionDataSet) {
  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadData: Starting", LOG_LEVEL_TRACE);

  // Clear existing data
  clear();
  TSDataTypeColumnTypes.clear();

  // Store the SessionDataSet for batch processing
  sessionDataSet = std::move(_sessionDataSet);

  bool res = loadDataMetaData();
  if (!res) {
    logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadData: Failed to load metadata",
               LOG_LEVEL_ERROR);
    return;
  }
  res = loadDataInternal();
  if (!res) {
    logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadData: Failed to load data",
               LOG_LEVEL_ERROR);
    return;
  }

  logMessage(stmt->getConnection(),
             "ODBCSessionResultSet::loadData: Initialized with " + std::to_string(numColumns) +
                 " columns, loaded first batch",
             LOG_LEVEL_DEBUG);
  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadData: Exiting", LOG_LEVEL_TRACE);
}

bool ODBCSessionResultSet::loadDataNextBatch() {
  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadDataNextBatch: Starting",
             LOG_LEVEL_TRACE);
  bool res = loadDataInternal();
  if (!res) {
    logMessage(stmt->getConnection(),
               "ODBCSessionResultSet::loadDataNextBatch: Failed to load data", LOG_LEVEL_ERROR);
    return false;
  }
  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadDataNextBatch: Exiting",
             LOG_LEVEL_TRACE);
  return true;
}

bool ODBCSessionResultSet::loadDataMetaData() {
  if (!sessionDataSet) {
    logMessage(stmt->getConnection(),
               "ODBCSessionResultSet::loadSessionData: No data set available", LOG_LEVEL_WARN);
    return false;
  }
  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadDataMetaData: Starting",
             LOG_LEVEL_TRACE);

  // Get column information
  columnNames = sessionDataSet->getColumnNames();
  columnTypes = sessionDataSet->getColumnTypeList();

  // Normalize each column name:
  // 1) If it ends with '`', keep content between the last two '`' (inclusive)
  // 2) Otherwise, strip prefix up to the last '.'
  for (size_t i = 0; i < columnNames.size(); ++i) {
    // Relational aliases are already final names and may contain dots.
    if (stmt->getConnection()->isTableModel)
      continue;
    if (!columnNames[i].empty() && columnNames[i].back() == '`') {
      size_t secondLastBacktick = columnNames[i].rfind('`', columnNames[i].size() - 2);
      if (secondLastBacktick != std::string::npos) {
        columnNames[i] = columnNames[i].substr(secondLastBacktick);
      }
    } else {
      size_t lastDot = columnNames[i].rfind('.');
      if (lastDot != std::string::npos) {
        columnNames[i] = columnNames[i].substr(lastDot + 1);
      }
    }
  }

  for (const auto& columnType : columnTypes) {
    if (columnTypeToTSDataTypeMap.find(columnType) == columnTypeToTSDataTypeMap.end()) {
      logMessage(stmt->getConnection(),
                 "ODBCSessionResultSet::loadSessionData: Unknown column type: " + columnType,
                 LOG_LEVEL_ERROR);
      throw std::runtime_error("Unsupported Session result type: " + columnType);
    }
    TSDataTypeColumnTypes.push_back(columnTypeToTSDataTypeMap.at(columnType));
  }
  numColumns = columnNames.size();
  logMessage(stmt->getConnection(),
             "ODBCSessionResultSet::loadSessionData: " + std::to_string(numColumns) +
                 " columns found",
             LOG_LEVEL_DEBUG);

  logMessage(stmt->getConnection(), "ODBCSessionResultSet::loadDataMetaData: Exiting",
             LOG_LEVEL_TRACE);
  return true;
}

bool ODBCSessionResultSet::loadDataInternal() {
  if (!sessionDataSet) {
    logMessage(stmt->getConnection(),
               "ODBCSessionResultSet::loadSessionData: No data set available", LOG_LEVEL_WARN);
    return false;
  }

  data.clear();
  numRows = 0;

  // Get data iterator for efficient access
  auto dataIterator = sessionDataSet->getIterator();
  // Load up to BATCH_SIZE rows
  int loadedRows = 0;
  try {
    while (loadedRows < stmt->getConnection()->batchSize) {
      bool hasNextRow = dataIterator.next();
      if (!hasNextRow) {
        // No more rows to load
        logMessage(stmt->getConnection(),
                   "ODBCSessionResultSet::loadDataInternal: No more rows to load, closing session "
                   "data set (closeOperationHandle())",
                   LOG_LEVEL_DEBUG);
        sessionDataSet->closeOperationHandle();
        sessionDataSet.reset();
        break;
      }
      std::vector<ODBCField> rowData(numColumns);

      // Process each column/field
      for (size_t col = 0; col < numColumns; ++col) {
        int dataIteratorIndex = col + 1;
        ODBCField& field = rowData[col];

        // Session result conversion is shared by table and tree model responses.
        // in the session response between the two models.
        logMessage(stmt->getConnection(),
                   "ODBCSessionResultSet::loadDataInternal: Loading column " + std::to_string(col),
                   LOG_LEVEL_TRACE);
        // Try different data types
        if (dataIterator.isNullByIndex(dataIteratorIndex)) {
          field = ODBCField::null();
          continue;
        }
        // Try to retrieve actual data
        TSDataType::TSDataType tsDataType = TSDataTypeColumnTypes[col];
        logMessage(stmt->getConnection(),
                   "ODBCSessionResultSet::loadDataInternal: Loading column " + std::to_string(col) +
                       " with type " + columnTypes[col],
                   LOG_LEVEL_DEBUG);

        switch (tsDataType) {
        case TSDataType::BOOLEAN: {
          Optional<bool> boolVal = dataIterator.getBooleanByIndex(dataIteratorIndex);
          if (boolVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Boolean Value: " +
                           std::to_string(boolVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(boolVal.value());
            field.setDataType(TSDataType::BOOLEAN);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Boolean Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::INT32: {
          Optional<int32_t> intVal = dataIterator.getIntByIndex(dataIteratorIndex);
          if (intVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Int Value: " +
                           std::to_string(intVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(static_cast<int>(intVal.value()));
            field.setDataType(TSDataType::INT32);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Int Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::INT64: {
          Optional<int64_t> longVal = dataIterator.getLongByIndex(dataIteratorIndex);
          if (longVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Long Value: " +
                           std::to_string(longVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(longVal.value());
            field.setDataType(TSDataType::INT64);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Long Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::FLOAT: {
          Optional<float> floatVal = dataIterator.getFloatByIndex(dataIteratorIndex);
          if (floatVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Float Value: " +
                           std::to_string(floatVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(floatVal.value());
            field.setDataType(TSDataType::FLOAT);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Float Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::DOUBLE: {
          Optional<double> doubleVal = dataIterator.getDoubleByIndex(dataIteratorIndex);
          if (doubleVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Double Value: " +
                           std::to_string(doubleVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(doubleVal.value());
            field.setDataType(TSDataType::DOUBLE);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Double Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::STRING: {
          Optional<std::string> stringVal = dataIterator.getStringByIndex(dataIteratorIndex);
          if (stringVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get String Value: " +
                           stringVal.value(),
                       LOG_LEVEL_DEBUG);
            field.setValue(stringVal.value());
            field.setDataType(TSDataType::STRING);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get String Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::DATE: {
          Optional<IoTDBDate> dateVal = dataIterator.getDateByIndex(dataIteratorIndex);
          if (dateVal) {
            logMessage(
                stmt->getConnection(),
                "ODBCSessionResultSet::loadDataInternal: Get Date Value (Dont output for now)",
                LOG_LEVEL_DEBUG);
            field.setValue(dateVal.value());
            field.setDataType(TSDataType::DATE);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Date Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::BLOB: {
          Optional<std::string> blobVal = dataIterator.getStringByIndex(dataIteratorIndex);
          if (blobVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Blob Value: " + blobVal.value(),
                       LOG_LEVEL_DEBUG);
            field.setValue(blobVal.value());
            field.setDataType(TSDataType::BLOB);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Blob Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::TIMESTAMP: {
          Optional<int64_t> timestampVal = dataIterator.getTimestampByIndex(dataIteratorIndex);
          if (timestampVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Timestamp Value: " +
                           std::to_string(timestampVal.value()),
                       LOG_LEVEL_DEBUG);
            field.setValue(timestampVal.value());
            field.setDataType(TSDataType::TIMESTAMP);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Timestamp Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        case TSDataType::TEXT: {
          Optional<std::string> textVal = dataIterator.getStringByIndex(dataIteratorIndex);
          if (textVal) {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Text Value: " + textVal.value(),
                       LOG_LEVEL_DEBUG);
            field.setValue(textVal.value());
            field.setDataType(TSDataType::TEXT);
          } else {
            logMessage(stmt->getConnection(),
                       "ODBCSessionResultSet::loadDataInternal: Get Text Value: null",
                       LOG_LEVEL_DEBUG);
          }
          break;
        }
        default:
          logMessage(stmt->getConnection(),
                     "ODBCSessionResultSet::loadDataInternal: UNKNOWN TYPE, setting to null",
                     LOG_LEVEL_ERROR);
          field = ODBCField::null();
          break;
        }
      }
      logMessage(stmt->getConnection(),
                 "ODBCSessionResultSet::loadDataInternal: copying row into data", LOG_LEVEL_DEBUG);
      data.push_back(rowData);
      loadedRows++;
    }
  } catch (const std::exception& e) {
    logMessage(stmt->getConnection(),
               std::string("ODBCSessionResultSet::loadDataInternal: Exception: ") + e.what(),
               LOG_LEVEL_ERROR);
    throw;
  } catch (...) {
    logMessage(stmt->getConnection(),
               "ODBCSessionResultSet::loadDataInternal: Unknown exception occurred",
               LOG_LEVEL_ERROR);
    throw std::runtime_error("Unable to read Session result batch");
  }

  numRows = data.size();

  logMessage(stmt->getConnection(),
             "ODBCSessionResultSet::loadDataInternal: Loaded " + std::to_string(numRows) +
                 " rows in this batch",
             LOG_LEVEL_DEBUG);

  // Return true if we loaded some data, false if no more data
  return numRows > 0;
}
