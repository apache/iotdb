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

#include "StatementHandle.h"
#include "Log.h"
#include "driver.h"

SQLSMALLINT streamNextBatch(StatementHandle* stmt) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "streamNextBatch: Entering", LOG_LEVEL_TRACE);

  // Only SELECT statements can be streamed
  if (stmt->isQuery == false || (stmt->resultSetPtr->isMetaData && !stmt->isStream)) {
    logMessage(cnct,
               "streamNextBatch: Statement is not a SELECT query, cannot stream, comes to an end. "
               "returning SQL_NO_DATA",
               LOG_LEVEL_INFO);
    return SQL_NO_DATA;
  }

  // if fewer than page size rows were returned, it means the end of the data
  if (stmt->resultSetPtr->getNumRows() < cnct->batchSize) {
    logMessage(cnct,
               "streamNextBatch: Fewer rows than fetch page size returned, end of data reached. "
               "returning SQL_NO_DATA",
               LOG_LEVEL_INFO);
    return SQL_NO_DATA;
  }

  if (!cnct->useRestful) {
    bool ret;
    try {
      ret = static_cast<ODBCSessionResultSet*>(stmt->resultSetPtr.get())->loadDataNextBatch();
    } catch (const std::exception& error) {
      stmt->addDiagnostic("HY000", error.what());
      return SQL_ERROR;
    }
    stmt->streamCurRow += stmt->resultSetPtr->getNumRows();
    if (!ret) {
      logMessage(cnct, "streamNextBatch: Failed to load next batch of data", LOG_LEVEL_ERROR);
      return SQL_NO_DATA;
    }
    return SQL_SUCCESS;
  }

  // REST responses that did not request pagination already contain all rows.
  if (!stmt->isStream)
    return SQL_NO_DATA;

  std::stringstream newStatementTextss;
  if (stmt->resultSetPtr->getNumRows() > 0) {
    stmt->streamCurRow += stmt->resultSetPtr->getNumRows();
    logMessage(cnct,
               "streamNextBatch: Updated streamCurRow to " + std::to_string(stmt->streamCurRow),
               LOG_LEVEL_DEBUG);
  }
  if (isLogLevelEnabled(cnct, LOG_LEVEL_DEBUG)) {
    std::stringstream oss;
    oss << "streamNextBatch: curRow = " << stmt->curRow;
    logMessage(cnct, oss.str(), LOG_LEVEL_DEBUG);
  }

  newStatementTextss << stmt->statementText << " OFFSET " << stmt->streamCurRow << " LIMIT 9000";

  logMessage(cnct, "streamNextBatch: Executing the next query batch", LOG_LEVEL_DEBUG);
  SQLRETURN ret = IoTDB_ExecDirect(stmt, newStatementTextss.str(), false);
  if (ret == SQL_NO_DATA) {
    logMessage(cnct, "streamNextBatch: Streaming ended. returning SQL_NO_DATA", LOG_LEVEL_ERROR);
    return ret;
  }
  if (ret != SQL_SUCCESS) {
    logMessage(cnct,
               "streamNextBatch: Failed to execute next batch with code: " + std::to_string(ret),
               LOG_LEVEL_ERROR);
    return ret;
  } else {
    logMessage(cnct, "streamNextBatch: Successfully executed next batch", LOG_LEVEL_TRACE);
  }
  logMessage(cnct, "streamNextBatch: Exiting", LOG_LEVEL_TRACE);
  return ret;
}

void StatementHandle::AllocateSessionResultSet() {
  logMessage(getConnection(), "StatementHandle::AllocateSessionResultSet: Entering",
             LOG_LEVEL_TRACE);
  resultSetPtr = std::make_shared<ODBCSessionResultSet>(this);
  resultSetPtr->bindColInfo = columnBindings;
  logMessage(getConnection(), "StatementHandle::AllocateSessionResultSet: Exiting",
             LOG_LEVEL_TRACE);
}
void StatementHandle::AllocateRestResultSet() {
  logMessage(getConnection(), "StatementHandle::AllocateRestResultSet: Entering", LOG_LEVEL_TRACE);
  resultSetPtr = std::make_shared<ODBCRestResultSet>(this);
  resultSetPtr->bindColInfo = columnBindings;
  logMessage(getConnection(), "StatementHandle::AllocateRestResultSet: Exiting", LOG_LEVEL_TRACE);
}
void StatementHandle::ClearResultSet() {
  logMessage(getConnection(), "StatementHandle::ClearResultSet: Entering", LOG_LEVEL_TRACE);
  resultSetPtr = nullptr;
  logMessage(getConnection(), "StatementHandle::ClearResultSet: Exiting", LOG_LEVEL_TRACE);
}
