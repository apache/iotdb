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
#include "Pch.h"
#include "session_api_client.h"

// Project includes
#include "Log.h"
#include "driver.h"
#include "DiagnosticManager.h"
#include "ODBCResultSet.h"
#include "StatementUtils.h"

// IoTDB Session API includes
#include <TableSession.h>
#include <TableSessionBuilder.h>
#include <Session.h>
#include <SessionBuilder.h>
#include <SessionDataSet.h>

// Forward declarations for functions defined in driver.cpp
SQLRETURN IoTDB_ExecDirect_Session(StatementHandle* stmt, const std::string& statementText);

// Test Session API connection and return connection result
SQLRETURN IoTDB_DriverConnect_Session(ConnectionHandle* cnct) {
  logMessage(cnct, "IoTDB_DriverConnect_Session: Testing connection with SHOW VERSION query",
             LOG_LEVEL_TRACE);

  try {
    // Parse server port from string to int
    int rpcPort = 6667; // Default port
    try {
      rpcPort = std::stoi(cnct->serverPort);
      logMessage(cnct,
                 "IoTDB_DriverConnect_Session: Using configured port: " + std::to_string(rpcPort),
                 LOG_LEVEL_DEBUG);
    } catch (...) {
      logMessage(cnct,
                 "IoTDB_DriverConnect_Session: Invalid port in serverPort (" + cnct->serverPort +
                     "), using default 6667",
                 LOG_LEVEL_WARN);
    }

    if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE)) {
      std::string tmp;
      tmp = "Server URL: " + cnct->serverHostName + ", Port: " + std::to_string(rpcPort) +
            ", User: " + cnct->userName + ", Database: " + cnct->database +
            ", Table Model: " + (cnct->isTableModel ? "true" : "false");
      logMessage(cnct, "IoTDB_DriverConnect_Session: All connection paras: " + tmp,
                 LOG_LEVEL_TRACE);
    }

    // Create session
    std::shared_ptr<TableSession> tableSession = nullptr;
    std::shared_ptr<Session> session = nullptr;
    if (cnct->isTableModel) {
      if (cnct->database.empty()) {
        logMessage(
            cnct, "IoTDB_DriverConnect_Session: Table model requires database name but none is set",
            LOG_LEVEL_ERROR);
        cnct->addDiagnostic("08001", "Database not specified for table model");
        return SQL_ERROR;
      }
      logMessage(cnct, "IoTDB_DriverConnect_Session: Creating table session", LOG_LEVEL_TRACE);
      cnct->OpenSession();
      tableSession = cnct->tableSessionPtr;
    } else {
      logMessage(cnct, "IoTDB_DriverConnect_Session: Creating tree session", LOG_LEVEL_TRACE);
      cnct->OpenSession();
      session = cnct->sessionPtr;
    }

    // Test connection with SHOW VERSION query
    std::unique_ptr<SessionDataSet> dataSet =
        cnct->isTableModel
            ? tableSession->executeQueryStatement("SHOW VERSION", cnct->sessionTimeoutMs)
            : session->executeQueryStatement("SHOW VERSION", cnct->sessionTimeoutMs);

    if (dataSet && dataSet->hasNext()) {
      logMessage(cnct, "IoTDB_DriverConnect_Session: Connection test successful", LOG_LEVEL_INFO);
      dataSet->closeOperationHandle();
      return SQL_SUCCESS;
    } else {
      logMessage(cnct, "IoTDB_DriverConnect_Session: Connection test failed - no data returned",
                 LOG_LEVEL_ERROR);
      cnct->CloseSession();
      cnct->addDiagnostic("08001", "Connection test failed");
      return SQL_ERROR;
    }

  } catch (const IoTDBConnectionException& e) {
    logMessage(cnct, "IoTDB_DriverConnect_Session: Connection exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    cnct->addDiagnostic("08001", "Connection failed: " + std::string(e.what()));
    return SQL_ERROR;
  } catch (const IoTDBException& e) {
    logMessage(cnct, "IoTDB_DriverConnect_Session: IoTDB exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    cnct->addDiagnostic("08001", "Query execution failed: " + std::string(e.what()));
    return SQL_ERROR;
  } catch (const std::exception& e) {
    logMessage(cnct, "IoTDB_DriverConnect_Session: Unexpected exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY000", "Unexpected error: " + std::string(e.what()));
    return SQL_ERROR;
  }

  logMessage(cnct, "IoTDB_DriverConnect_Session: Exiting with error", LOG_LEVEL_TRACE);
  return SQL_ERROR;
}

// Execute direct statement execution for Session API
SQLRETURN IoTDB_ExecDirect_Session(StatementHandle* stmt, const std::string& statementText) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "IoTDB_ExecDirect_Session: Entering", LOG_LEVEL_TRACE);

  bool isQuery = IsQueryStatement(statementText);
  // Do not log SQL text: statements can contain credentials or user data.
  if (isLogLevelEnabled(cnct, LOG_LEVEL_DEBUG)) {
    const std::string logMessageText =
        "IoTDB_ExecDirect_Session: Statement type = " + std::string(isQuery ? "query" : "nonQuery");
    logMessage(cnct, logMessageText, LOG_LEVEL_DEBUG);
  }

  try {
    // Parse server port from string to int
    int rpcPort = 6667; // Default port
    try {
      rpcPort = std::stoi(cnct->serverPort);
      logMessage(cnct,
                 "IoTDB_ExecDirect_Session: Using configured port: " + std::to_string(rpcPort),
                 LOG_LEVEL_DEBUG);
    } catch (...) {
      logMessage(cnct,
                 "IoTDB_ExecDirect_Session: Invalid port in serverPort (" + cnct->serverPort +
                     "), using default 6667",
                 LOG_LEVEL_WARN);
    }

    // Ensure session is created
    stmt->ClearResultSet();
    if (cnct->isTableModel) {
      if (cnct->database.empty()) {
        logMessage(cnct,
                   "IoTDB_ExecDirect_Session: Table model requires database name but none is set",
                   LOG_LEVEL_ERROR);
        stmt->addDiagnostic("HY000", "Database not specified for table model");
        return SQL_ERROR;
      }
      if (!cnct->tableSessionPtr) {
        logMessage(cnct, "IoTDB_ExecDirect_Session: Creating table session", LOG_LEVEL_TRACE);
        cnct->OpenSession();
      }
    } else {
      if (!cnct->sessionPtr) {
        logMessage(cnct, "IoTDB_ExecDirect_Session: Creating tree session", LOG_LEVEL_TRACE);
        cnct->OpenSession();
      }
    }

    if (isQuery) {
      // Execute query statement
      std::unique_ptr<SessionDataSet> dataSet =
          cnct->isTableModel
              ? cnct->tableSessionPtr->executeQueryStatement(statementText, cnct->sessionTimeoutMs)
              : cnct->sessionPtr->executeQueryStatement(statementText, cnct->sessionTimeoutMs);
      // Process query results
      if (dataSet) {
        stmt->isQuery = true;
        stmt->AllocateSessionResultSet();
        stmt->resultSetPtr->loadData(std::move(dataSet));
        logMessage(cnct,
                   "IoTDB_ExecDirect_Session: Query executed successfully with batch processing",
                   LOG_LEVEL_INFO);
        if (isLogLevelEnabled(cnct, LOG_LEVEL_TRACE))
          stmt->resultSetPtr->outputTable();
        return SQL_SUCCESS;
      } else {
        logMessage(cnct, "IoTDB_ExecDirect_Session: Query returned no data", LOG_LEVEL_WARN);
        stmt->isQuery = false;
        stmt->ClearResultSet();
        return SQL_NO_DATA;
      }
    } else {
      // Execute non-query statement
      if (cnct->isTableModel) {
        cnct->tableSessionPtr->executeNonQueryStatement(statementText);
      } else {
        cnct->sessionPtr->executeNonQueryStatement(statementText);
      }
      stmt->ClearResultSet(); // Clear any existing data
      logMessage(cnct, "IoTDB_ExecDirect_Session: Non-query statement executed successfully",
                 LOG_LEVEL_INFO);
      return SQL_SUCCESS;
    }

  } catch (const IoTDBConnectionException& e) {
    logMessage(cnct, "IoTDB_ExecDirect_Session: Connection exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("08001", "Connection failed: " + std::string(e.what()));
    return SQL_ERROR;
  } catch (const IoTDBException& e) {
    logMessage(cnct, "IoTDB_ExecDirect_Session: IoTDB exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);

    // Handle specific error codes if possible
    std::string errorMsg = e.what();
    if (errorMsg.find("mismatched input") != std::string::npos) {
      stmt->addDiagnostic("42000", "SQL syntax error: " + errorMsg);
    } else {
      stmt->addDiagnostic("HY000", "Query execution failed: " + errorMsg);
    }
    return SQL_ERROR;
  } catch (const std::exception& e) {
    logMessage(cnct, "IoTDB_ExecDirect_Session: Unexpected exception: " + std::string(e.what()),
               LOG_LEVEL_ERROR);
    stmt->addDiagnostic("HY000", "Unexpected error: " + std::string(e.what()));
    return SQL_ERROR;
  }

  logMessage(cnct, "IoTDB_ExecDirect_Session: Exiting with error", LOG_LEVEL_TRACE);
  return SQL_ERROR;
}
