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
#include "rest_api_client.h"

// Additional third-party includes not in Pch.h
#include <curl/curl.h>

// Project includes
#include "Log.h"
#include "driver.h"
#include "DiagnosticManager.h"
#include "StatementUtils.h"

namespace {
class CurlGlobalState {
public:
  CurlGlobalState() : result(curl_global_init(CURL_GLOBAL_ALL)) {}
  ~CurlGlobalState() {
    if (result == CURLE_OK)
      curl_global_cleanup();
  }

  CURLcode result;
};

struct CurlEasyDeleter {
  void operator()(CURL* handle) const {
    if (handle)
      curl_easy_cleanup(handle);
  }
};

struct CurlHeadersDeleter {
  void operator()(curl_slist* headers) const {
    if (headers)
      curl_slist_free_all(headers);
  }
};
} // namespace

// Callback function for writing received data
size_t WriteCallback(void* contents, const size_t size, const size_t nmemb, std::string* response) {
  const size_t totalSize = size * nmemb;
  response->append(static_cast<char*>(contents), totalSize);
  return totalSize;
}

SQLRETURN executeRestCall(ConnectionHandle* cnct, const std::string& relativeUrl,
                          const nlohmann::json& jsonPayload, nlohmann::json* jsonResponse) {
  logMessage(cnct, "  Entering executeRestCall", LOG_LEVEL_TRACE);

  static CurlGlobalState curlGlobal;
  if (curlGlobal.result != CURLE_OK) {
    logMessage(cnct, "Curl global initialization failed", LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY000", "Couldn't initialize the REST client");
    return SQL_ERROR;
  }

  std::unique_ptr<CURL, CurlEasyDeleter> curl(curl_easy_init());
  if (!curl) {
    logMessage(cnct, "Curl initialization failed", LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY000", "Couldn't initialize the REST client");
    return SQL_ERROR;
  }

  try {
    const std::string fullUrl =
        "http://" + cnct->serverHostName + ":" + cnct->serverPort + relativeUrl;
    logMessage(cnct, "Connecting to: " + fullUrl, LOG_LEVEL_DEBUG);
    curl_easy_setopt(curl.get(), CURLOPT_URL, fullUrl.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_HTTPAUTH, static_cast<long>(CURLAUTH_BASIC));
    curl_easy_setopt(curl.get(), CURLOPT_USERNAME, cnct->userName.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_PASSWORD, cnct->password.c_str());

    std::unique_ptr<curl_slist, CurlHeadersDeleter> headers(
        curl_slist_append(nullptr, "Content-Type: application/json"));
    if (!headers) {
      cnct->addDiagnostic("HY001", "Couldn't allocate REST request headers");
      return SQL_ERROR;
    }
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());

    const std::string jsonString = jsonPayload.dump();
    curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, jsonString.c_str());

    std::string response;
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);

    const CURLcode result = curl_easy_perform(curl.get());
    if (result != CURLE_OK) {
      logMessage(cnct, curl_easy_strerror(result), LOG_LEVEL_ERROR);
      cnct->addDiagnostic("08001", "The specified server URL could not be reached.");
      return SQL_ERROR;
    }

    long httpCode = 0;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &httpCode);
    logMessage(cnct, "Http response code: " + std::to_string(httpCode), LOG_LEVEL_DEBUG);
    if (httpCode != 200) {
      logMessage(cnct, "Got an invalid response code from the server", LOG_LEVEL_ERROR);
      cnct->addDiagnostic("08001", "Invalid response");
      return SQL_ERROR;
    }

    try {
      *jsonResponse = nlohmann::json::parse(response);
    } catch (const nlohmann::json::parse_error& e) {
      logMessage(cnct, "Failed to parse JSON response: " + std::string(e.what()), LOG_LEVEL_ERROR);
      cnct->addDiagnostic("22018", "Failed to parse JSON response: " + std::string(e.what()));
      return SQL_ERROR;
    }
  } catch (const std::exception& e) {
    logMessage(cnct, "Unexpected error: " + std::string(e.what()), LOG_LEVEL_ERROR);
    cnct->addDiagnostic("HY000", "Unexpected error: " + std::string(e.what()));
    return SQL_ERROR;
  }

  logMessage(cnct, "Exiting executeRestCall", LOG_LEVEL_TRACE);
  return SQL_SUCCESS;
}

// Execute a simple query or modification statement directly through connection handle and return json data, no statement handle involved.
SQLRETURN executeQuery(ConnectionHandle* cnct, std::string statementText,
                       nlohmann::json* jsonResponse) {
  logMessage(cnct, "executeQuery: Entering", LOG_LEVEL_TRACE);
  bool isQuery = IsQueryStatement(statementText);

  std::string logMessageText =
      "executeQuery: is " + std::string(isQuery ? "query" : "nonQuery") + ".";
  logMessage(cnct, logMessageText, LOG_LEVEL_DEBUG);

  nlohmann::json jsonPayload;
  if (cnct->isTableModel) {
    if (cnct->database == "") {
      logMessage(cnct, "executeQuery: In table model mode, but no database is set",
                 LOG_LEVEL_ERROR);
      return SQL_ERROR;
    }
    jsonPayload = {{"database", cnct->database}, {"sql", statementText}};
  } else {
    jsonPayload = {{"sql", statementText}};
  }
  const std::string relativeUrl = std::string(cnct->isTableModel ? "/rest/table/v1" : "/rest/v2") +
                                  std::string(isQuery ? "/query" : "/nonQuery");
  logMessage(cnct, "Target URL: " + relativeUrl, LOG_LEVEL_DEBUG);
  const SQLRETURN returnCode = executeRestCall(cnct, relativeUrl, jsonPayload, jsonResponse);
  logMessage(cnct, "Exiting executeQuery", LOG_LEVEL_TRACE);
  return returnCode;
}

// Test RESTful API connection and return connection result
SQLRETURN IoTDB_DriverConnect_Rest(ConnectionHandle* cnct) {
  try {
    cnct->ValidateTransport();
  } catch (const std::exception& e) {
    cnct->addDiagnostic("08001", e.what());
    return SQL_ERROR;
  }
  logMessage(cnct, "IoTDB_DriverConnect_Rest: Testing connection with SHOW VERSION query",
             LOG_LEVEL_TRACE);
  nlohmann::json jsonPayload;
  if (cnct->isTableModel) {
    jsonPayload = {{"database", cnct->database}, {"sql", "SHOW VERSION"}};
  } else {
    jsonPayload = {{"sql", "SHOW VERSION"}}; // Use SHOW VERSION to test connection
  }
  nlohmann::json jsonResponse;
  std::string relativeUrl = cnct->isTableModel ? "/rest/table/v1/query" : "/rest/v2/query";
  SQLRETURN returnCode = executeRestCall(cnct, relativeUrl, jsonPayload, &jsonResponse);

  if (returnCode == SQL_SUCCESS) {
    logMessage(cnct, "IoTDB_DriverConnect_Rest: Received successful HTTP response",
               LOG_LEVEL_DEBUG);
    try {
      auto responseCode = jsonResponse.value("code", 200);
      switch (responseCode) {
      case 200:
        logMessage(cnct, "IoTDB_DriverConnect_Rest: Connection test successful", LOG_LEVEL_INFO);
        returnCode = SQL_SUCCESS;
        break;

      case 708:
        logMessage(cnct, "IoTDB_DriverConnect_Rest: Response too large", LOG_LEVEL_ERROR);
        cnct->addDiagnostic("28000", "Response too large");
        returnCode = SQL_ERROR;
        break;

      case 411:
        logMessage(cnct, "IoTDB_DriverConnect_Rest: Data requires streaming transmission",
                   LOG_LEVEL_ERROR);
        cnct->addDiagnostic("28000", "Data requires streaming transmission for connection test");
        returnCode = SQL_ERROR;
        break;

      case 801:
        logMessage(cnct, "IoTDB_DriverConnect_Rest: Invalid credentials", LOG_LEVEL_ERROR);
        cnct->addDiagnostic("28000", "Invalid username or password");
        returnCode = SQL_ERROR;
        break;

      case 700:
        logMessage(cnct, "IoTDB_DriverConnect_Rest: SQL syntax error in connection test",
                   LOG_LEVEL_ERROR);
        cnct->addDiagnostic("42000", "SQL syntax error in connection test query");
        returnCode = SQL_ERROR;
        break;

      default:
        if (responseCode >= 200 && responseCode < 300) {
          logMessage(cnct,
                     "IoTDB_DriverConnect_Rest: Connection test successful with response code " +
                         std::to_string(responseCode),
                     LOG_LEVEL_INFO);
          returnCode = SQL_SUCCESS;
        } else {
          logMessage(cnct,
                     "IoTDB_DriverConnect_Rest: Unhandled response code: " +
                         std::to_string(responseCode),
                     LOG_LEVEL_ERROR);
          cnct->addDiagnostic("28000", "Unexpected return code " + std::to_string(responseCode));
          returnCode = SQL_ERROR;
        }
        break;
      }
    } catch (const std::exception& e) {
      logMessage(
          cnct, "IoTDB_DriverConnect_Rest: Exception processing response: " + std::string(e.what()),
          LOG_LEVEL_ERROR);
      cnct->addDiagnostic("HY000", "Exception processing response: " + std::string(e.what()));
      returnCode = SQL_ERROR;
    } catch (...) {
      logMessage(cnct, "IoTDB_DriverConnect_Rest: Unknown exception processing response",
                 LOG_LEVEL_ERROR);
      cnct->addDiagnostic("HY000", "Unknown exception processing response");
      returnCode = SQL_ERROR;
    }
  } else {
    logMessage(cnct,
               "IoTDB_DriverConnect_Rest: Connection test failed with code: " +
                   std::to_string(returnCode),
               LOG_LEVEL_ERROR);
  }

  logMessage(cnct,
             "IoTDB_DriverConnect_Rest: Exiting with return code: " + std::to_string(returnCode),
             LOG_LEVEL_TRACE);
  return returnCode;
}

// Execute direct statement execution for RESTful API
SQLRETURN IoTDB_ExecDirect_Rest(StatementHandle* stmt, const std::string& statementText) {
  ConnectionHandle* cnct = stmt->getConnection();
  logMessage(cnct, "IoTDB_ExecDirect_Rest: Entering", LOG_LEVEL_TRACE);

  bool isQuery = IsQueryStatement(statementText);
  // Do not log SQL text: statements can contain credentials or user data.
  if (isLogLevelEnabled(cnct, LOG_LEVEL_DEBUG)) {
    const std::string logMessageText =
        "IoTDB_ExecDirect_Rest: Statement type = " + std::string(isQuery ? "query" : "nonQuery");
    logMessage(cnct, logMessageText, LOG_LEVEL_DEBUG);
  }

  nlohmann::json jsonPayload;
  if (stmt->getConnection()->isTableModel) {
    if (stmt->getConnection()->database == "") {
      logMessage(cnct, "IoTDB_ExecDirect_Rest: Table model requires database name but none is set",
                 LOG_LEVEL_ERROR);
      return SQL_ERROR;
    }
    jsonPayload = {{"database", stmt->getConnection()->database}, {"sql", statementText}};
  } else {
    jsonPayload = {{"sql", statementText}};
  }

  nlohmann::json jsonResponse;
  const std::string relativeUrl =
      std::string(stmt->getConnection()->isTableModel ? "/rest/table/v1" : "/rest/v2") +
      std::string(isQuery ? "/query" : "/nonQuery");

  std::string fullServerUrl =
      "http://" + stmt->getConnection()->serverHostName + ":" + stmt->getConnection()->serverPort;
  logMessage(cnct,
             "IoTDB_ExecDirect_Rest: Calling executeRestCall with serverHostName: " +
                 fullServerUrl + "; relative URL: " + relativeUrl,
             LOG_LEVEL_DEBUG);
  const SQLRETURN returnCode =
      executeRestCall(stmt->getConnection(), relativeUrl, jsonPayload, &jsonResponse);
  if (returnCode == SQL_SUCCESS) {
    logMessage(cnct, "IoTDB_ExecDirect_Rest: Received successful HTTP response", LOG_LEVEL_DEBUG);
    try {
      const auto responseCode = jsonResponse.value("code", 200);
      logMessage(cnct, "IoTDB_ExecDirect_Rest: Response code = " + std::to_string(responseCode),
                 LOG_LEVEL_DEBUG);
      switch (responseCode) {
      case 200: {
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Request completed successfully", LOG_LEVEL_INFO);
        stmt->isQuery = isQuery;
        if (!isQuery) {
          stmt->ClearResultSet();
          return SQL_SUCCESS;
        }
        stmt->AllocateRestResultSet();
        stmt->resultSetPtr->loadData(jsonResponse);
        if (stmt->isStream && stmt->resultSetPtr->getNumRows() == 0) {
          logMessage(cnct, "IoTDB_ExecDirect_Rest: Streaming completed (empty result set)",
                     LOG_LEVEL_INFO);
          stmt->isStream = false;
          // receiving an empty set means streaming is end.
          return SQL_NO_DATA;
        }
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Exiting with success", LOG_LEVEL_TRACE);
        return SQL_SUCCESS;
      }
      case 801:
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Invalid credentials", LOG_LEVEL_ERROR);

        stmt->addDiagnostic("28000", "Invalid username or password");
        return SQL_ERROR;

      case 708:
      case 411:
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Data requires streaming transmission",
                   LOG_LEVEL_INFO);
        stmt->isQuery = true;
        stmt->AllocateRestResultSet();
        stmt->resultSetPtr->loadData(jsonResponse);
        stmt->isStream = true;
        stmt->streamCurRow = 0;
        return streamNextBatch(stmt);
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Exiting with streaming setup", LOG_LEVEL_TRACE);
        return SQL_SUCCESS;

      case 700:
        logMessage(cnct, "IoTDB_ExecDirect_Rest: Mismatched input in SQL statement",
                   LOG_LEVEL_ERROR);
        stmt->addDiagnostic("42000", "Mismatched input.");
        return SQL_ERROR;

      default:
        logMessage(
            cnct, "IoTDB_ExecDirect_Rest: Unhandled response code: " + std::to_string(responseCode),
            LOG_LEVEL_ERROR);

        stmt->addDiagnostic("28000", "Unexpected return code " + std::to_string(returnCode));
        return SQL_ERROR;
      }
    } catch (const std::exception& e) {
      logMessage(cnct,
                 "IoTDB_ExecDirect_Rest: Exception processing response: " + std::string(e.what()),
                 LOG_LEVEL_ERROR);
      stmt->addDiagnostic("HY000", "Exception processing response: " + std::string(e.what()));
      return SQL_ERROR;
    } catch (...) {
      logMessage(cnct, "IoTDB_ExecDirect_Rest: Unknown exception processing response",
                 LOG_LEVEL_ERROR);
      stmt->addDiagnostic("HY000", "Unknown exception processing response");
      return SQL_ERROR;
    }
  }

  logMessage(cnct,
             "IoTDB_ExecDirect_Rest: HTTP request failed with code: " + std::to_string(returnCode),
             LOG_LEVEL_ERROR);
  logMessage(cnct, "IoTDB_ExecDirect_Rest: Exiting with error", LOG_LEVEL_TRACE);
  return returnCode;
}
