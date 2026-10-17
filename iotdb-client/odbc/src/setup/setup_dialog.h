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
#ifndef SETUP_DIALOG_H
#define SETUP_DIALOG_H

#ifdef WIN32

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <odbcinst.h>
#include <string>

BOOL ShowDSNDialog(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszAttributes);

struct DsnInfo {
  std::string dsnName;
  std::string description;
  std::string server;
  std::string port;
  std::string uid;
  std::string pwd;
  std::string database;
  std::string isTableModel;
  std::string logLevel;
  std::string sessionTimeoutMs;
  std::string batchSize;
  std::string ssl = "0";
  std::string sslca;
  std::string sslcert;
  std::string sslkey;
};

void ReadDsnFromOdbcIni(const std::string& dsnName, DsnInfo& info);
BOOL WriteDsnToOdbcIni(const DsnInfo& info, const std::string& driverName);

#endif // WIN32
#endif // SETUP_DIALOG_H
