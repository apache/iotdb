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
#ifdef WIN32

#include "setup_dialog.h"
#include "../ConnectionHandle.h"
#include "resource.h"
#include "../Log.h"

#include <windows.h>
#include <string>
#include <sstream>
#include <odbcinst.h>

#include <Session.h>
#include <SessionBuilder.h>
#include <TableSession.h>
#include <TableSessionBuilder.h>
#include <SessionDataSet.h>

// ─── Helpers ────────────────────────────────────────────────────────────────

static HINSTANCE GetCurrentModuleHandle() {
  HMODULE hModule = nullptr;
  GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                         GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                     reinterpret_cast<LPCSTR>(&GetCurrentModuleHandle), &hModule);
  return static_cast<HINSTANCE>(hModule);
}

static std::string ParseDSNFromAttributes(LPCSTR lpszAttributes) {
  if (!lpszAttributes)
    return "";
  const char* p = lpszAttributes;
  while (*p) {
    std::string entry(p);
    size_t eq = entry.find('=');
    if (eq != std::string::npos) {
      std::string key = entry.substr(0, eq);
      std::string keyLower = key;
      for (auto& c : keyLower)
        c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
      if (keyLower == "dsn")
        return entry.substr(eq + 1);
    }
    p += entry.length() + 1;
  }
  return "";
}

static std::string ReadOdbcIniString(const std::string& dsn, const char* key, const char* def) {
  char buf[1024] = {};
  SQLGetPrivateProfileString(dsn.c_str(), key, def, buf, sizeof(buf), "ODBC.INI");
  return buf;
}

// ─── DSN read / write via ODBC Installer API ───────────────────────────────

void ReadDsnFromOdbcIni(const std::string& dsnName, DsnInfo& info) {
  logMessage(nullptr, "ReadDsnFromOdbcIni: Reading DSN '" + dsnName + "'", LOG_LEVEL_INFO);

  info.dsnName = dsnName;
  info.description = ReadOdbcIniString(dsnName, "Description", "");
  info.server = ReadOdbcIniString(dsnName, "SERVER", "127.0.0.1");
  info.port = ReadOdbcIniString(dsnName, "PORT", "6667");
  info.uid = ReadOdbcIniString(dsnName, "UID", "root");
  info.pwd = ReadOdbcIniString(dsnName, "PWD", "root");
  info.database = ReadOdbcIniString(dsnName, "DATABASE", "");
  info.isTableModel = ReadOdbcIniString(dsnName, "ISTABLEMODEL", "1");
  info.logLevel = ReadOdbcIniString(dsnName, "LOGLEVEL", "0");
  info.sessionTimeoutMs = ReadOdbcIniString(dsnName, "SESSIONTIMEOUTMS", "0");
  info.batchSize = ReadOdbcIniString(dsnName, "BATCHSIZE", "1000");
  info.ssl = ReadOdbcIniString(dsnName, "SSL", "0");
  info.sslca = ReadOdbcIniString(dsnName, "SSLCA", "");
  info.sslcert = ReadOdbcIniString(dsnName, "SSLCERT", "");
  info.sslkey = ReadOdbcIniString(dsnName, "SSLKEY", "");

  std::ostringstream oss;
  oss << "ReadDsnFromOdbcIni: SERVER=" << info.server << ", PORT=" << info.port
      << ", UID=" << info.uid << ", DATABASE=" << info.database
      << ", ISTABLEMODEL=" << info.isTableModel << ", LOGLEVEL=" << info.logLevel
      << ", SESSIONTIMEOUTMS=" << info.sessionTimeoutMs << ", BATCHSIZE=" << info.batchSize;
  logMessage(nullptr, oss.str(), LOG_LEVEL_DEBUG);
}

BOOL WriteDsnToOdbcIni(const DsnInfo& info, const std::string& driverName) {
  logMessage(nullptr, "WriteDsnToOdbcIni: Writing DSN '" + info.dsnName + "'", LOG_LEVEL_INFO);

  if (info.dsnName.empty()) {
    logMessage(nullptr, "WriteDsnToOdbcIni: DSN name is empty, aborting", LOG_LEVEL_ERROR);
    return FALSE;
  }

  if (!SQLValidDSN(info.dsnName.c_str())) {
    logMessage(nullptr, "WriteDsnToOdbcIni: Invalid DSN name '" + info.dsnName + "'",
               LOG_LEVEL_ERROR);
    return FALSE;
  }

  if (!SQLWriteDSNToIni(info.dsnName.c_str(), driverName.c_str())) {
    logMessage(nullptr, "WriteDsnToOdbcIni: SQLWriteDSNToIni failed", LOG_LEVEL_ERROR);
    return FALSE;
  }

  const char* dsn = info.dsnName.c_str();
  bool success = true;
  auto w = [&](const char* key, const std::string& val) {
    if (!SQLWritePrivateProfileString(dsn, key, val.c_str(), "ODBC.INI"))
      success = false;
  };

  w("Description", info.description);
  w("SERVER", info.server);
  w("PORT", info.port);
  w("UID", info.uid);
  w("PWD", info.pwd);
  w("DATABASE", info.database);
  w("ISTABLEMODEL", info.isTableModel);
  w("LOGLEVEL", info.logLevel);
  w("SESSIONTIMEOUTMS", info.sessionTimeoutMs);
  w("BATCHSIZE", info.batchSize);
  w("SSL", info.ssl);
  w("SSLCA", info.sslca);
  w("SSLCERT", info.sslcert);
  w("SSLKEY", info.sslkey);

  if (!success) {
    logMessage(nullptr, "WriteDsnToOdbcIni: Failed to write one or more DSN attributes",
               LOG_LEVEL_ERROR);
    return FALSE;
  }

  std::ostringstream oss;
  oss << "WriteDsnToOdbcIni: Written DSN '" << info.dsnName << "' with SERVER=" << info.server
      << ", PORT=" << info.port << ", UID=" << info.uid << ", DATABASE=" << info.database
      << ", ISTABLEMODEL=" << info.isTableModel << ", LOGLEVEL=" << info.logLevel
      << ", SESSIONTIMEOUTMS=" << info.sessionTimeoutMs << ", BATCHSIZE=" << info.batchSize;
  logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

  return TRUE;
}

// ─── Dialog context passed via lParam ───────────────────────────────────────

struct DialogContext {
  DsnInfo info;
  std::string driverName;
  BOOL isNewDsn;
};

// ─── Dialog Proc ────────────────────────────────────────────────────────────

static void UpdateDatabaseEnabled(HWND hDlg) {
  BOOL isTable = (IsDlgButtonChecked(hDlg, IDC_TABLE_MODEL) == BST_CHECKED);
  EnableWindow(GetDlgItem(hDlg, IDC_DATABASE), isTable);
}

static void SetDialogFields(HWND hDlg, const DsnInfo& info, BOOL isNew) {
  SetDlgItemTextA(hDlg, IDC_DSN_NAME, info.dsnName.c_str());
  SetDlgItemTextA(hDlg, IDC_DSN_DESCRIPTION, info.description.c_str());
  SetDlgItemTextA(hDlg, IDC_SERVER, info.server.c_str());
  SetDlgItemTextA(hDlg, IDC_PORT, info.port.c_str());
  SetDlgItemTextA(hDlg, IDC_UID, info.uid.c_str());
  SetDlgItemTextA(hDlg, IDC_PWD, info.pwd.c_str());
  SetDlgItemTextA(hDlg, IDC_DATABASE, info.database.c_str());
  CheckDlgButton(hDlg, IDC_TABLE_MODEL,
                 (info.isTableModel == "1" || info.isTableModel == "true") ? BST_CHECKED
                                                                           : BST_UNCHECKED);
  SetDlgItemTextA(hDlg, IDC_LOG_LEVEL, info.logLevel.c_str());
  SetDlgItemTextA(hDlg, IDC_SESSION_TIMEOUT, info.sessionTimeoutMs.c_str());
  SetDlgItemTextA(hDlg, IDC_BATCH_SIZE, info.batchSize.c_str());
  CheckDlgButton(hDlg, IDC_SSL,
                 (info.ssl == "1" || info.ssl == "true") ? BST_CHECKED : BST_UNCHECKED);
  SetDlgItemTextA(hDlg, IDC_SSLCA, info.sslca.c_str());
  SetDlgItemTextA(hDlg, IDC_SSLCERT, info.sslcert.c_str());
  SetDlgItemTextA(hDlg, IDC_SSLKEY, info.sslkey.c_str());

  EnableWindow(GetDlgItem(hDlg, IDC_DSN_NAME), isNew);
  UpdateDatabaseEnabled(hDlg);
}

static void GetDialogFields(HWND hDlg, DsnInfo& info) {
  auto getText = [&](int id) -> std::string {
    char buf[1024] = {};
    GetDlgItemTextA(hDlg, id, buf, sizeof(buf));
    return buf;
  };

  info.dsnName = getText(IDC_DSN_NAME);
  info.description = getText(IDC_DSN_DESCRIPTION);
  info.server = getText(IDC_SERVER);
  info.port = getText(IDC_PORT);
  info.uid = getText(IDC_UID);
  info.pwd = getText(IDC_PWD);
  info.database = getText(IDC_DATABASE);
  info.isTableModel = (IsDlgButtonChecked(hDlg, IDC_TABLE_MODEL) == BST_CHECKED) ? "1" : "0";
  info.logLevel = getText(IDC_LOG_LEVEL);
  info.sessionTimeoutMs = getText(IDC_SESSION_TIMEOUT);
  info.batchSize = getText(IDC_BATCH_SIZE);
  info.ssl = IsDlgButtonChecked(hDlg, IDC_SSL) == BST_CHECKED ? "1" : "0";
  info.sslca = getText(IDC_SSLCA);
  info.sslcert = getText(IDC_SSLCERT);
  info.sslkey = getText(IDC_SSLKEY);
}

static INT_PTR CALLBACK DSNDialogProc(HWND hDlg, UINT message, WPARAM wParam, LPARAM lParam) {
  DialogContext* ctx = reinterpret_cast<DialogContext*>(GetWindowLongPtrA(hDlg, GWLP_USERDATA));

  switch (message) {
  case WM_INITDIALOG: {
    ctx = reinterpret_cast<DialogContext*>(lParam);
    SetWindowLongPtrA(hDlg, GWLP_USERDATA, reinterpret_cast<LONG_PTR>(ctx));

    logMessage(nullptr,
               "DSNDialogProc: WM_INITDIALOG, DSN='" + ctx->info.dsnName +
                   "', isNew=" + (ctx->isNewDsn ? "true" : "false"),
               LOG_LEVEL_INFO);

    SetDialogFields(hDlg, ctx->info, ctx->isNewDsn);

    logMessage(nullptr, "DSNDialogProc: Dialog fields populated", LOG_LEVEL_DEBUG);
    return TRUE;
  }

  case WM_COMMAND:
    switch (LOWORD(wParam)) {
    case IDOK: {
      logMessage(nullptr, "DSNDialogProc: OK clicked", LOG_LEVEL_INFO);

      GetDialogFields(hDlg, ctx->info);

      if (ctx->info.dsnName.empty()) {
        logMessage(nullptr, "DSNDialogProc: DSN name is empty", LOG_LEVEL_WARN);
        MessageBoxA(hDlg, "DSN Name cannot be empty.", "Validation Error", MB_OK | MB_ICONWARNING);
        SetFocus(GetDlgItem(hDlg, IDC_DSN_NAME));
        return TRUE;
      }

      std::ostringstream oss;
      oss << "DSNDialogProc: Saving DSN '" << ctx->info.dsnName << "', driver='" << ctx->driverName
          << "'";
      logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

      if (!WriteDsnToOdbcIni(ctx->info, ctx->driverName)) {
        logMessage(nullptr, "DSNDialogProc: WriteDsnToOdbcIni failed", LOG_LEVEL_ERROR);
        MessageBoxA(hDlg, "Failed to save DSN configuration.", "Error", MB_OK | MB_ICONERROR);
        return TRUE;
      }

      logMessage(nullptr, "DSNDialogProc: DSN saved successfully", LOG_LEVEL_INFO);
      EndDialog(hDlg, IDOK);
      return TRUE;
    }

    case IDCANCEL:
      logMessage(nullptr, "DSNDialogProc: Cancel clicked", LOG_LEVEL_INFO);
      EndDialog(hDlg, IDCANCEL);
      return TRUE;

    case IDC_TABLE_MODEL:
      UpdateDatabaseEnabled(hDlg);
      return TRUE;

    case IDC_BTN_TEST: {
      logMessage(nullptr, "DSNDialogProc: Test Connection clicked", LOG_LEVEL_INFO);

      DsnInfo tmp;
      GetDialogFields(hDlg, tmp);

      bool isTable = (tmp.isTableModel == "1" || tmp.isTableModel == "true");

      std::ostringstream oss;
      oss << "DSNDialogProc: Test params - Server=" << tmp.server << ", Port=" << tmp.port
          << ", UID=" << tmp.uid << ", Database=" << tmp.database
          << ", TableModel=" << (isTable ? "true" : "false");
      logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

      if (isTable && tmp.database.empty()) {
        MessageBoxA(hDlg, "Table Model requires a database name.", "Test Connection",
                    MB_OK | MB_ICONWARNING);
        SetFocus(GetDlgItem(hDlg, IDC_DATABASE));
        return TRUE;
      }

      SetCursor(LoadCursor(nullptr, IDC_WAIT));

      try {
        std::string version;
        ConnectionHandle connection(nullptr);
        connection.serverHostName = tmp.server;
        connection.serverPort = tmp.port;
        connection.userName = tmp.uid;
        connection.password = tmp.pwd;
        connection.database = tmp.database;
        connection.isTableModel = isTable;
        SetConnectionHandle(&connection, "SSL", tmp.ssl);
        SetConnectionHandle(&connection, "SSLCA", tmp.sslca);
        SetConnectionHandle(&connection, "SSLCERT", tmp.sslcert);
        SetConnectionHandle(&connection, "SSLKEY", tmp.sslkey);
        connection.OpenSession();
        if (isTable) {
          auto session = connection.tableSessionPtr;
          auto dataSet = session->executeQueryStatement("SHOW VERSION");
          if (dataSet && dataSet->hasNext())
            version = dataSet->next()->toString();
          session->close();
        } else {
          auto session = connection.sessionPtr;
          auto dataSet = session->executeQueryStatement("SHOW VERSION");
          if (dataSet && dataSet->hasNext())
            version = dataSet->next()->toString();
          session->close();
        }

        SetCursor(LoadCursor(nullptr, IDC_ARROW));

        logMessage(nullptr, "DSNDialogProc: Test connection succeeded, version: " + version,
                   LOG_LEVEL_INFO);

        std::string msg = "Connection successful!";
        if (!version.empty())
          msg += "\n\nServer response:\n" + version;
        MessageBoxA(hDlg, msg.c_str(), "Test Connection", MB_OK | MB_ICONINFORMATION);
      } catch (const std::exception& e) {
        SetCursor(LoadCursor(nullptr, IDC_ARROW));

        std::string errMsg = e.what();
        logMessage(nullptr, "DSNDialogProc: Test connection failed: " + errMsg, LOG_LEVEL_ERROR);

        std::string msg = "Connection failed!\n\n" + errMsg;
        MessageBoxA(hDlg, msg.c_str(), "Test Connection", MB_OK | MB_ICONERROR);
      }
      return TRUE;
    }
    }
    break;

  case WM_CLOSE:
    logMessage(nullptr, "DSNDialogProc: WM_CLOSE", LOG_LEVEL_INFO);
    EndDialog(hDlg, IDCANCEL);
    return TRUE;
  }

  return FALSE;
}

// ─── Entry point called from ConfigDSN ──────────────────────────────────────

BOOL ShowDSNDialog(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszAttributes) {
  std::ostringstream oss;
  oss << "ShowDSNDialog: fRequest=" << fRequest
      << ", driver=" << (lpszDriver ? lpszDriver : "(null)")
      << ", hwndParent=" << (hwndParent ? "non-null" : "null");
  logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

  switch (fRequest) {
  case ODBC_ADD_DSN: {
    logMessage(nullptr, "ShowDSNDialog: ODBC_ADD_DSN", LOG_LEVEL_INFO);

    if (!hwndParent) {
      logMessage(nullptr, "ShowDSNDialog: No parent window for ADD, aborting", LOG_LEVEL_WARN);
      return FALSE;
    }

    DialogContext ctx;
    ctx.driverName = lpszDriver ? lpszDriver : "";
    ctx.isNewDsn = TRUE;
    ctx.info.server = "127.0.0.1";
    ctx.info.port = "6667";
    ctx.info.uid = "root";
    ctx.info.pwd = "root";
    ctx.info.database = "";
    ctx.info.isTableModel = "1";
    ctx.info.logLevel = "0";
    ctx.info.sessionTimeoutMs = "0";
    ctx.info.batchSize = "1000";

    HINSTANCE hInst = GetCurrentModuleHandle();
    logMessage(nullptr, "ShowDSNDialog: Showing ADD dialog", LOG_LEVEL_DEBUG);

    INT_PTR result = DialogBoxParamA(hInst, MAKEINTRESOURCEA(IDD_DSN_DIALOG), hwndParent,
                                     DSNDialogProc, reinterpret_cast<LPARAM>(&ctx));

    oss.str("");
    oss.clear();
    oss << "ShowDSNDialog: ADD dialog returned " << result;
    logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

    return (result == IDOK) ? TRUE : FALSE;
  }

  case ODBC_CONFIG_DSN: {
    std::string dsnName = ParseDSNFromAttributes(lpszAttributes);
    logMessage(nullptr, "ShowDSNDialog: ODBC_CONFIG_DSN, DSN='" + dsnName + "'", LOG_LEVEL_INFO);

    if (!hwndParent) {
      logMessage(nullptr, "ShowDSNDialog: No parent window for CONFIG, aborting", LOG_LEVEL_WARN);
      return FALSE;
    }

    DialogContext ctx;
    ctx.driverName = lpszDriver ? lpszDriver : "";
    ctx.isNewDsn = FALSE;

    if (!dsnName.empty()) {
      ReadDsnFromOdbcIni(dsnName, ctx.info);
    }

    HINSTANCE hInst = GetCurrentModuleHandle();
    logMessage(nullptr, "ShowDSNDialog: Showing CONFIG dialog", LOG_LEVEL_DEBUG);

    INT_PTR result = DialogBoxParamA(hInst, MAKEINTRESOURCEA(IDD_DSN_DIALOG), hwndParent,
                                     DSNDialogProc, reinterpret_cast<LPARAM>(&ctx));

    oss.str("");
    oss.clear();
    oss << "ShowDSNDialog: CONFIG dialog returned " << result;
    logMessage(nullptr, oss.str(), LOG_LEVEL_INFO);

    return (result == IDOK) ? TRUE : FALSE;
  }

  case ODBC_REMOVE_DSN: {
    std::string dsnName = ParseDSNFromAttributes(lpszAttributes);
    logMessage(nullptr, "ShowDSNDialog: ODBC_REMOVE_DSN, DSN='" + dsnName + "'", LOG_LEVEL_INFO);

    if (dsnName.empty()) {
      logMessage(nullptr, "ShowDSNDialog: DSN name empty, cannot remove", LOG_LEVEL_ERROR);
      return FALSE;
    }

    BOOL removed = SQLRemoveDSNFromIni(dsnName.c_str());
    logMessage(nullptr,
               "ShowDSNDialog: SQLRemoveDSNFromIni returned " +
                   std::string(removed ? "TRUE" : "FALSE"),
               LOG_LEVEL_INFO);

    return removed;
  }

  default:
    logMessage(nullptr, "ShowDSNDialog: Unknown fRequest=" + std::to_string(fRequest),
               LOG_LEVEL_ERROR);
    return FALSE;
  }
}

#endif // WIN32
