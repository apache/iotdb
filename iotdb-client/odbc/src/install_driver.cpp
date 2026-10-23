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
#include <cstdio>
#include <cstring>
#include <string>

#ifdef _WIN32
#include <windows.h>
#endif

#include <odbcinst.h>

static const char* DRIVER_NAME = "Apache IoTDB ODBC Driver";
static const char* DRIVER_DESCRIPTION = "Apache IoTDB ODBC Driver";

static void PrintInstallerError() {
  DWORD errCode = 0;
  char errMsg[512] = {};
  SQLInstallerError(1, &errCode, errMsg, sizeof(errMsg), nullptr);
  fprintf(stderr, "  ODBC Installer error [%lu]: %s\n", errCode, errMsg);
}

static int InstallDriver(const char* dllPath) {
  DWORD usageCount = 0;

#ifdef _WIN32
  const DWORD attributes = GetFileAttributesA(dllPath);
  if (attributes == INVALID_FILE_ATTRIBUTES || (attributes & FILE_ATTRIBUTE_DIRECTORY)) {
    fprintf(stderr, "Driver DLL does not exist: %s\n", dllPath);
    return 1;
  }
#endif

  printf("Removing previous registration of '%s' (if any)...\n", DRIVER_NAME);
  SQLRemoveDriver(DRIVER_NAME, FALSE, &usageCount);

  std::string driverDir;
  std::string fullPath(dllPath);
#ifdef _WIN32
  size_t lastSep = fullPath.find_last_of("\\/");
#else
  size_t lastSep = fullPath.find_last_of('/');
#endif
  if (lastSep != std::string::npos)
    driverDir = fullPath.substr(0, lastSep);
  else
    driverDir = ".";

  std::string driverDescr;
  auto appendEntry = [&](const std::string& entry) {
    driverDescr.append(entry);
    driverDescr.push_back('\0');
  };
  appendEntry(DRIVER_NAME);
  appendEntry("Driver=" + fullPath);
  appendEntry("Setup=" + fullPath);
  appendEntry(std::string("Description=") + DRIVER_DESCRIPTION);
  appendEntry("DriverODBCVer=03.80");
  appendEntry("ConnectFunctions=YYN");
  appendEntry("APILevel=1");
  appendEntry("SQLLevel=1");
  appendEntry("FileUsage=0");
  driverDescr.push_back('\0');

  char outPath[512] = {};
  printf("Installing driver '%s'\n", DRIVER_NAME);
  printf("  DLL: %s\n", dllPath);
  printf("  Dir: %s\n", driverDir.c_str());

  if (!SQLInstallDriverEx(driverDescr.c_str(), driverDir.c_str(), outPath, sizeof(outPath), nullptr,
                          ODBC_INSTALL_COMPLETE, nullptr)) {
    fprintf(stderr, "Failed to install driver.\n");
    PrintInstallerError();
    return 1;
  }

  printf("Driver installed successfully.\n");
  printf("  Registered at: %s\n", outPath);
  return 0;
}

static int UninstallDriver(bool removeDSNs) {
  DWORD usageCount = 0;

  printf("Removing driver '%s'%s...\n", DRIVER_NAME, removeDSNs ? " and associated DSNs" : "");

  if (!SQLRemoveDriver(DRIVER_NAME, removeDSNs ? TRUE : FALSE, &usageCount)) {
    fprintf(stderr, "Failed to remove driver.\n");
    PrintInstallerError();
    return 1;
  }

  printf("Driver removed successfully. Remaining usage count: %lu\n", usageCount);
  if (usageCount > 0)
    printf("  Note: Driver entry still exists (usage count > 0).\n");

  return 0;
}

static void PrintUsage(const char* progName) {
  printf("Apache IoTDB ODBC Driver Installer\n\n");
  printf("Usage:\n");
  printf("  %s install <path_to_dll>\n", progName);
  printf("    Register the ODBC driver using the specified DLL path.\n\n");
  printf("  %s uninstall\n", progName);
  printf("    Unregister the ODBC driver.\n\n");
  printf("  %s uninstall --remove-dsn\n", progName);
  printf("    Unregister the driver and remove all associated DSNs.\n\n");
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintUsage(argv[0]);
    return 1;
  }

  std::string action(argv[1]);

  if (action == "install") {
    if (argc < 3) {
      fprintf(stderr, "Error: DLL path is required for install.\n\n");
      PrintUsage(argv[0]);
      return 1;
    }
    return InstallDriver(argv[2]);
  } else if (action == "uninstall") {
    bool removeDSNs = false;
    if (argc >= 3 && strcmp(argv[2], "--remove-dsn") == 0)
      removeDSNs = true;
    return UninstallDriver(removeDSNs);
  } else {
    fprintf(stderr, "Error: Unknown action '%s'.\n\n", argv[1]);
    PrintUsage(argv[0]);
    return 1;
  }
}
