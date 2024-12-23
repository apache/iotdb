@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM
@echo off
pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

set "reCheck=%1"
if not "%reCheck%" == "-f" (
    echo "Do you want to clean the data of datanode in the IoTDB ? y/n (default n): "
    set /p CLEAN_SERVICE=
)

if not "%CLEAN_SERVICE%"=="y" if not "%CLEAN_SERVICE%"=="Y" (
  echo "Exiting..."
  goto finally
)
start cmd /c "%IOTDB_HOME%\\sbin\\stop-datanode.bat -f"
timeout /t 3 > nul
rmdir /s /q "%IOTDB_HOME%\data\datanode\" 2>nul
if exist "%IOTDB_HOME%\conf\iotdb-system.properties" (
  set IOTDB_DATANODE_CONFIG="%IOTDB_HOME%\conf\iotdb-system.properties"
) ELSE (
  set IOTDB_DATANODE_CONFIG="%IOTDB_HOME%\conf\iotdb-datanode.properties"
)
set "delimiter=,;"
for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_system_dir"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_system_dir=%%i
)
if "%dn_system_dir%"=="" (
    set "dn_system_dir=data\\datanode\\system"
)
setlocal enabledelayedexpansion
set "dn_system_dir=!dn_system_dir:%delimiter%= !"
for %%i in (%dn_system_dir%) do (
  set "var=%%i"
  if "!var:~0,2!"=="\\" (
    rmdir /s /q "%%i" 2>nul
  ) else if "!var:~1,3!"==":\\" (
    rmdir /s /q "%%i" 2>nul
  ) else (
    rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
  )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_data_dirs"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_data_dirs=%%i
)
if "%dn_data_dirs%"=="" (
    set "dn_data_dirs=data\\datanode\\data"
)

set "dn_data_dirs=!dn_data_dirs:%delimiter%= !"
for %%i in (%dn_data_dirs%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_consensus_dir"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_consensus_dir=%%i
)
if "%dn_consensus_dir%"=="" (
    set "dn_consensus_dir=data\\datanode\\consensus"
)

set "dn_consensus_dir=!dn_consensus_dir:%delimiter%= !"
for %%i in (%dn_consensus_dir%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_wal_dirs"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_wal_dirs=%%i
)
if "%dn_wal_dirs%"=="" (
    set "dn_wal_dirs=data\\datanode\\wal"
)

set "dn_wal_dirs=!dn_wal_dirs:%delimiter%= !"
for %%i in (%dn_wal_dirs%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_tracing_dir"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_tracing_dir=%%i
)
if "%dn_tracing_dir%"=="" (
    set "dn_tracing_dir=data\\datanode\\wal"
)
set "dn_tracing_dir=%dn_tracing_dir:"=%"

set "dn_tracing_dir=!dn_tracing_dir:%delimiter%= !"
for %%i in (%dn_tracing_dir%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_sync_dir"
  %IOTDB_DATANODE_CONFIG%') do (
  set dn_sync_dir=%%i
)
if "%dn_sync_dir%"=="" (
    set "dn_sync_dir=data\\datanode\\sync"
)
set "dn_sync_dir=%dn_sync_dir:"=%"

set "dn_sync_dir=!dn_sync_dir:%delimiter%= !"
for %%i in (%dn_sync_dir%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^pipe_receiver_file_dirs"
  %IOTDB_DATANODE_CONFIG%') do (
  set pipe_receiver_file_dirs=%%i
)
if "%pipe_receiver_file_dirs%"=="" (
    set "pipe_receiver_file_dirs=data\\datanode\\system\\pipe\\receiver"
)

set "pipe_receiver_file_dirs=!pipe_receiver_file_dirs:%delimiter%= !"
for %%i in (%pipe_receiver_file_dirs%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^iot_consensus_v2_receiver_file_dirs"
  %IOTDB_DATANODE_CONFIG%') do (
  set iot_consensus_v2_receiver_file_dirs=%%i
)
if "%iot_consensus_v2_receiver_file_dirs%"=="" (
    set "iot_consensus_v2_receiver_file_dirs=data\\datanode\\system\\pipe\\consensus\\receiver"
)

set "iot_consensus_v2_receiver_file_dirs=!iot_consensus_v2_receiver_file_dirs:%delimiter%= !"
for %%i in (%iot_consensus_v2_receiver_file_dirs%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^sort_tmp_dir"
  %IOTDB_DATANODE_CONFIG%') do (
  set sort_tmp_dir=%%i
)
if "%sort_tmp_dir%"=="" (
    set "sort_tmp_dir=data\\datanode\\tmp"
)

set "sort_tmp_dir=!sort_tmp_dir:%delimiter%= !"
for %%i in (%sort_tmp_dir%) do (
  set "var=%%i"
    if "!var:~0,2!"=="\\" (
      rmdir /s /q "%%i" 2>nul
    ) else if "!var:~1,3!"==":\\" (
      rmdir /s /q "%%i" 2>nul
    ) else (
      rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
    )
)

endlocal
echo "DataNode clean done ..."

:finally
exit /b