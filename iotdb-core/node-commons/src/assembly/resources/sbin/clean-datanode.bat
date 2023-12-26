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
echo %reCheck%
if not "%reCheck%" == "-f" (
    echo -n "Do you want to clean all the data in the IoTDB ? y/n (default n): "
    set /p CLEAN_SERVICE=
)

if not "%CLEAN_SERVICE%"=="y" if not "%CLEAN_SERVICE%"=="Y" (
  echo "Exiting..."
  exit 0
)

rmdir /s /q "%IOTDB_HOME%\data\datanode\"

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_system_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_system_dir=%%i
)
if "%dn_system_dir%"=="" (
    set "dn_system_dir=data\confignode\system"
)
set "dn_system_dir=%dn_system_dir:"=%"

setlocal enabledelayedexpansion
if "%dn_system_dir:~0,2%"=="\\" (
    rmdir /s /q "%dn_system_dir%"
) else if "%dn_system_dir:~1,3%"==":\\" (
    rmdir /s /q "%dn_system_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_system_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_data_dirs"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_data_dirs=%%i
)
if "%dn_data_dirs%"=="" (
    set "dn_data_dirs=data\\datanode\\data"
)
set "dn_data_dirs=%dn_data_dirs:"=%"

setlocal enabledelayedexpansion
if "%dn_data_dirs:~0,2%"=="\\" (
    rmdir /s /q "%dn_data_dirs%"
) else if "%dn_data_dirs:~1,3%"==":\\" (
    rmdir /s /q "%dn_data_dirs%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_data_dirs%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_consensus_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_consensus_dir=%%i
)
if "%dn_consensus_dir%"=="" (
    set "dn_consensus_dir=data\\datanode\\consensus"
)
set "dn_consensus_dir=%dn_consensus_dir:"=%"

setlocal enabledelayedexpansion
if "%dn_consensus_dir:~0,2%"=="\\" (
    rmdir /s /q "%dn_consensus_dir%"
) else if "%dn_consensus_dir:~1,3%"==":\\" (
    rmdir /s /q "%dn_consensus_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_consensus_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_wal_dirs"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_wal_dirs=%%i
)
if "%dn_wal_dirs%"=="" (
    set "dn_wal_dirs=data\\datanode\\wal"
)
set "dn_wal_dirs=%dn_wal_dirs:"=%"

setlocal enabledelayedexpansion
if "%dn_wal_dirs:~0,2%"=="\\" (
    rmdir /s /q "%dn_wal_dirs%"
) else if "%dn_wal_dirs:~1,3%"==":\\" (
    rmdir /s /q "%dn_wal_dirs%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_wal_dirs%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_tracing_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_tracing_dir=%%i
)
if "%dn_tracing_dir%"=="" (
    set "dn_tracing_dir=data\\datanode\\wal"
)
set "dn_tracing_dir=%dn_tracing_dir:"=%"

setlocal enabledelayedexpansion
if "%dn_tracing_dir:~0,2%"=="\\" (
    rmdir /s /q "%dn_tracing_dir%"
) else if "%dn_tracing_dir:~1,3%"==":\\" (
    rmdir /s /q "%dn_tracing_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_tracing_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_sync_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set dn_sync_dir=%%i
)
if "%dn_sync_dir%"=="" (
    set "dn_sync_dir=data\\datanode\\sync"
)
set "dn_sync_dir=%dn_sync_dir:"=%"

setlocal enabledelayedexpansion
if "%dn_sync_dir:~0,2%"=="\\" (
    rmdir /s /q "%dn_sync_dir%"
) else if "%dn_sync_dir:~1,3%"==":\\" (
    rmdir /s /q "%dn_sync_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%dn_sync_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^pipe_receiver_file_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set pipe_receiver_file_dir=%%i
)
if "%pipe_receiver_file_dir%"=="" (
    set "pipe_receiver_file_dir=data\\datanode\\system\\pipe\\receiver"
)
set "pipe_receiver_file_dir=%pipe_receiver_file_dir:"=%"

setlocal enabledelayedexpansion
if "%pipe_receiver_file_dir:~0,2%"=="\\" (
    rmdir /s /q "%pipe_receiver_file_dir%"
) else if "%pipe_receiver_file_dir:~1,3%"==":\\" (
    rmdir /s /q "%pipe_receiver_file_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%pipe_receiver_file_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^sort_tmp_dir"
  %IOTDB_HOME%\conf\iotdb-datanode.properties') do (
  set sort_tmp_dir=%%i
)
if "%sort_tmp_dir%"=="" (
    set "sort_tmp_dir=data\\datanode\\tmp"
)
set "sort_tmp_dir=%sort_tmp_dir:"=%"

setlocal enabledelayedexpansion
if "%sort_tmp_dir:~0,2%"=="\\" (
    rmdir /s /q "%sort_tmp_dir%"
) else if "%sort_tmp_dir:~1,3%"==":\\" (
    rmdir /s /q "%sort_tmp_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%sort_tmp_dir%"
)

echo "DataNode clean done ..."