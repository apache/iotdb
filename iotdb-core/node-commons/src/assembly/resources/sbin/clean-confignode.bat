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
    echo -n "Do you want to clean all the data in the IoTDB ? y/n (default n): "
    set /p CLEAN_SERVICE=
)

if not "%CLEAN_SERVICE%"=="y" if not "%CLEAN_SERVICE%"=="Y" (
  echo "Exiting..."
  exit 0
)

rmdir /s /q "%IOTDB_HOME%\data\confignode\"
set IOTDB_CONFIGNODE_CONFIG=%IOTDB_HOME%\conf\iotdb-confignode.properties
for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_system_dir"
  %IOTDB_CONFIGNODE_CONFIG%') do (
  set cn_system_dir=%%i
)

if "%cn_system_dir%"=="" (
    set "cn_system_dir=data\confignode\system"
)
set "cn_system_dir=%cn_system_dir:"=%"
setlocal enabledelayedexpansion

if "%cn_system_dir:~0,2%"=="\\" (
    rmdir /s /q "%cn_system_dir%"
) else if "%cn_system_dir:~1,3%"==":\\" (
    rmdir /s /q "%cn_system_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\%cn_system_dir%"
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_consensus_dir"
  %IOTDB_CONFIGNODE_CONFIG%') do (
  set cn_consensus_dir=%%i
)
echo %cn_consensus_dir%
  if "%cn_consensus_dir%"=="" (
    set "cn_consensus_dir=data\confignode\consensus"
  )
  echo clean %cn_consensus_dir%
set "cn_consensus_dir=%cn_consensus_dir:"=%"
setlocal enabledelayedexpansion

if "%cn_consensus_dir:~0,2%"=="\\" (
    rmdir /s /q "%cn_consensus_dir%"
) else if "%cn_consensus_dir:~1,3%"==":\\" (
    rmdir /s /q "%cn_consensus_dir%"
) else (
    rmdir /s /q "%IOTDB_HOME%\\%cn_consensus_dir%"
)

endlocal

echo "ConfigNode clean done ..."