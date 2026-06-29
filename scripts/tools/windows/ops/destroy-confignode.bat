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
pushd %~dp0..\..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

set "reCheck=%1"
if not "%reCheck%" == "-f" (
    echo "Do you want to clean the data of confignode in the IoTDB ? y/n (default n): "
    set /p CLEAN_SERVICE=
)

if not "%CLEAN_SERVICE%"=="y" if not "%CLEAN_SERVICE%"=="Y" (
  echo "Exiting..."
  goto finally
)

start cmd /c "%IOTDB_HOME%\\sbin\\windows\\stop-confignode.bat -f"
timeout /t 3 > nul
rmdir /s /q "%IOTDB_HOME%\data\confignode\" 2>nul
if exist "%IOTDB_HOME%\conf\iotdb-system.properties" (
  set IOTDB_CONFIGNODE_CONFIG="%IOTDB_HOME%\conf\iotdb-system.properties"
) ELSE (
  set IOTDB_CONFIGNODE_CONFIG="%IOTDB_HOME%\conf\iotdb-confignode.properties"
)
set "delimiter=,;"
for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_system_dir"
  %IOTDB_CONFIGNODE_CONFIG%') do (
  set cn_system_dir=%%i
)
if "%cn_system_dir%"=="" (
  set "cn_system_dir=data\confignode\system"
)

setlocal enabledelayedexpansion
set "cn_system_dir=!cn_system_dir:%delimiter%= !"
for %%i in (%cn_system_dir%) do (
  set "var=%%i"
  if "!var:~0,2!"=="\\" (
     rmdir /s /q "%%i" 2>nul
  ) else if "!var:~1,3!"==":\\" (
     rmdir /s /q "%%i" 2>nul
  ) else (
     rmdir /s /q "%IOTDB_HOME%\%%i" 2>nul
  )
)

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_consensus_dir"
  %IOTDB_CONFIGNODE_CONFIG%') do (
  set cn_consensus_dir=%%i
)
if "%cn_consensus_dir%"=="" (
set "cn_consensus_dir=data\confignode\consensus"
)

set "cn_consensus_dir=!cn_consensus_dir:%delimiter%= !"
for %%i in (%cn_consensus_dir%) do (
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

echo "ConfigNode clean done ..."

:finally
exit /b