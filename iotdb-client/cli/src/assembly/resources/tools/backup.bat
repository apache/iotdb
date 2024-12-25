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
setlocal enabledelayedexpansion

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%CD%
popd

if NOT DEFINED JAVA_HOME goto :err

set JAVA_OPTS=-ea^
 -DIOTDB_HOME="%IOTDB_HOME%"

SET IOTDB_CONF=%IOTDB_HOME%\conf
IF EXIST "%IOTDB_CONF%\datanode-env.bat" (
  CALL "%IOTDB_CONF%\datanode-env.bat" > nul 2>&1
) ELSE (
  echo Can't find datanode-env.bat
)

IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_rpc_port"
    "%IOTDB_CONF%\iotdb-system.properties"') do (
      set dn_rpc_port=%%i
  )
) ELSE IF EXIST "%IOTDB_CONF%\iotdb-datanode.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_rpc_port"
    "%IOTDB_CONF%\iotdb-datanode.properties"') do (
      set dn_rpc_port=%%i
  )
) ELSE (
  set dn_rpc_port=6667
)

IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_internal_port"
    "%IOTDB_CONF%\iotdb-system.properties"') do (
      set cn_internal_port=%%i
  )
) ELSE IF EXIST "%IOTDB_CONF%\iotdb-confignode.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_internal_port"
    "%IOTDB_CONF%\iotdb-confignode.properties"') do (
      set cn_internal_port=%%i
  )
) ELSE (
  set cn_internal_port=10710
)

set "local_iotdb_occupied_ports="
set "operation_dirs="
set dn_rpc_port_occupied=0
set cn_internal_port_occupied=0

for /f  "tokens=1,3,7 delims=: " %%i in ('netstat /ano') do (
    if %%i==TCP (
       if %%j==%dn_rpc_port% (
         if !dn_rpc_port_occupied!==0 (
           set spid=%%k
           call :checkIfIOTDBProcess !spid! is_iotdb
           if !is_iotdb!==1 (
             set local_iotdb_occupied_ports=%dn_rpc_port% !local_iotdb_occupied_ports!
           )
         )

       ) else if %%j==%cn_internal_port% (
         if !cn_internal_port_occupied!==0 (
             set spid=%%k
             call :checkIfIOTDBProcess !spid! is_iotdb
             if !is_iotdb!==1 (
              set local_iotdb_occupied_ports=%cn_internal_port% !local_iotdb_occupied_ports!
             )
         )
       )
    )
)

if defined local_iotdb_occupied_ports (
     goto :checkFail
)
echo ------------------------------------------
echo Starting IoTDB Client Data Back Script
echo ------------------------------------------

set CLASSPATH="%IOTDB_HOME%\lib\*"
if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.tool.backup.IoTDBDataBackTool

set logsDir="%IOTDB_HOME%\logs"
if not exist "%logsDir%" (
    mkdir "%logsDir%"
)

set IOTDB_CLI_CONF=%IOTDB_HOME%\conf
set "iotdb_cli_params=-Dlogback.configurationFile=!IOTDB_CLI_CONF!\logback-backup.xml"
start /B "" cmd /C "("%JAVA_HOME%\bin\java" -DIOTDB_HOME=!IOTDB_HOME! !iotdb_cli_params! !JAVA_OPTS! -cp !CLASSPATH! !MAIN_CLASS! %*) > nul 2>&1"
exit /b

:checkIfIOTDBProcess
setlocal

set "pid_to_check=%~1"
set "is_iotdb=0"

for /f "usebackq tokens=*" %%i in (`wmic process where "ProcessId=%pid_to_check%" get CommandLine /format:list ^| findstr /c:"CommandLine="`) do (
    set command_line=%%i
)
echo %command_line% | findstr /i /c:"iotdb" >nul && set is_iotdb=1
endlocal & set "is_iotdb=%is_iotdb%"
exit /b

:err
echo JAVA_HOME environment variable must be set!
set ret_code=1
exit /b

:checkFail
echo Please stop IoTDB
exit /b