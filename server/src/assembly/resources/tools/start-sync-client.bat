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

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%CD%
popd

set IOTDB_CONF=%IOTDB_HOME%\conf
set IOTDB_LOGS=%IOTDB_HOME%\logs


if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.db.sync.sender.transfer.SyncClient
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%IOTDB_CONF%\logback.xml"^
 -DIOTDB_HOME=%IOTDB_HOME%^
 -DTSFILE_HOME=%IOTDB_HOME%^
 -DIOTDB_CONF=%IOTDB_CONF%

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%IOTDB_HOME%\lib\*"

goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

@rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %IOTDB_HEAP_OPTS% -cp %CLASSPATH% %IOTDB_JMX_OPTS% %MAIN_CLASS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
