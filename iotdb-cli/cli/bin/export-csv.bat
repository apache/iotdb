@REM
@REM Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
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
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM

@echo off

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%CD%
popd

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.db.tool.ExportCsv
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIOTDB_HOME=%IOTDB_HOME%

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH=""

REM For each jar in the IOTDB_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%IOTDB_HOME%\lib\*.jar") do call :append "%%i"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath

"%JAVA_HOME%\bin\java" -DIOTDB_HOME=%IOTDB_HOME% %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %*

goto finally


:err
echo JAVA_HOME environment variable must be set!
pause

@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL