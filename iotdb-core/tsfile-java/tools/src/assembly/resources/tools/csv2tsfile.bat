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
if NOT DEFINED TSFILE_HOME set TSFILE_HOME=%CD%
popd

set JAVA_OPTS=-ea^
 -DTSFILE_HOME="%TSFILE_HOME%"

if NOT DEFINED JAVA_HOME goto :err

echo ------------------------------------------
echo Starting Csv to TsFile Script
echo ------------------------------------------

set CLASSPATH="%TSFILE_HOME%\lib\*"
if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.tsfile.tools.TsFileTool

set TSFILE_CONF=%TSFILE_HOME%\conf
set "tsfile_params=-Dlogback.configurationFile=!TSFILE_CONF!\logback-cvs2tsfile.xml"
start /B /WAIT "" cmd /C "("%JAVA_HOME%\bin\java" -DTSFILE_HOME=!TSFILE_HOME! !tsfile_params! !JAVA_OPTS! -cp !CLASSPATH! !MAIN_CLASS! %*)"
exit /b

:err
echo JAVA_HOME environment variable must be set!
set ret_code=1
exit /b