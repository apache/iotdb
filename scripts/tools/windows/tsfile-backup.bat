@echo off
REM Licensed to the Apache Software Foundation (ASF) under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  The ASF licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing,
REM software distributed under the License is distributed on an
REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM KIND, either express or implied.  See the License for the
REM specific language governing permissions and limitations
REM under the License.

@echo off
title TsFile Backup Tool

if "%OS%" == "Windows_NT" setlocal

@REM -----------------------------------------------------------------------------
@REM 1. Dynamically infer tool root and IOTDB_HOME (Smart Fallback)
@REM -----------------------------------------------------------------------------
pushd "%~dp0..\.." >nul
set "TOOL_ROOT=%CD%"

@REM Fallback: If IOTDB_HOME is not explicitly set by the user, default to the current directory.
if NOT DEFINED IOTDB_HOME set "IOTDB_HOME=%CD%"
popd >nul

@REM -----------------------------------------------------------------------------
@REM 2. Strict Environment Validation
@REM -----------------------------------------------------------------------------
if NOT DEFINED JAVA_HOME goto :err_java

set "PLUGIN_JAR="

if NOT DEFINED PLUGIN_JAR for %%f in ("%TOOL_ROOT%\ext\pipe\tsfile-remote-sink-*-jar-with-dependencies.jar") do (
    if EXIST "%%~f" set "PLUGIN_JAR=%%~f"
)

@REM -----------------------------------------------------------------------------
@REM 3. JVM Options and Dependency Assembly
@REM -----------------------------------------------------------------------------
if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.tool.pipe.TsFileBackup

@REM Centralized system properties and encoding settings
set "JAVA_OPTS=-ea -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 -DIOTDB_HOME="%IOTDB_HOME%""
if DEFINED PLUGIN_JAR if EXIST "%PLUGIN_JAR%" set "JAVA_OPTS=%JAVA_OPTS% -Dtsfile.backup.plugin.jar="%PLUGIN_JAR%""

@REM Elegant dependency loading using wildcards (*) to avoid string concatenation hell
set "CLASSPATH=%TOOL_ROOT%\lib\*;%IOTDB_HOME%\lib\*"

@REM -----------------------------------------------------------------------------
@REM 4. Core Execution and Exit Code Propagation
@REM -----------------------------------------------------------------------------
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %*

@REM Accurately capture the exit status code of the Java process
set ret_code=%ERRORLEVEL%
goto finally


@REM -----------------------------------------------------------------------------
@REM Error Handling Block
@REM -----------------------------------------------------------------------------
:err_java
echo [ERROR] JAVA_HOME environment variable must be set!
set ret_code=1
pause
goto finally


@REM -----------------------------------------------------------------------------
@REM Cleanup and Exit
@REM -----------------------------------------------------------------------------
:finally
if "%OS%" == "Windows_NT" endlocal
EXIT /B %ret_code%
