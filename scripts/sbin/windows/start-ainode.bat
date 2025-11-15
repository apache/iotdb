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

echo ```````````````````````````
echo Starting IoTDB AINode
echo ```````````````````````````

pushd %~dp0..\..
if NOT DEFINED IOTDB_AINODE_HOME set IOTDB_AINODE_HOME=%cd%

set ain_ainode_executable=%IOTDB_AINODE_HOME%\lib\ainode

echo Script got ainode executable: %ain_ainode_executable%

set daemon_mode=false
:parse_args
if "%~1"=="" goto end_parse
if /i "%~1"=="-d" set daemon_mode=true
shift
goto parse_args
:end_parse

if "%daemon_mode%"=="true" (
  echo Starting AINode in daemon mode...
  start /B "" %ain_ainode_executable% start
  echo AINode started in background
) else (
  echo Starting AINode...
  %ain_ainode_executable% start
  pause
)