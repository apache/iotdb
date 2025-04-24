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

echo "Do you want to clean the data in the IoTDB ? y/n (default n): "
set /p CLEAN_SERVICE=

if not "%CLEAN_SERVICE%"=="y" if not "%CLEAN_SERVICE%"=="Y" (
  echo "Exiting..."
  goto finally
)

pushd %~dp0..\..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

start cmd /c "%IOTDB_HOME%\\sbin\\windows\\stop-standalone.bat -f"
timeout /t 5 > nul
rmdir /s /q "%IOTDB_HOME%\\data\\"

start cmd /c "%IOTDB_HOME%\\tools\\windows\\ops\\destroy-datanode.bat -f"
start cmd /c "%IOTDB_HOME%\\tools\\windows\\ops\\destroy-confignode.bat -f"

ECHO "Cluster cleanup complete ..."
:finally
exit /b