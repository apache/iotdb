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

set current_dir=%~dp0
set superior_dir=%current_dir%\..\

IF EXIST "%superior_dir%\conf\iotdb-system.properties" (
  set config_file="%superior_dir%\conf\iotdb-system.properties"
) ELSE (
  IF EXIST "%superior_dir%\conf\iotdb-confignode.properties" (
    set config_file="%superior_dir%\conf\iotdb-confignode.properties"
  ) ELSE (
    echo "No configuration file found. Exiting."
    exit /b 1
  )
)

for /f  "eol=; tokens=2,2 delims==" %%i in ('findstr /i "^cn_internal_port"
"%config_file%"') do (
  set cn_internal_port=%%i
)

if not defined cn_internal_port (
  echo "WARNING: cn_internal_port not found in the configuration file. Using default value cn_internal_port = 10710"
  set cn_internal_port=10710
)

echo "check whether the cn_internal_port is used..., port is %cn_internal_port%"

for /f  "eol=; tokens=2,2 delims==" %%i in ('findstr /i "cn_internal_address"
"%config_file%"') do (
  set cn_internal_address=%%i
)

if not defined cn_internal_address (
  echo "WARNING: cn_internal_address not found in the configuration file. Using default value cn_internal_address = 127.0.0.1"
  set cn_internal_address=127.0.0.1
)

for /f "tokens=5" %%a in ('netstat /ano ^| findstr %cn_internal_address%:%cn_internal_port% ^| findstr LISTENING ') do (
  taskkill /f /pid %%a
    echo "close ConfigNode, PID:" %%a
)
