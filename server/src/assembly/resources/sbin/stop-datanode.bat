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

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_rpc_port"
%superior_dir%\conf\iotdb-datanode.properties') do (
  set dn_rpc_port=%%i
)

echo Check whether the rpc_port is used..., port is %dn_rpc_port%

for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "dn_rpc_address"
%superior_dir%\conf\iotdb-datanode.properties') do (
  set dn_rpc_address=%%i
)

for /f "tokens=5" %%a in ('netstat /ano ^| findstr %dn_rpc_address%:%dn_rpc_port%') do (
  taskkill /f /pid %%a
  echo Close DataNode, PID: %%a
)
rem ps ax | grep -i 'iotdb.DataNode' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
