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
set THRIFT_EXE=D:\software\Thrift\thrift-0.13.0.exe
set BAT_DIR=%~dp0
set THRIFT_SCRIPT=%BAT_DIR%..\thrift\src\main\thrift\rpc.thrift
set THRIFT_OUT=%BAT_DIR%target\iotdb

rmdir /Q /S %THRIFT_OUT%
mkdir -p %THRIFT_OUT%

%THRIFT_EXE% -gen py -out %THRIFT_OUT%  %THRIFT_SCRIPT%
