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

wmic process where (commandline like "%%iotdb.IoTDB%%" and not name="wmic.exe") delete
rem ps ax | grep -i 'iotdb.IoTDB' | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
