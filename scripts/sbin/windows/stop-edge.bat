@echo off
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

echo Stopping IoTDB Edge (the merged ConfigNode + DataNode process)
pushd %~dp0\..\..
set "IOTDB_HOME=%cd%"
popd
powershell -NoProfile -Command "$plain='-DIOTDB_HOME=' + $env:IOTDB_HOME; $quoted='-DIOTDB_HOME=' + [char]34 + $env:IOTDB_HOME + [char]34; Get-CimInstance Win32_Process -Filter \"name='java.exe'\" | Where-Object { $line=$_.CommandLine; $sameHome=$line -and ($line.Contains($plain + ' ') -or $line.EndsWith($plain) -or $line.Contains($quoted + ' ') -or $line.EndsWith($quoted)); $sameHome -and $line.Contains('org.apache.iotdb.edge.EdgeNode') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('IoTDB Edge process ' + $_.ProcessId + ' stopped.') }"
pause
