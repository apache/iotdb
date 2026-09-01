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

setlocal
echo Stopping @brand.name@ Edge (the merged ConfigNode + DataNode process)
pushd "%~dp0\..\.."
set "IOTDB_HOME=%cd%"
popd
@REM The main class can be overridden (same convention as start-edge.bat), keep them in sync.
if NOT DEFINED EDGE_MAIN_CLASS set "EDGE_MAIN_CLASS=com.timecho.iotdb.edge.EdgeNode"
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; $plain='-DIOTDB_HOME=' + $env:IOTDB_HOME; $quoted='-DIOTDB_HOME=' + [char]34 + $env:IOTDB_HOME + [char]34; $mainClass=$env:EDGE_MAIN_CLASS; Get-CimInstance Win32_Process -Filter \"name='java.exe'\" | Where-Object { $line=$_.CommandLine; $sameHome=$line -and ($line.Contains($plain + ' ') -or $line.EndsWith($plain) -or $line.Contains($quoted + ' ') -or $line.EndsWith($quoted)); $sameHome -and $line.Contains($mainClass) } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('@brand.name@ Edge process ' + $_.ProcessId + ' stopped.') }"
set "STOP_STATUS=%errorlevel%"
if not "%~1"=="-f" pause
exit /b %STOP_STATUS%
