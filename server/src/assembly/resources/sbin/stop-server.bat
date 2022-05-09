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

for /f  "eol=; tokens=2,2 delims==" %%i in ('findstr /i "^[\s]*rpc_port"
%superior_dir%\conf\iotdb-engine.properties') do (
  set rpc_port=%%i
)

for /f  "eol=; tokens=2,2 delims==" %%i in ('findstr /i "^[\s]*rpc_address"
%superior_dir%\conf\iotdb-engine.properties') do (
  set rpc_address=%%i
)

rem Try to gracefully stop server, at first.

set pid=''
call:findPid
if not %pid% == '' (
  if "%PROCESSOR_ARCHITECTURE%"=="x86" (
    start %current_dir%\win32-kill.exe -2 %pid% >nul
  ) else (
    start %current_dir%\win64-kill.exe -2 %pid% >nul
  )
) else (
  echo No IoTDB server to stop
  exit /b 1
)

echo|set /p="Begin to stop IoTDB ..."

set oldPid=%pid%
set i=0
:continue
  call:findPid
  if %pid% == '' (
    echo. closed gracefully.
    exit /b 0
  )
  echo|set /p="."
  choice /t 1 /c q  /d q /n >nul
  set /a i+=1
if %i% lss 10 goto continue

rem Force to shutdown server.
taskkill /f /pid %oldPid% >nul
echo. forced to kill.
exit /b 0


:findPid
  set pid=''
  for /f "tokens=5" %%a in ('netstat /ano ^| findstr %rpc_address%:%rpc_port%') do (
    set pid=%%a
  )
goto:eof