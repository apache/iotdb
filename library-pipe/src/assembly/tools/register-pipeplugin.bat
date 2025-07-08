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
setlocal enabledelayedexpansion

REM ======================== Configuration ========================
set host=127.0.0.1
set rpcPort=6667
set user=root
set pass=root

REM ===================== Resolve JAR Path ========================
set jar_abs_path=
if not "%~1"=="" (
  set jar_abs_path=%~1
)
if "%jar_abs_path%" == "" (
  for %%I in ("..\ext\pipe\library-pipe.jar") do set jar_abs_path=%%~fI
  echo [INFO] No jar path provided, using default: !jar_abs_path!
)

REM ======================= Convert to URI ========================
if "!jar_abs_path:~0,8!"! == "https://" (
    set uri_jar_path=!jar_abs_path!
) else (
    set uri_jar_path=!jar_abs_path:\=/!
    set uri_jar_path=file:/!uri_jar_path!
)

echo [INFO] Using jar URI: !uri_jar_path!

REM ================= Register Pipe Plugin ========================
call ..\sbin\windows\start-cli.bat -h %host% -p %rpcPort% -u %user% -pw %pass% -e "create pipeplugin if not exists MQTTExtractor as 'org.apache.iotdb.libpipe.extractor.mqtt.MQTTExtractor' USING URI '!uri_jar_path!'"

endlocal
