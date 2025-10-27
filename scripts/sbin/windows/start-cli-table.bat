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

@echo off

REM -------------------------------
REM Path to start-cli.bat script (adjust if needed)
set CLI_SCRIPT=%~dp0start-cli.bat

REM Verify start-cli.bat script exists
if not exist "%CLI_SCRIPT%" (
    echo Error: start-cli.bat script not found at %CLI_SCRIPT%
    pause
    exit /b 1
)

REM Execute main script with:
REM Pass all arguments through and add -sql_dialect table if not already specified
setlocal
"%CLI_SCRIPT%" -sql_dialect table %*
endlocal