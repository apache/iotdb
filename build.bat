@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ----------------------------------------------------------------------------

@echo off

@REM Check if Maven is installed on the machine
where mvn >nul 2>nul
if %errorlevel% equ 0 (
    set "MVN_COMMAND=mvn"
) else (
    set "MVN_COMMAND=mvnw.bat"
)

@REM Check if JDK version is 21 or above
for /f "delims=. tokens=2" %%A in ('javac -version 2^>^&1') do (
    set /a "JDK_VERSION=%%A"
)
if %JDK_VERSION% geq 21 (
    set "SPOTLESS_OPTION=-Dspotless.skip=true"
) else (
    set "SPOTLESS_OPTION="
)

@REM Build distribution using Maven 
%MVN_COMMAND% clean package -pl distribution -am -DskipTests %SPOTLESS_OPTION%
