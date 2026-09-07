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

@REM set cmd format
powershell -NoProfile -Command "$v=(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion').CurrentMajorVersionNumber; if($v -gt 6) { cmd /c 'chcp 65001' }"

title IoTDB Edge

echo ````````````````````````
echo Starting IoTDB Edge (ConfigNode + DataNode in one process)
echo ````````````````````````

@REM -----------------------------------------------------------------------------
@REM SET JAVA
if DEFINED JAVA_HOME set "PATH=%JAVA_HOME%\bin;%PATH%"
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="

for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

@REM IoTDB requires JDK 17 or later.
IF "%JAVA_VERSION%" == "" (
	echo Failed to determine Java version. IoTDB only supports jdk ^>= 17, please check your java installation.
	exit /b 1
)
IF %JAVA_VERSION% LSS 17 (
	echo IoTDB only supports jdk ^>= 17, please check your java version.
	exit /b 1
)

@REM -----------------------------------------------------------------------------
@REM SET DIRS
pushd "%~dp0..\.."
if NOT DEFINED IOTDB_HOME set "IOTDB_HOME=%cd%"
popd
if NOT DEFINED IOTDB_CONF set "IOTDB_CONF=%IOTDB_HOME%\conf"
set "IOTDB_LOG_DIR=%IOTDB_HOME%\logs"
if NOT EXIST "%IOTDB_LOG_DIR%" mkdir "%IOTDB_LOG_DIR%"

@REM Check both nodes' configured ports before starting the merged process.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0check-edge.ps1" -ConfigFile "%IOTDB_CONF%\iotdb-system.properties"
if ERRORLEVEL 1 exit /b 1

@REM -----------------------------------------------------------------------------
@REM SET JVM OPTIONS
if EXIST "%IOTDB_CONF%\windows\edge-env.bat" (
	call "%IOTDB_CONF%\windows\edge-env.bat"
) else (
	echo Can't find %IOTDB_CONF%\windows\edge-env.bat
	exit /b 1
)

set illegal_access_params=--add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED

set CLASSPATH=%IOTDB_HOME%\lib\*
set MAIN_CLASS=org.apache.iotdb.edge.EdgeNode

@REM CONFIGNODE_HOME must also point to the installation directory, otherwise the
@REM ConfigNode part resolves its data directories against the working directory.
set iotdb_parms=-Dlogback.configurationFile="%IOTDB_CONF%\logback-edge.xml"
set iotdb_parms=%iotdb_parms% -DIOTDB_HOME="%IOTDB_HOME%"
set iotdb_parms=%iotdb_parms% -DCONFIGNODE_HOME="%IOTDB_HOME%"
set iotdb_parms=%iotdb_parms% -DIOTDB_DATA_HOME="%IOTDB_HOME%"
set iotdb_parms=%iotdb_parms% -DTSFILE_HOME="%IOTDB_HOME%"
set iotdb_parms=%iotdb_parms% -DIOTDB_CONF="%IOTDB_CONF%"
set iotdb_parms=%iotdb_parms% -DCONFIGNODE_CONF="%IOTDB_CONF%"
set iotdb_parms=%iotdb_parms% -DTSFILE_CONF="%IOTDB_CONF%"
set iotdb_parms=%iotdb_parms% -Dname=iotdb.EdgeNode
set iotdb_parms=%iotdb_parms% -DIOTDB_LOG_DIR="%IOTDB_LOG_DIR%"
set iotdb_parms=%iotdb_parms% -DCONFIGNODE_LOG_DIR="%IOTDB_LOG_DIR%"
set iotdb_parms=%iotdb_parms% -DOFF_HEAP_MEMORY=%OFF_HEAP_MEMORY%

@REM -----------------------------------------------------------------------------
@REM START
java %illegal_access_params% %iotdb_parms% %IOTDB_JMX_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% -s
set "EDGE_EXIT_CODE=%ERRORLEVEL%"
pause
exit /b %EDGE_EXIT_CODE%
