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

IF "%~1"=="--help" (
    echo The script will remove one or more DataNodes.
    echo Before removing DataNodes, ensure that the cluster has at least the required number of data/schema replicas DataNodes.
    echo Usage: ./sbin/remove-datanode.sh [DataNode_ID ...]
    echo Remove one or more DataNodes by specifying their IDs.
    echo Note that this datanode is removed by default if DataNode_ID is not specified.
    echo Example:
    echo   ./sbin/remove-datanode.sh 1         # Remove DataNode with ID 1
    echo   ./sbin/remove-datanode.sh 1 2 3     # Remove DataNodes with IDs 1, 2, and 3
    EXIT /B 0
)

REM Check for duplicate DataNode IDs
set "ids=%*"
set "unique_ids="
for %%i in (%ids%) do (
    if "!unique_ids!" == "" (
        set "unique_ids=%%i"
    ) else (
        echo !unique_ids! | findstr /b /c:"%%i " >nul
        if errorlevel 1 (
            set "unique_ids=!unique_ids! %%i"
        ) else (
            echo Error: Duplicate DataNode ID %%i found.
            EXIT /B 1
        )
    )
)

echo -------------------------
echo Starting to remove DataNodes: %ids%
echo -------------------------

PATH %PATH%;%JAVA_HOME%\bin\
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

IF "%JAVA_VERSION%" == "6" (
	echo IoTDB only supports jdk >= 8, please check your java version.
	goto finally
)
IF "%JAVA_VERSION%" == "7" (
	echo IoTDB only supports jdk >= 8, please check your java version.
	goto finally
)

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

set IOTDB_CONF=%IOTDB_HOME%\conf
set IOTDB_LOGS=%IOTDB_HOME%\logs

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set CONF_PARAMS=-r
set is_conf_path=false
for %%i in (%unique_ids%) do (
	set CONF_PARAMS=!CONF_PARAMS! %%i
)

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.db.service.DataNode
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%IOTDB_CONF%\logback-datanode.xml"^
 -DIOTDB_HOME="%IOTDB_HOME%"^
 -DTSFILE_HOME="%IOTDB_HOME%"^
 -DIOTDB_CONF="%IOTDB_CONF%"

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
if EXIST "%IOTDB_HOME%\lib" (set CLASSPATH="%IOTDB_HOME%\lib\*") else set CLASSPATH="%IOTDB_HOME%\..\lib\*"

@REM For each jar in the IOTDB_HOME lib directory call append to build the CLASSPATH variable.
set CLASSPATH=%CLASSPATH%;"%IOTDB_HOME%\lib\*"
set CLASSPATH=%CLASSPATH%;iotdb.db.service.DataNode
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

@REM In case the 2g memory is not enough in some scenarios, users can further reduce the memory usage manually.
@REM on heap memory size
set ON_HEAP_MEMORY=2G

@REM off heap memory size
set OFF_HEAP_MEMORY=512M

"%JAVA_HOME%\bin\java" %ILLEGAL_ACCESS_PARAMS%  %JAVA_OPTS% %IOTDB_HEAP_OPTS% -Xms%ON_HEAP_MEMORY% -Xmx%ON_HEAP_MEMORY% -XX:MaxDirectMemorySize=%OFF_HEAP_MEMORY% -cp %CLASSPATH% %IOTDB_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
