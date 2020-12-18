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
echo ````````````````````````
echo Starting IoTDB
echo ````````````````````````

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

IF NOT %JAVA_VERSION% == 8 (
	IF NOT %JAVA_VERSION% == 11 (
		echo IoTDB only supports jdk8 or jdk11, please check your java version.
		goto finally
	)
)

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

set IOTDB_CONF=%IOTDB_HOME%\conf
set IOTDB_LOGS=%IOTDB_HOME%\logs


IF EXIST "%IOTDB_CONF%\iotdb-env.bat" (
    IF "%1" == "printgc" (
      CALL "%IOTDB_CONF%\iotdb-env.bat" printgc
      SHIFT
    ) ELSE (
      CALL "%IOTDB_CONF%\iotdb-env.bat"
    )
) ELSE (
    echo "can't find %IOTDB_CONF%\iotdb-env.bat"
)

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set CONF_PARAMS=-s
set is_conf_path=false
for %%i in (%*) do (
	IF "%%i" == "-c" (
		set is_conf_path=true
	) ELSE IF "!is_conf_path!" == "true" (
		set is_conf_path=false
		set IOTDB_CONF=%%i
	) ELSE (
		set CONF_PARAMS=!CONF_PARAMS! %%i
	)
)

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.cluster.ClusterMain
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%IOTDB_CONF%\logback.xml"^
 -DIOTDB_HOME="%IOTDB_HOME%"^
 -DTSFILE_HOME="%IOTDB_HOME%"^
 -DCLUSTER_CONF="%IOTDB_CONF%"^
 -DIOTDB_CONF="%IOTDB_CONF%"

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%IOTDB_HOME%\lib"

@REM For each jar in the IOTDB_HOME lib directory call append to build the CLASSPATH variable.
set CLASSPATH=%CLASSPATH%;"%IOTDB_HOME%\lib\*"
set CLASSPATH=%CLASSPATH%;iotdb.ClusterMain
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %IOTDB_HEAP_OPTS% -cp %CLASSPATH% %IOTDB_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL