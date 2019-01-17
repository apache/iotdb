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
echo ````````````````````````
echo Starting IoTDB
echo ````````````````````````

PATH %PATH%;%JAVA_HOME%\bin\
for /f tokens^=2-5^ delims^=.-_^" %%j in ('java -fullversion 2^>^&1') do (
		set "FULL_VERSION=%%j%%k%%l%%m"
		set "MAJOR_VERSION=%%j"
		set "MINOR_VERSION=%%k"
	)

IF "%MAJOR_VERSION%" == "1" (
	set JAVA_VERSION=%MINOR_VERSION%
) else (
	set JAVA_VERSION=%MAJOR_VERSION%
)

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
    CALL "%IOTDB_CONF%\iotdb-env.bat"
    ) ELSE (
    echo "can't find %IOTDB_CONF%\iotdb-env.bat"
    )

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.db.service.IoTDB
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%IOTDB_CONF%\logback.xml"^
 -DIOTDB_HOME="%IOTDB_HOME%"^
 -DTSFILE_HOME="%IOTDB_HOME%"^
 -DIOTDB_CONF="%IOTDB_CONF%"

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%IOTDB_HOME%\lib"

REM For each jar in the IOTDB_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%IOTDB_HOME%\lib\*.jar") do call :append "%%i"
set CLASSPATH=%CLASSPATH%;iotdb.IoTDB
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %IOTDB_HEAP_OPTS% -cp %CLASSPATH% %IOTDB_JMX_OPTS% %MAIN_CLASS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
