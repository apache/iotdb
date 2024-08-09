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
    echo The script will remove a ConfigNode.
    echo Before removing a ConfigNode, ensure that there is at least one active ConfigNode in the cluster after the removal.
    echo Usage:
    echo Remove the ConfigNode with confignode_id
    echo ./sbin/remove-confignode.bat [confignode_id]
    EXIT /B 0
)

echo ```````````````````````````
echo Starting to remove IoTDB ConfigNode
echo ```````````````````````````


set PATH="%JAVA_HOME%\bin\";%PATH%
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

@REM we do not check jdk that version less than 1.8 because they are too stale...
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
if NOT DEFINED CONFIGNODE_HOME set CONFIGNODE_HOME=%cd%
popd

set CONFIGNODE_CONF=%CONFIGNODE_HOME%\conf
set CONFIGNODE_LOGS=%CONFIGNODE_HOME%\logs

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set CONF_PARAMS=-r
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

IF EXIST "%CONFIGNODE_CONF%\confignode-env.bat" (
    CALL "%CONFIGNODE_CONF%\confignode-env.bat" %1
    ) ELSE (
    echo "can't find %CONFIGNODE_CONF%\confignode-env.bat"
    )

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.confignode.service.ConfigNode
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%CONFIGNODE_CONF%\logback-confignode.xml"^
 -DCONFIGNODE_HOME="%CONFIGNODE_HOME%"^
 -DCONFIGNODE_CONF="%CONFIGNODE_CONF%"^
 -Dsun.jnu.encoding=UTF-8^
 -Dfile.encoding=UTF-8

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
if EXIST "%CONFIGNODE_HOME%\lib" (set CLASSPATH="%CONFIGNODE_HOME%\lib\*") else set CLASSPATH="%CONFIGNODE_HOME%\..\lib\*"
set CLASSPATH=%CLASSPATH%;iotdb.ConfigNode
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1

goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% %CONFIGNODE_HEAP_OPTS% -cp %CLASSPATH% %CONFIGNODE_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
