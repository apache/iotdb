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

echo ```````````````````````````
echo Starting IoTDB ConfigNode
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
set is_conf_path=false
for %%i in (%*) do (
	IF "%%i" == "-c" (
		set is_conf_path=true
	) ELSE IF "!is_conf_path!" == "true" (
		set is_conf_path=false
		set CONFIGNODE_CONF=%%i
	)
)

IF EXIST "%CONFIGNODE_CONF%\confignode-env.bat" (
    CALL "%CONFIGNODE_CONF%\confignode-env.bat" %1
    ) ELSE (
    echo "Can't find %CONFIGNODE_CONF%\confignode-env.bat"
    )

@REM CHECK THE PORT USAGES
IF EXIST "%CONFIGNODE_CONF%\iotdb-confignode.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_internal_port"
    %CONFIGNODE_CONF%\iotdb-confignode.properties') do (
      set cn_internal_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_consensus_port"
    %CONFIGNODE_CONF%\iotdb-confignode.properties') do (
      set cn_consensus_port=%%i
  )
) ELSE IF EXIST "%CONFIGNODE_HOME%\conf\iotdb-confignode.properties" (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_internal_port"
      %CONFIGNODE_HOME%\conf\iotdb-confignode.properties') do (
        set cn_internal_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_consensus_port"
      %CONFIGNODE_HOME%\conf\iotdb-confignode.properties') do (
        set cn_consensus_port=%%i
  )
) ELSE (
  echo "Can't find iotdb-confignode.properties, check the default ports"
  set cn_internal_port=10710
  set cn_consensus_port=10720
)

echo Check whether the ports are occupied....
set occupied=0
set cn_internal_port_occupied=0
set cn_consensus_port_occupied=0
for /f  "tokens=1,3,7 delims=: " %%i in ('netstat /ano') do (
    if %%i==TCP (
       if %%j==%cn_internal_port% (
         if !cn_internal_port_occupied!==0 (
           echo The cn_internal_port %cn_internal_port% is already occupied, pid:%%k
           set occupied=1
           set cn_internal_port_occupied=1
         )
       ) else if %%j==%cn_consensus_port% (
         if !cn_consensus_port_occupied!==0 (
           echo The cn_consensus_port %cn_consensus_port% is already occupied, pid:%%k
           set occupied=1
           set cn_consensus_port_occupied=1
         )
       )
    )
)

if %occupied%==1 (
  echo There exists occupied port, please change the configuration.
  TIMEOUT /T 10 /NOBREAK
  exit 0
)

set CONF_PARAMS=-s
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
if EXIST %CONFIGNODE_HOME%\lib (set CLASSPATH="%CONFIGNODE_HOME%\lib\*") else set CLASSPATH="%CONFIGNODE_HOME%\..\lib\*"
set CLASSPATH=%CLASSPATH%;iotdb.ConfigNode
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1

goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

@REM SET PARA

@REM Before v0.14, iotdb-server runs in foreground by default
@REM set foreground=0
set foreground=yes

:checkPara
set COMMANSLINE=%*
@REM setlocal ENABLEDELAYEDEXPANSION
:STR_VISTOR
for /f "tokens=1* delims= " %%a in ("%COMMANSLINE%") do (
@REM -----more para-----
for /f "tokens=1* delims==" %%1 in ("%%a") do (
@REM echo 1=%%1 "|||" 2=%%2
if "%%1"=="-v" ( java %JAVA_OPTS% -Dlogback.configurationFile="%CONFIGNODE_CONF%/logback-tool.xml" -cp %CLASSPATH% org.apache.iotdb.db.service.GetVersion & goto finally )
if "%%1"=="-f" ( set foreground=yes)
if "%%1"=="-d" ( set foreground=0)
)
set COMMANSLINE=%%b
goto STR_VISTOR
)

@REM ----------------------------------------------------------------------------
@REM START
:start
if %foreground%==yes (
	java %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% %CONFIGNODE_HEAP_OPTS% -cp %CLASSPATH% %CONFIGNODE_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
	) ELSE (
	start javaw %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% %CONFIGNODE_HEAP_OPTS% -cp %CLASSPATH% %CONFIGNODE_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
	)
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally
@ENDLOCAL
pause