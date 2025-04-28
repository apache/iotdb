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
@REM set cmd format
powershell -NoProfile -Command "$v=(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion').CurrentMajorVersionNumber; if($v -gt 6) { cmd /c 'chcp 65001' }"


@REM DEFAULT_SQL_DIALECT is used to set the default SQL dialect for the CLI.
@REM empty value means using "tree".
@REM Optional values: "table" or "tree"
set DEFAULT_SQL_DIALECT=table

@REM You can put your env variable here
@REM set JAVA_HOME=%JAVA_HOME%

title IoTDB CLI

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

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%CD%
popd

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.cli.Cli
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIOTDB_HOME="%IOTDB_HOME%"

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
if EXIST "%IOTDB_HOME%\lib" (set CLASSPATH="%IOTDB_HOME%\lib\*") else set CLASSPATH="%IOTDB_HOME%\..\lib\*"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1

goto :eof

REM -----------------------------------------------------------------------------
:okClasspath
set PARAMETERS=%*

@REM if "%PARAMETERS%" == "" set PARAMETERS=-h 127.0.0.1 -p 6667 -u root -pw root

@REM if DEFAULT_SQL_DIALECT is empty, set it to "tree"
if "%DEFAULT_SQL_DIALECT%" == "" set DEFAULT_SQL_DIALECT=tree

@REM set default parameters
set pw_parameter=-pw root
set u_parameter=-u root
set p_parameter=-p 6667
set h_parameter=-h 127.0.0.1
set sql_dialect__parameter=-sql_dialect %DEFAULT_SQL_DIALECT%

@REM Added parameters when default parameters are missing
echo %PARAMETERS% | findstr /c:"-sql_dialect ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%sql_dialect__parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-pw ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%pw_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-u ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%u_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-p ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%p_parameter% %PARAMETERS%)
echo %PARAMETERS% | findstr /c:"-h ">nul && (set PARAMETERS=%PARAMETERS%) || (set PARAMETERS=%h_parameter% %PARAMETERS%)

echo %PARAMETERS%

@REM Add args for Java 11 and above, due to [JEP 396: Strongly Encapsulate JDK Internals by Default] (https://openjdk.java.net/jeps/396)
IF "%JAVA_VERSION%" == "8" (
    set ILLEGAL_ACCESS_PARAMS=
) ELSE (
    set ILLEGAL_ACCESS_PARAMS=--add-opens=java.base/java.lang=ALL-UNNAMED
)

java %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %PARAMETERS%
set ret_code=%ERRORLEVEL%
goto finally


:err
echo JAVA_HOME environment variable must be set!
set ret_code=1
pause


@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL

EXIT /B %ret_code%
