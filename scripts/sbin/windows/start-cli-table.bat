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


REM -------------------------------
REM Default SQL dialect
if "%DEFAULT_SQL_DIALECT%"=="" set DEFAULT_SQL_DIALECT=table

REM Default connection parameters
set user_param=-u root
set passwd_param=-pw root
set host_param=-h 127.0.0.1
set port_param=-p 6667
set sql_dialect_param=-sql_dialect %DEFAULT_SQL_DIALECT%
set PARAMETERS=

REM -------------------------------
REM Parse command-line arguments
:parse_args
if "%~1"=="" goto after_parse

if /I "%~1"=="-u" (
    set user_param=-u %~2
    shift
    shift
    goto parse_args
)
if /I "%~1"=="-pw" (
    if "%~2"=="" (
        set passwd_param=-pw
        shift
    ) else (
        set passwd_param=-pw %~2
        shift
        shift
    )
    goto parse_args
)
if /I "%~1"=="-h" (
    set host_param=-h %~2
    shift
    shift
    goto parse_args
)
if /I "%~1"=="-p" (
    set port_param=-p %~2
    shift
    shift
    goto parse_args
)
if /I "%~1"=="-sql_dialect" (
    set sql_dialect_param=-sql_dialect %~2
    shift
    shift
    goto parse_args
)

REM Any other arguments
set PARAMETERS=%PARAMETERS% %~1
shift
goto parse_args

:after_parse

REM Combine all parameters
set PARAMETERS=%host_param% %port_param% %user_param% %passwd_param% %sql_dialect_param% %PARAMETERS%

REM -------------------------------
REM Set IOTDB_HOME
if not defined IOTDB_HOME (
    pushd %~dp0..\..
    set IOTDB_HOME=%CD%
    popd
)

REM CLI configuration
set IOTDB_CLI_CONF=%IOTDB_HOME%\conf
set MAIN_CLASS=org.apache.iotdb.cli.Cli

REM -------------------------------
REM CLASSPATH setup
if exist "%IOTDB_HOME%\lib" (
    set CLASSPATH=%IOTDB_HOME%\lib\*
) else (
    set CLASSPATH=%IOTDB_HOME%\..\lib\*
)

REM -------------------------------
REM JAVA executable
if defined JAVA_HOME (
    if exist "%JAVA_HOME%\bin\java.exe" (
        set JAVA=%JAVA_HOME%\bin\java.exe
    ) else (
        set JAVA=java
    )
) else (
    set JAVA=java
)

REM -------------------------------
REM JVM options
set JVM_OPTS=-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8
set IOTDB_CLI_PARAMS=-Dlogback.configurationFile=%IOTDB_CLI_CONF%\logback-cli.xml
set JVM_OPTS=-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 --add-opens=java.base/java.lang=ALL-UNNAMED

REM -------------------------------
REM Run CLI
echo %PARAMETERS%
"%JAVA%" %JVM_OPTS% %IOTDB_CLI_PARAMS% -cp "%CLASSPATH%" %MAIN_CLASS% %PARAMETERS%
exit /b %ERRORLEVEL%
