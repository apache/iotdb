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

if "%OS%" == "Windows_NT" setlocal
pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

setlocal enabledelayedexpansion
set "timestamp=%date:~0,4%%date:~5,2%%date:~8,2%"
set "COLLECTION_DIR=%IOTDB_HOME%\collectioninfo"
set "COLLECTION_FILE=%COLLECTION_DIR%\collection-%timestamp%.txt"
set "START_CLI_PATH=%IOTDB_HOME%\sbin\start-cli.bat"

set "HELP=Usage: %0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-jp <jdk_path>] [-dd <data_dir>]"
set "user_param=root"
set "passwd_param=root"
set "host_param=127.0.0.1"
set "port_param=6667"
set "jdk_path_param="
set "data_dir_param=%IOTDB_HOME%\data"

:parse_args
if "%~1"=="" goto done
if "%~1"=="-u" (
    set "user_param=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="-pw" (
    set "passwd_param=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="-h" (
    set "host_param=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="-p" (
    set "port_param=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="-jp" (
    set "jdk_path_param=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="-dd" (
    set "data_dir_param=%~2"
    shift
    shift
    goto parse_args
)
echo Unrecognized option: %~1
echo %HELP%
exit /b 1

:done
echo user_param: %user_param%
echo passwd_param: %passwd_param%
echo host_param: %host_param%
echo port_param: %port_param%
echo jdk_path_param: %jdk_path_param%
echo data_dir_param: %data_dir_param%

set "command=show version"

call :collect_info
call :execute_command "show version" >> "%COLLECTION_FILE%"
call :execute_command "show cluster details" >> "%COLLECTION_FILE%"
call :execute_command "show regions" >> "%COLLECTION_FILE%"
call :execute_command "show databases" >> "%COLLECTION_FILE%"
call :execute_command "count devices" >> "%COLLECTION_FILE%"
call :execute_command "count timeseries" >> "%COLLECTION_FILE%"
echo "Program execution completed, directory name is %COLLECTION_DIR%"
exit /b

:collect_info
echo ---------------------
echo Start Collection info
echo ---------------------

if exist "%COLLECTION_DIR%" rmdir /s /q "%COLLECTION_DIR%"

mkdir "%COLLECTION_DIR%"

xcopy /E /I /Q "%IOTDB_HOME%\conf" "%COLLECTION_DIR%\conf"

set "files_to_zip=%COLLECTION_FILE% ../conf"

call :collect_cpu_info >> "%COLLECTION_FILE%"
call :collect_memory_info >> "%COLLECTION_FILE%"
call :collect_system_info >> "%COLLECTION_FILE%"
call :collect_jdk_version >> "%COLLECTION_FILE%"
call :collect_activation_info >> "%COLLECTION_FILE%"
call :total_file_num >> "%COLLECTION_FILE%"

exit /b

:collect_cpu_info
echo ====================== CPU Info ======================
wmic cpu get name | more +1
for /f %%b in ('wmic cpu get numberofcores ^| findstr "[0-9]"') do (
	set system_cpu_cores=%%b
)
if %system_cpu_cores% LSS 1 set system_cpu_cores=1
echo %system_cpu_cores% core
exit /b


:collect_memory_info
echo ===================== Memory Info =====================

REM Get total memory size
for /f  %%b in ('wmic ComputerSystem get TotalPhysicalMemory ^| findstr "[0-9]"') do (
	set system_memory=%%b
)

echo wsh.echo FormatNumber(cdbl(%system_memory%)/(1024*1024*1024), 0) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set system_memory_in_gb=%%a
del tmp.vbs
set system_memory_in_gb=%system_memory_in_gb:,=%

REM Output memory information
echo Total Memory: !system_memory_in_gb! GB
exit /b

:collect_system_info
echo ===================== System Info =====================
wmic os get Caption
exit /b

:collect_jdk_version
echo ===================== JDK Version =====================
if not "%jdk_path_param%"=="" (
    if exist "%jdk_path_param%" (
        "%jdk_path_param%\bin\java" -version 2>&1
    ) else (
        echo Invalid JDK path: %jdk_path_param%
    )
) else (
    java -version 2>&1
)
exit /b

:collect_activation_info
echo =================== Activation Info ====================
if exist "%~dp0/../activation" (
    if exist "%~dp0/../activation/license" (
        echo Active
    ) else (
        echo Not active
    )
) else (
    echo Open source version
)
exit /b

:execute_command
setlocal enabledelayedexpansion
set "command=%~1"
echo =================== "%command%" ====================
if not "%jdk_path_param%"=="" (
    set JAVA_HOME="%jdk_path_param%";call "%START_CLI_PATH%" -h "%host_param%" -p "%port_param%" -u "%user_param%" -pw "%passwd_param%" -e "%command%"
) else (
    call "%START_CLI_PATH%" -h "%host_param%" -p "%port_param%" -u "%user_param%" -pw "%passwd_param%" -e "%command%"
)

exit /b

:total_file_num
echo '===================== TsFile Info====================='
set "directories=%data_dir_param%"
set "seqFileCount=0"
set "unseqFileCount=0"
set /a seqFileSize=0
set /a unseqFileSize=0

for %%d in ("%directories: =" "%") do (
    set "seqdirectory=%%~d\datanode\data\sequence"
    set "unseqdirectory=%%~d\datanode\data\unsequence"

    for /f %%a in ('dir /s /b /a-d "!seqdirectory!" ^| find /c /v ""') do (
        set /a "seqFileCount+=%%a"
    )
    for /f %%a in ('dir /s /b /a-d "!unseqdirectory!" ^| find /c /v ""') do (
        set /a "unseqFileCount+=%%a"
    )

    call :processDirectory "!seqdirectory!" seqFileSize
    call :processDirectory "!unseqdirectory!" unseqFileSize
)

echo sequence(tsfile number): %seqFileCount%
echo unsequence(tsfile number): %unseqFileCount%
call :convertSize !seqFileSize! convertedSeqSize
call :convertSize !unseqFileSize! convertedUnseqSize
echo sequence(tsfile size): %convertedSeqSize%
echo unsequence(tsfile size): %convertedUnseqSize%
exit /b

:processDirectory
set "dir=%~1"
set "sizeVar=%~2"
set /a "size=0"

for /r "%dir%" %%f in (*) do (
    set /a "size+=%%~zf"
)

endlocal & set "%sizeVar%=%size%"
exit /b

:convertSize
setlocal enabledelayedexpansion
set "size=%~1"
set "unit=bytes"


echo wsh.echo FormatNumber(cdbl(%size%)/(1024*1024*1024), 0) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_gb=%%a
del tmp.vbs
set data_size_gb=%data_size_gb:,=%

echo wsh.echo FormatNumber(cdbl(%size%)/(1024*1024), 0) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_mb=%%a
del tmp.vbs
set data_size_mb=%data_size_mb:,=%

echo wsh.echo FormatNumber(cdbl(%size%)/(1024*1024), 0) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_kb=%%a
del tmp.vbs
set data_size_kb=%data_size_kb:,=%

if %data_size_gb% GTR 1 (
    set "unit=GB"
    set "data_size=%data_size_gb%"
)else if %data_size_mb% GTR 1 (
    set "unit=MB"
    set "data_size=%data_size_mb%"
) else if %data_size_kb% GTR 1 (
     set "unit=KB"
     set "data_size=%data_size_kb%"
)else (
    set "data_size=%size%"
)

endlocal & set "%~2=%data_size%%unit%"

exit /b