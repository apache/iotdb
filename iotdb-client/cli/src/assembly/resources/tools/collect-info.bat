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

set "COLLECTION_DIR_NAME=iotdb-info"
set "COLLECTION_DIR=%IOTDB_HOME%\%COLLECTION_DIR_NAME%"
set "COLLECTION_DIR_LOGS=%COLLECTION_DIR%\logs"
set "COLLECTION_FILE=%COLLECTION_DIR%\collection.txt"
set "START_CLI_PATH=%IOTDB_HOME%\sbin\start-cli.bat"

set "HELP=Usage: %0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-dd <data_dir>]"
set "user_param=root"
set "passwd_param=root"
set "host_param=127.0.0.1"
set "port_param=6667"

set "properties_file=%IOTDB_HOME%\conf\iotdb-datanode.properties"
set "key=dn_data_dirs"

for /f "usebackq tokens=1,* delims==" %%a in ("%properties_file%") do (
    if "%%a"=="%key%" (
        set "value=%%b"
    )
)

IF "%value%"=="" (
    set "data_dir_param=%IOTDB_HOME%\data\datanode\data"
) else (
    set "data_dir_param=%value%"
)

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
echo "Program execution completed, directory name is %COLLECTION_DIR%"
exit /b

:collect_info
echo ---------------------
echo Start collecting info
echo ---------------------

if exist "%COLLECTION_DIR%" rmdir /s /q "%COLLECTION_DIR%"

mkdir "%COLLECTION_DIR_LOGS%"

xcopy /E /I /Q "%IOTDB_HOME%\conf" "%COLLECTION_DIR%\conf"

call :collection_logs
call :collect_system_info >> "%COLLECTION_FILE%"
call :collect_jdk_version >> "%COLLECTION_FILE%"
call :collect_activation_info >> "%COLLECTION_FILE%"
call :total_file_num >> "%COLLECTION_FILE%"
call :execute_command "show version" >> "%COLLECTION_FILE%" 2>&1
call :execute_command "show cluster details" >> "%COLLECTION_FILE%" 2>&1
call :execute_command "show regions" >> "%COLLECTION_FILE%" 2>&1
call :execute_command "show databases" >> "%COLLECTION_FILE%" 2>&1
call :execute_command "count devices" >> "%COLLECTION_FILE%" 2>&1
call :execute_command "count timeseries" >> "%COLLECTION_FILE%" 2>&1

exit /b

:collect_system_info
echo ===================== System Info =====================
systeminfo
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

:collection_logs
for %%F in ("%IOTDB_HOME%\logs\*.log") do (
    echo %%F
    if /I "%%~xF"==".log" (
        copy "%%F" "%COLLECTION_DIR_LOGS%"
    )
)
exit /b

:execute_command
setlocal enabledelayedexpansion
set "command=%~1"
echo =================== "%command%" ====================
call "%START_CLI_PATH%" -h "%host_param%" -p "%port_param%" -u "%user_param%" -pw "%passwd_param%" -e "%command%"

exit /b

:total_file_num
echo '===================== TsFile Info====================='
set "directories=%data_dir_param%"
set "seqFileCount=0"
set "unseqFileCount=0"


set /a totalSeqFileSize=0
set /a totalUnseqFileSize=0

set "directories=!directories:,= !"
set /a seqFileSize=0
set /a unseqFileSize=0

for %%d in ("%directories: =" "%") do (
    set "seqdirectory=%%~d\sequence"
    set "unseqdirectory=%%~d\unsequence"
    if exist "!seqdirectory!" (
        for /f %%a in ('dir /s /b /a-d "!seqdirectory!\\*.tsfile"  2^>nul ^| find /c /v ""') do (
            set /a "seqFileCount+=%%a"
        )
     call :processDirectory "!seqdirectory!" seqFileSize
    )
    if exist "!unseqdirectory!\*" (
        for /f %%a in ('dir /s /b /a-d "!unseqdirectory!\\*.tsfile" 2^>nul ^| find /c /v ""') do (
            set /a "unseqFileCount+=%%a"
        )
     call :processDirectory "!unseqdirectory!" unseqFileSize
    )


    set /a "totalSeqFileSize+=!seqFileSize!"
    set /a "totalUnseqFileSize+=!unseqFileSize!"
)

echo sequence(tsfile number): %seqFileCount%
echo unsequence(tsfile number): %unseqFileCount%
call :convertSize %totalSeqFileSize% convertedSeqSize
call :convertSize %totalUnseqFileSize% convertedUnSeqSize
echo sequence(tsfile size): %convertedSeqSize%
echo unsequence(tsfile size): %convertedUnSeqSize%
exit /b

:processDirectory
setlocal enabledelayedexpansion
set "dir=%~1"
set "sizeVar=%~2"

echo Set objFSO = CreateObject("Scripting.FileSystemObject") > tmp.vbs
echo Set objFolder = objFSO.GetFolder("%dir%") >> tmp.vbs
echo Dim totalSize >> tmp.vbs
echo totalSize = 0 >> tmp.vbs
echo CalculateFolderSize objFolder >> tmp.vbs
echo WScript.Echo totalSize >> tmp.vbs
echo Sub CalculateFolderSize(objCurrentFolder) >> tmp.vbs
echo    For Each objFile In objCurrentFolder.Files >> tmp.vbs
echo        totalSize = totalSize + objFile.Size >> tmp.vbs
echo    Next >> tmp.vbs

echo    For Each objSubFolder In objCurrentFolder.SubFolders >> tmp.vbs
echo        CalculateFolderSize objSubFolder >> tmp.vbs
echo    Next >> tmp.vbs
echo End Sub >> tmp.vbs

for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size=%%a
del tmp.vbs

endlocal & set "%sizeVar%=%data_size%"
exit /b

:convertSize
setlocal enabledelayedexpansion
set "size=%~1"
echo wsh.echo FormatNumber(cdbl(%size%)/(1024), 0) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_kb=%%a
del tmp.vbs
set data_size_kb=!data_size_kb:,=!

echo wsh.echo FormatNumber(cdbl(%size%)/(1024*1024), 1) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_mb=%%a
del tmp.vbs
set data_size_mb=!data_size_mb:,=!

echo wsh.echo FormatNumber(cdbl(%size%)/(1024*1024*1024), 1) > tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo tmp.vbs') do set data_size_gb=%%a
del tmp.vbs
set data_size_gb=!data_size_gb:,=!

if !data_size_gb! gtr 1 (
    set "size=!data_size_gb!GB"
) else if !data_size_mb! gtr 1 (
    set "size=!data_size_mb!MB"
) else if !data_size_kb! gtr 1 (
    set "size=!data_size_kb!KB"
) else (
    set "size=!size!B"
)
endlocal & set "%~2=%size%"
exit /b
