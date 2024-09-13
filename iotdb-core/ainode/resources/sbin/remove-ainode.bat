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
    echo The script will remove an AINode.
    echo When it is necessary to move an already connected AINode out of the cluster, the corresponding removal script can be executed.
    echo Usage:
    echo Remove the AINode with ainode_id
    echo ./sbin/remove-ainode.bat -t [ainode_id]
    echo.
    echo Options:
    echo ^ ^ -t = ainode_id
    echo ^ ^ -i = When specifying the Python interpreter please enter the address of the executable file of the Python interpreter in the virtual environment. Currently AINode supports virtual environments such as venv, conda, etc. Inputting the system Python interpreter as the installation location is not supported. In order to ensure that scripts are recognized properly, please use absolute paths whenever possible!
    EXIT /B 0
)

echo ```````````````````````````
echo Removing IoTDB AINode
echo ```````````````````````````

set REMOVE_SCRIPT_DIR=%~dp0
call %REMOVE_SCRIPT_DIR%\\..\\conf\\\ainode-env.bat %*
if %errorlevel% neq 0 (
    echo Environment check failed. Exiting...
    exit /b 1
)

:initial
if "%1"=="" goto interpreter
set aux=%1
if "%aux:~0,1%"=="-" (
   set nome=%aux:~1,250%
) else (
   set "%nome%=%1"
   set nome=
)
shift
goto initial

for /f "tokens=2 delims==" %%a in ('findstr /i /c:"^ain_interpreter_dir" "%REMOVE_SCRIPT_DIR%\\..\\conf\\\ainode-env.bat"') do (
    set _ain_interpreter_dir=%%a
    goto :interpreter
)

:interpreter
if "%i%"=="" (
    if "%_ain_interpreter_dir%"=="" (
        set _ain_interpreter_dir=%REMOVE_SCRIPT_DIR%\\..\\venv\\Scripts\\python.exe
    )
) else (
    set _ain_interpreter_dir=%i%
)


for /f "tokens=2 delims==" %%a in ('findstr /i /c:"^ain_system_dir" "%REMOVE_SCRIPT_DIR%\\..\\conf\\iotdb-\ainode.properties"') do (
    set _ain_system_dir=%%a
    goto :system
)

:system
if "%_ain_system_dir%"=="" (
    set _ain_system_dir=%REMOVE_SCRIPT_DIR%\\..\\data\\\ainode\\system
)

echo Script got parameters: ain_interpreter_dir: %_ain_interpreter_dir%, ain_system_dir: %_ain_system_dir%

cd %REMOVE_SCRIPT_DIR%\\..
for %%i in ("%_ain_interpreter_dir%") do set "parent=%%~dpi"
set ain_\ainode_dir=%parent%\\\ainode.exe

if "%t%"=="" (
    echo No target AINode set, use system.properties
    %ain_\ainode_dir% remove
) else (
    %ain_\ainode_dir% remove %t%
)

if %errorlevel% neq 0 (
    echo Remove AINode failed. Exiting...
    exit /b 1
)

call %REMOVE_SCRIPT_DIR%\\stop-\ainode.bat %*

rd /s /q %_ain_system_dir%

pause