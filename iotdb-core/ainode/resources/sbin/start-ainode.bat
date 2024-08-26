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
echo Starting IoTDB AINode
echo ```````````````````````````

set START_SCRIPT_DIR=%~dp0
call %START_SCRIPT_DIR%\\..\\conf\\ainode-env.bat %*
if %errorlevel% neq 0 (
    echo Environment check failed. Exiting...
    exit /b 1
)

for /f "tokens=2 delims==" %%a in ('findstr /i /c:"^ain_interpreter_dir" "%START_SCRIPT_DIR%\\..\\conf\\ainode-env.bat"') do (
    set _ain_interpreter_dir=%%a
    goto :done
)

:initial
if "%1"=="" goto done
set aux=%1
if "%aux:~0,1%"=="-" (
   set nome=%aux:~1,250%
) else (
   set "%nome%=%1"
   set nome=
)
shift
goto initial

:done
if "%i%"=="" (
    if "%_ain_interpreter_dir%"=="" (
        set _ain_interpreter_dir=%START_SCRIPT_DIR%\\..\\venv\\Scripts\\python.exe
    )
) else (
    set _ain_interpreter_dir=%i%
)

echo Script got parameter: ain_interpreter_dir: %_ain_interpreter_dir%

cd %START_SCRIPT_DIR%\\..

for %%i in ("%_ain_interpreter_dir%") do set "parent=%%~dpi"

set ain_ainode_dir=%parent%\ainode.exe

set ain_ainode_dir_new=%parent%\Scripts\\ainode.exe

echo Starting AINode...

%ain_ainode_dir% start
if %errorlevel% neq 0 (
    echo ain_ainode_dir_new is %ain_ainode_dir_new%
    %ain_ainode_dir_new% start
)

pause