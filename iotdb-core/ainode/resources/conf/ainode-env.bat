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

@REM The defaulte venv environment is used if ain_interpreter_dir is not set. Please use absolute path without quotation mark
@REM set ain_interpreter_dir=

@REM Set ain_force_reinstall to 1 to force reinstall ainode
set ain_force_reinstall=0

@REM don't install dependencies online
set ain_install_offline=0

set ENV_SCRIPT_DIR=%~dp0

:initial
if "%1"=="" goto done
set aux=%1
if "%aux:~0,2%"=="-r" (
    set ain_force_reinstall=1
    shift
    goto initial
)
if "%aux:~0,2%"=="-n" (
    set ain_no_dependencies=--no-dependencies
    shift
    goto initial
)
if "%aux:~0,1%"=="-" (
   set nome=%aux:~1,250%
) else (
   set "%nome%=%1"
   set nome=
)
shift
goto initial

:done
@REM check if the parameters are set
if "%i%"=="" (
    echo No interpreter_dir is set, use default value.
) else (
    set ain_interpreter_dir=%i%
)

echo Script got inputs: ain_interpreter_dir: %ain_interpreter_dir% , ain_force_reinstall: %ain_force_reinstall%
if "%ain_interpreter_dir%"=="" (
    %ENV_SCRIPT_DIR%//..//venv//Scripts//python.exe -c "import sys; print(sys.executable)" && (
        echo Activate default venv environment
    ) || (
        echo Creating default venv environment
        python -m venv "%ENV_SCRIPT_DIR%//..//venv"
    )
    set ain_interpreter_dir="%ENV_SCRIPT_DIR%//..//venv//Scripts//python.exe"
)

@REM Switch the working directory to the directory one level above the script
cd %ENV_SCRIPT_DIR%/../

echo Confirming ainode
%ain_interpreter_dir% -m pip config set global.disable-pip-version-check true
%ain_interpreter_dir% -m pip list | findstr /C:"apache-iotdb-ainode" >nul
if %errorlevel% == 0 (
    if %ain_force_reinstall% == 0 (
        echo ainode is already installed
        exit /b 0
    )
)

set ain_only_ainode=1
@REM if $ain_install_offline is 1 then do not install dependencies
if %ain_install_offline% == 1 (
    @REM if offline and not -n, then install dependencies
    if "%ain_no_dependencies%"=="" (
        set ain_only_ainode=0
    ) else (
        set ain_only_ainode=1
    )
    set ain_no_dependencies=--no-dependencies
    echo Installing ainode offline----without dependencies...
)

if %ain_force_reinstall% == 1 (
    set ain_force_reinstall=--force-reinstall
) else (
    set ain_force_reinstall=
)

echo Installing ainode...
@REM Print current work dir
cd lib
for %%i in (*.whl *.tar.gz) do (
    echo %%i | findstr "ainode" >nul && (
        echo Installing ainode body: %%i
        %ain_interpreter_dir% -m pip install %%i %ain_force_reinstall% --no-warn-script-location %ain_no_dependencies% --find-links https://download.pytorch.org/whl/cpu/torch_stable.html
    ) || (
        @REM if ain_only_ainode is 0 then install dependencies
        if %ain_only_ainode% == 0 (
            echo Installing dependencies: %%i
            set ain_force_reinstall=--force-reinstall
            %ain_interpreter_dir% -m pip install %%i %ain_force_reinstall% --no-warn-script-location %ain_no_dependencies% --find-links https://download.pytorch.org/whl/cpu/torch_stable.html
        )
    )
    if %errorlevel% == 1 (
        echo Failed to install ainode
        exit /b 1
    )
)
echo ainode is installed successfully
cd ..
exit /b 0
