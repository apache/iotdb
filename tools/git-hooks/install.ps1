################################################################################
##
##  Licensed to the Apache Software Foundation (ASF) under one or more
##  contributor license agreements.  See the NOTICE file distributed with
##  this work for additional information regarding copyright ownership.
##  The ASF licenses this file to You under the Apache License, Version 2.0
##  (the "License"); you may not use this file except in compliance with
##  the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

if (!([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] 'Administrator')) {
    Start-Process -FilePath PowerShell.exe -Verb Runas -ArgumentList "-File `"$($MyInvocation.MyCommand.Path)`"  `"$($MyInvocation.MyCommand.UnboundArguments)`""
    exit
}

Push-Location $PSScriptRoot

# project directory
$PROJECT_DIR="..\.."
# hooks directory
$HOOKS_DIR="$PROJECT_DIR\.git\hooks"
# tool directory rel to project
$REL_SCRIPT_DIR="tools\git-hooks"

# config sample filename rel to script dir
$REL_CONFIG_SAMPLE_FILE="config.sample.sh"
# config filename rel to script dir
$REL_CONFIG_FILE="config.sh"

function Check-Paths {
    Write-Output "Checking paths..."
    if (-Not(Test-Path -Path "$PROJECT_DIR" -PathType Container)) {
        throw "Project directory '$PROJECT_DIR' does not exist."
    }
    if (-Not(Test-Path -Path "$HOOKS_DIR" -PathType Container)) {
        throw "Hooks directory '$HOOKS_DIR' does not exist."
    }
}

function Install {
    # create the relative symlink of pre-commit script
    Write-Output "Installing pre-commit hook to '$HOOKS_DIR/pre-commit'..."
    if (Test-Path -Path "$HOOKS_DIR\pre-commit" -PathType Leaf) {
        Write-Warning "Overwriting '$HOOKS_DIR\pre-commit'"
    }
    New-Item -ItemType SymbolicLink -Force `
        -Path "$HOOKS_DIR\pre-commit" `
        -Target "$PROJECT_DIR\$REL_SCRIPT_DIR\pre-commit" `
        | Out-Null
    if (-Not($?)) {
        throw "Fail to create symlink for pre-commit script."
    }
    # create the relative symlink of config file
    Write-Output "Creating '$REL_CONFIG_FILE'..."
    if (-Not(Test-Path -Path "$REL_CONFIG_FILE" -PathType Leaf)) {
        if (-Not(Test-Path -Path "$REL_CONFIG_SAMPLE_FILE")) {
            throw "Could not find '$REL_CONFIG_SAMPLE_FILE'."
        }
        Copy-Item -Path "$REL_CONFIG_SAMPLE_FILE" -Destination "$REL_CONFIG_FILE"
    } else {
        Write-Warning "$REL_CONFIG_FILE is already existed. Ignored."
    }
    New-Item -ItemType SymbolicLink -Force `
        -Path "$HOOKS_DIR\config.sh" `
        -Target "$PROJECT_DIR\$REL_SCRIPT_DIR\config.sh" `
        | Out-Null
    if (-Not($?)) {
        throw "Fail to create symlink for config file."
    }
    # finished
    Write-Output "Installed hooks."
}

function Main {
    Write-Output "Installing pre-commit hook..."
    Check-Paths
    Install
}

trap [Exception] {
    $e=$_.Exception.Message
    $lineno=$_.Exception.InvocationInfo.ScriptLineNumber
    Write-Error "Caught exception: $e at line $lineno"
    Write-Error "Installation failed."
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

Main
Pop-Location
Write-Host "Installation succeed." -ForegroundColor Cyan
Read-Host -Prompt "Press Enter to exit"
