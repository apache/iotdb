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
setlocal EnableExtensions DisableDelayedExpansion

if not "%~2"=="" goto usage
if "%~1"=="-f" goto confirmed
if not "%~1"=="" goto usage
set "CLEAN_SERVICE="
set /p "CLEAN_SERVICE=Do you want to clean all the data of IoTDB Edge? y/n (default n): "
if /i "%CLEAN_SERVICE%"=="y" goto confirmed
echo Exiting...
exit /b 0

:confirmed
if not defined IOTDB_HOME set "IOTDB_HOME=%~dp0..\..\.."
for %%i in ("%IOTDB_HOME%") do set "IOTDB_HOME=%%~fi"
if not defined IOTDB_CONF set "IOTDB_CONF=%IOTDB_HOME%\conf"
if not exist "%IOTDB_HOME%\sbin\windows\stop-edge.bat" (
    echo Cannot find the Edge stop script. No data has been removed.
    exit /b 1
)

@REM Wait for the merged process and propagate stop errors before removing data.
call "%IOTDB_HOME%\sbin\windows\stop-edge.bat" -f
if errorlevel 1 exit /b 1

@REM PowerShell is already required by stop-edge.bat. Literal paths preserve spaces,
@REM Unicode, drive/UNC paths and wildcard characters in configured directories.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ErrorActionPreference = 'Stop';" ^
    "$iotdbHome = [IO.Path]::GetFullPath($env:IOTDB_HOME).TrimEnd('\', '/');" ^
    "$cn = [ordered]@{cn_system_dir='data/confignode/system'; cn_consensus_dir='data/confignode/consensus'};" ^
    "$dn = [ordered]@{dn_system_dir='data/datanode/system'; dn_data_dirs='data/datanode/data'; dn_consensus_dir='data/datanode/consensus'; dn_wal_dirs='data/datanode/wal'; dn_tracing_dir='datanode/tracing'; dn_sync_dir='data/datanode/sync'; pipe_receiver_file_dirs='data/datanode/system/pipe/receiver'; iot_consensus_v2_receiver_file_dirs='data/datanode/system/pipe/consensus/receiver'; sort_tmp_dir='data/datanode/tmp'};" ^
    "function Read-Directories($file, $directories) {" ^
    "    $defaults = @{}; foreach ($key in $directories.Keys) { $defaults[$key] = $directories[$key]; }" ^
    "    if (Test-Path -LiteralPath $file) {" ^
    "        foreach ($line in Get-Content -LiteralPath $file -Encoding UTF8) {" ^
    "            if ($line -match '^\s*([^#!\s][^=]*?)\s*=\s*(.*?)\s*$') {" ^
    "                $key = $Matches[1].Trim(); $value = $Matches[2].Trim();" ^
    "                if ($directories.Contains($key)) { $directories[$key] = if ($value) { $value } else { $defaults[$key] }; }" ^
    "            }" ^
    "        }" ^
    "    }" ^
    "}" ^
    "$systemConfig = Join-Path $env:IOTDB_CONF 'iotdb-system.properties';" ^
    "if (Test-Path -LiteralPath $systemConfig) {" ^
    "    Read-Directories $systemConfig $cn; Read-Directories $systemConfig $dn;" ^
    "} else {" ^
    "    Read-Directories (Join-Path $env:IOTDB_CONF 'iotdb-confignode.properties') $cn;" ^
    "    Read-Directories (Join-Path $env:IOTDB_CONF 'iotdb-datanode.properties') $dn;" ^
    "}" ^
    "$targets = @();" ^
    "foreach ($directories in (@('data') + @($cn.Values) + @($dn.Values))) {" ^
    "    foreach ($directory in ($directories -split '[,;]')) {" ^
    "        $directory = $directory.Trim();" ^
    "        if (-not $directory -or $directory -match '://' -or $directory -eq 'OBJECT_STORAGE') { continue; }" ^
    "        if (-not [IO.Path]::IsPathRooted($directory)) { $directory = Join-Path $iotdbHome $directory; }" ^
    "        $directory = [IO.Path]::GetFullPath($directory).TrimEnd('\', '/');" ^
    "        $root = [IO.Path]::GetPathRoot($directory).TrimEnd('\', '/');" ^
    "        if ($directory -eq $root) { throw ('Refusing to remove a filesystem root: ' + $directory); }" ^
    "        foreach ($protected in @($iotdbHome, $env:USERPROFILE)) {" ^
    "            if (-not $protected) { continue; }" ^
    "            $protected = [IO.Path]::GetFullPath($protected).TrimEnd('\', '/');" ^
    "            if ($protected.Equals($directory, [StringComparison]::OrdinalIgnoreCase) -or $protected.StartsWith($directory + '\', [StringComparison]::OrdinalIgnoreCase)) {" ^
    "                throw ('Refusing to remove a home directory or its parent: ' + $directory);" ^
    "            }" ^
    "        }" ^
    "        $targets += $directory;" ^
    "    }" ^
    "}" ^
    "foreach ($directory in ($targets | Select-Object -Unique)) {" ^
    "    if (Test-Path -LiteralPath $directory) { Remove-Item -LiteralPath $directory -Recurse -Force; }" ^
    "}" ^
    "Write-Host 'IoTDB Edge clean done ...';"
exit /b %errorlevel%

:usage
echo Usage: %~nx0 [-f]
exit /b 1
