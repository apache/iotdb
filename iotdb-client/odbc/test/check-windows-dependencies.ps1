# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
param([Parameter(Mandatory=$true)][string]$Driver)
$ErrorActionPreference = 'Stop'
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
$vs = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
$dumpbin = Get-ChildItem "$vs\VC\Tools\MSVC\*\bin\Hostx64\x64\dumpbin.exe" | Sort-Object FullName | Select-Object -Last 1
if (-not $dumpbin) { throw 'dumpbin not found' }
$imports = & $dumpbin.FullName /dependents $Driver
if ($LASTEXITCODE) { throw 'Cannot inspect driver imports' }
$allowed = '^(KERNEL32|ADVAPI32|USER32|GDI32|SHELL32|OLE32|OLEAUT32|WS2_32|CRYPT32|BCRYPT|SECUR32|NORMALIZ|NTDLL|ODBC32|ODBCCP32|COMDLG32|COMCTL32|RPCRT4|VERSION)\.dll$'
$dependencies = $imports | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^[A-Za-z0-9_.-]+\.dll$' }
if (-not $dependencies) { throw 'Cannot determine driver imports' }
foreach ($dependency in $dependencies) {
    if ($dependency -notmatch $allowed -and $dependency -notmatch '^api-ms-win-.*\.dll$') {
        throw "Unexpected runtime dependency: $dependency"
    }
}
$headers = & $dumpbin.FullName /headers $Driver
if ($LASTEXITCODE -or -not ($headers -match '8664 machine')) { throw 'Expected an x64 driver' }
Write-Host 'Windows x64 single-library dependency check passed'
