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
param([string]$BuildDirectory = 'iotdb-client/odbc/target/build', [int]$Parallel = 4)
$ErrorActionPreference = 'Stop'
if (-not [Environment]::Is64BitProcess) { throw 'Run this script in 64-bit PowerShell' }
$repository = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $repository
try {
    $version = ([xml](Get-Content iotdb-client/pom.xml)).project.parent.version
    $commit = git rev-parse HEAD
    if ($LASTEXITCODE) { throw 'Cannot determine source commit' }
    $python = python -c 'import sys; assert sys.version_info >= (3, 8); print(sys.executable)'
    if ($LASTEXITCODE -or -not $python) { throw 'Python 3.8+ is required for ABI and release tests' }
    cmake -S iotdb-client/odbc -B $BuildDirectory -A x64 -DCMAKE_BUILD_TYPE=Release "-DPython3_EXECUTABLE=$python" "-DODBC_PACKAGE_VERSION=$version" -DODBC_PACKAGE_CLASSIFIER=windows-x86_64 -DODBC_BUILD_TESTS=ON -DODBC_ENABLE_LOGGING=OFF
    if ($LASTEXITCODE) { throw 'ODBC configure failed' }
    cmake --build $BuildDirectory --config Release --parallel $Parallel
    if ($LASTEXITCODE) { throw 'ODBC build failed' }
    ctest --test-dir $BuildDirectory -C Release --output-on-failure --no-tests=error
    if ($LASTEXITCODE) { throw 'ODBC tests failed' }
    & iotdb-client/odbc/test/check-windows-dependencies.ps1 "$BuildDirectory/Release/apache_iotdb_odbc.dll"
    Push-Location $BuildDirectory
    try {
        cpack -C Release
        if ($LASTEXITCODE) { throw 'ODBC packaging failed' }
    } finally { Pop-Location }
    $package = "$BuildDirectory/iotdb-odbc-$version-windows-x86_64.zip"
    & $python iotdb-client/odbc/test/verify_release.py $package --commit $commit --platform windows-x86_64
    if ($LASTEXITCODE) { throw 'ODBC release verification failed' }
} finally { Pop-Location }
