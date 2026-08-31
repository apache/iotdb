#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$testRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('iotdb-edge-windows-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $testRoot | Out-Null
$javaStub = Join-Path $testRoot 'java.exe'
$script:caseCount = 0
$portNames = @(
    'cn_internal_port', 'cn_consensus_port', 'dn_rpc_port', 'dn_internal_port',
    'dn_mpp_data_exchange_port', 'dn_schema_region_consensus_port', 'dn_data_region_consensus_port'
)

function Get-FreePorts {
    $result = @{}
    $reservations = @()
    try {
        foreach ($name in $portNames) {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
            $listener.Start()
            $reservations += $listener
            $result[$name] = $listener.LocalEndpoint.Port
        }
        return $result
    } finally {
        foreach ($listener in $reservations) {
            $listener.Stop()
        }
    }
}

function Invoke-EdgeLauncher {
    param(
        [System.Collections.IDictionary]$Ports,
        [string]$Locale = '',
        [string[]]$ExtraLines = @(),
        [switch]$MissingConfig
    )

    $script:caseCount++
    $caseDir = Join-Path $testRoot "case-$script:caseCount"
    $edgeHome = Join-Path $caseDir 'edge installation'
    $configDir = Join-Path $caseDir 'custom configuration'
    $launcherDir = Join-Path $edgeHome 'sbin/windows'
    $envDir = Join-Path $configDir 'windows'
    $javaHome = Join-Path $caseDir 'fake java'
    $javaBin = Join-Path $javaHome 'bin'
    New-Item -ItemType Directory -Path $launcherDir, $envDir, $javaBin -Force | Out-Null
    foreach ($file in @('start-edge.bat', 'check-edge.ps1')) {
        Copy-Item -LiteralPath (Join-Path $repositoryRoot "scripts/sbin/windows/$file") -Destination $launcherDir
    }
    Copy-Item -LiteralPath (Join-Path $repositoryRoot 'scripts/conf/windows/edge-env.bat') -Destination $envDir
    $common = Get-Content -LiteralPath (Join-Path $repositoryRoot 'scripts/conf/windows/iotdb-common.bat') -Raw
    Set-Content -LiteralPath (Join-Path $envDir 'iotdb-common.bat') -Value $common.Replace('@tsfile.locale.opt@', $Locale) -Encoding ASCII
    if (-not $MissingConfig) {
        $lines = @($ExtraLines)
        foreach ($key in $Ports.Keys) {
            $lines += "  $key = $($Ports[$key])  "
        }
        Set-Content -LiteralPath (Join-Path $configDir 'iotdb-system.properties') -Value $lines -Encoding ASCII
    }

    Copy-Item -LiteralPath $javaStub -Destination $javaBin
    $argsFile = Join-Path $caseDir 'java-arguments.txt'
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $env:ComSpec
    $startInfo.Arguments = '/d /c call "' + (Join-Path $launcherDir 'start-edge.bat') + '"'
    $startInfo.WorkingDirectory = $caseDir
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.RedirectStandardInput = $true
    $startInfo.EnvironmentVariables['IOTDB_HOME'] = $edgeHome
    $startInfo.EnvironmentVariables['IOTDB_CONF'] = $configDir
    $startInfo.EnvironmentVariables['JAVA_HOME'] = $javaHome
    $startInfo.EnvironmentVariables['EDGE_TEST_JAVA_ARGS'] = $argsFile
    $startInfo.EnvironmentVariables['IOTDB_JMX_OPTS'] = ''
    $startInfo.EnvironmentVariables['TSFILE_LOCALE_JVM_OPT'] = '-Dtsfile.locale=stale'
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        [void]$process.Start()
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $process.StandardInput.Close()
        if (-not $process.WaitForExit(30000)) {
            $process.Kill()
            throw 'Timed out running the Edge Windows launcher'
        }
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            Output = $stdout.Result + $stderr.Result
            JavaInvoked = Test-Path -LiteralPath $argsFile
            Arguments = if (Test-Path -LiteralPath $argsFile) { Get-Content -LiteralPath $argsFile -Raw } else { '' }
        }
    } finally {
        $process.Dispose()
    }
}

function Assert-LaunchResult {
    param($Result, [bool]$ShouldLaunch, [string]$Name)
    if ($ShouldLaunch) {
        if ($Result.ExitCode -ne 0 -or -not $Result.JavaInvoked) {
            throw "${Name}: expected a successful Java launch. $($Result.Output)"
        }
        if ($Result.Arguments -notmatch 'org\.apache\.iotdb\.edge\.EdgeNode') {
            throw "${Name}: the Edge main class was not invoked"
        }
    } elseif ($Result.ExitCode -eq 0 -or $Result.JavaInvoked) {
        throw "${Name}: expected rejection before starting Java. $($Result.Output)"
    }
    Write-Host "PASS: $Name"
}

try {
    # Use an executable stub so batch control flow and argument quoting match a real JVM.
    Add-Type -OutputAssembly $javaStub -OutputType ConsoleApplication -TypeDefinition @'
using System;
using System.IO;

internal static class EdgeJavaStub
{
    private static int Main(string[] args)
    {
        if (args.Length == 1 && args[0] == "-fullversion")
        {
            Console.Error.WriteLine("openjdk full version \"17.0.5+8\"");
            return 0;
        }
        File.WriteAllLines(Environment.GetEnvironmentVariable("EDGE_TEST_JAVA_ARGS"), args);
        return 0;
    }
}
'@

    $freePorts = Get-FreePorts
    foreach ($locale in @('', '-Dtsfile.locale=zh')) {
        $result = Invoke-EdgeLauncher -Ports $freePorts -Locale $locale
        Assert-LaunchResult $result $true "locale '$locale', custom config and paths with spaces"
        if ($locale -eq '') {
            if ($result.Arguments -match '-Dtsfile\.locale=') {
                throw 'The default package inherited a stale TsFile locale option'
            }
        } elseif ([regex]::Matches($result.Arguments, '-Dtsfile\.locale=zh').Count -ne 1 -or $result.Arguments -match 'tsfile\.locale=stale') {
            throw 'The zh package did not apply exactly one filtered TsFile locale option'
        }
    }

    foreach ($name in $portNames) {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
        $listener.Start()
        try {
            $ports = $freePorts.Clone()
            $ports[$name] = $listener.LocalEndpoint.Port
            $result = Invoke-EdgeLauncher -Ports $ports
            Assert-LaunchResult $result $false "occupied $name"
            if ($result.Output -notmatch "The $name $($ports[$name]) is already occupied") {
                throw "The occupied port was not identified correctly: $($result.Output)"
            }
        } finally {
            $listener.Stop()
        }
    }

    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        $result = Invoke-EdgeLauncher -Ports $freePorts -ExtraLines @(
            "# cn_internal_port=$($listener.LocalEndpoint.Port)",
            "! dn_rpc_port=$($listener.LocalEndpoint.Port)",
            "cn_internal_port=$($listener.LocalEndpoint.Port)"
        )
        Assert-LaunchResult $result $true 'comments, whitespace and the last value of a repeated property'
    } finally {
        $listener.Stop()
    }

    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 10720)
    $listener.Start()
    try {
        $ports = $freePorts.Clone()
        $ports.Remove('cn_consensus_port')
        $result = Invoke-EdgeLauncher -Ports $ports
        Assert-LaunchResult $result $false 'default port for an omitted property'
        $result = Invoke-EdgeLauncher -Ports @{} -MissingConfig
        Assert-LaunchResult $result $false 'default ports when the configuration file is absent'
        if ($result.Output -notmatch 'cn_consensus_port 10720 is already occupied') {
            throw 'The missing-file fallback did not check the default ConfigNode consensus port'
        }
    } finally {
        $listener.Stop()
    }

    foreach ($value in @('0', '65536', 'not-a-port')) {
        $ports = $freePorts.Clone()
        $ports['dn_rpc_port'] = $value
        $result = Invoke-EdgeLauncher -Ports $ports
        Assert-LaunchResult $result $false "invalid port '$value'"
    }
    Write-Host "All $script:caseCount Windows Edge launcher cases passed."
} finally {
    Remove-Item -LiteralPath $testRoot -Recurse -Force
}
