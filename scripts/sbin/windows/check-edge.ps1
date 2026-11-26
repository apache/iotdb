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

param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigFile
)

$ErrorActionPreference = 'Stop'

# Match the ConfigNode and DataNode defaults when a property is absent.
$ports = [ordered]@{
    cn_internal_port = 10710
    cn_consensus_port = 10720
    dn_rpc_port = 6667
    dn_internal_port = 10730
    dn_mpp_data_exchange_port = 10740
    dn_schema_region_consensus_port = 10750
    dn_data_region_consensus_port = 10760
}

if (Test-Path -LiteralPath $ConfigFile -PathType Leaf) {
    foreach ($line in Get-Content -LiteralPath $ConfigFile) {
        if ($line -cmatch '^\s*([^#!\s=]+)\s*=\s*(.*?)\s*$' -and $ports.Keys -ccontains $Matches[1]) {
            $name = $Matches[1]
            $value = $Matches[2]
            $port = 0
            if (-not [int]::TryParse($value, [ref]$port) -or $port -lt 1 -or $port -gt 65535) {
                throw "Invalid port for ${name}: $value"
            }
            $ports[$name] = $port
        }
    }
} else {
    Write-Host "Cannot find $ConfigFile; checking the default ports."
}

Write-Host 'Checking whether the ConfigNode and DataNode ports are already occupied...'
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$occupied = $false
foreach ($entry in $ports.GetEnumerator()) {
    if ($listeners.Port -contains $entry.Value) {
        Write-Host "The $($entry.Key) $($entry.Value) is already occupied."
        $occupied = $true
    }
}
if ($occupied) {
    exit 1
}
exit 0
