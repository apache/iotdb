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
param([Parameter(Mandatory = $true)][string]$Driver)
$ErrorActionPreference = 'Stop'
if (-not $env:IOTDB_ODBC_CONNECTION) { throw 'Set IOTDB_ODBC_CONNECTION to a disposable table-model server' }
if (-not [Environment]::Is64BitProcess) { throw 'The release driver requires a 64-bit consumer' }

$dsn = 'IoTDB-' + $PID + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 12)
$dsnPath = 'Software\ODBC\ODBC.INI\' + $dsn
try {
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey(
        $dsnPath, $true, [Microsoft.Win32.RegistryOptions]::None)
} catch {
    # ERROR_CHILD_MUST_BE_VOLATILE: temporary user hives require volatile keys.
    if (($_.Exception.InnerException.HResult -band 0xffff) -ne 1021) { throw }
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey(
        $dsnPath, $true, [Microsoft.Win32.RegistryOptions]::Volatile)
}
$key.SetValue('Driver', (Resolve-Path -LiteralPath $Driver).Path)
$key.Close()
$options = 'DSN=' + $dsn + ';' + $env:IOTDB_ODBC_CONNECTION
$connection = New-Object System.Data.Odbc.OdbcConnection($options)
$database = 'odbc_consumer_' + [Guid]::NewGuid().ToString('N')
$ado = $null
$recordset = $null
$created = $false

function Assert-True($Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}
function Execute([string]$Sql) {
    $command = $connection.CreateCommand()
    try { $command.CommandText = $Sql; [void]$command.ExecuteNonQuery() }
    finally { $command.Dispose() }
}
try {
    $connection.Open()
    Execute "CREATE DATABASE $database"
    $created = $true
    Execute "CREATE TABLE $database.samples (device STRING TAG, val INT32 FIELD, txt STRING FIELD, day DATE FIELD, active BOOLEAN FIELD)"
    # ChangeDatabase calls SQLSetConnectAttrW; underscores must be accepted.
    $connection.ChangeDatabase($database)
    Execute "INSERT INTO samples(time,device,val,txt,day,active) VALUES (1,'设备',1,'中文','2024-02-29',true),(2,'设备',2,null,'2024-03-01',false),(3,'设备',3,'','2024-03-02',true),(4,'设备',4,'last','2024-03-03',true)"
    $command = $connection.CreateCommand()
    try {
        $command.CommandText = 'INSERT INTO samples(time,device,val,txt,day,active) VALUES (?,?,?,?,?,?)'
        [void]$command.Parameters.Add('time', [System.Data.Odbc.OdbcType]::BigInt)
        [void]$command.Parameters.Add('device', [System.Data.Odbc.OdbcType]::NVarChar, 16)
        [void]$command.Parameters.Add('val', [System.Data.Odbc.OdbcType]::Int)
        [void]$command.Parameters.Add('txt', [System.Data.Odbc.OdbcType]::NVarChar, 12000)
        [void]$command.Parameters.Add('day', [System.Data.Odbc.OdbcType]::Date)
        [void]$command.Parameters.Add('active', [System.Data.Odbc.OdbcType]::Bit)
        $longText = '中文😀' * 1500
        $values = [object[]]@(5L, '设备', 5, $longText, [DateTime]'2024-03-04', $true)
        for ($i = 0; $i -lt $values.Length; $i++) { $command.Parameters[$i].Value = $values[$i] }
        [void]$command.ExecuteNonQuery()
        $command.Parameters.Clear()
        $command.CommandText = 'SELECT time,device,val,txt,day,active FROM samples ORDER BY time'
        $reader = $command.ExecuteReader()
        try {
            Assert-True ($reader.FieldCount -eq 6) 'The explicit time projection was lost'
            $count = 0
            while ($reader.Read()) {
                $count++
                Assert-True ($reader.GetInt64(0) -eq $count) 'Time or batch order mismatch'
                Assert-True ($reader.GetString(1) -eq '设备') 'Unicode device mismatch'
                Assert-True ($reader.GetInt32(2) -eq $count) 'Integer mismatch'
                if ($count -eq 1) { Assert-True ($reader.GetString(3) -eq '中文') 'Unicode data mismatch' }
                if ($count -eq 2) { Assert-True ($reader.IsDBNull(3)) 'NULL mismatch' }
                if ($count -eq 3) { Assert-True ($reader.GetString(3) -eq '') 'Empty string mismatch' }
                if ($count -eq 5) { Assert-True ($reader.GetString(3) -eq $longText) 'Chunked Unicode data mismatch' }
                Assert-True ($reader.GetDateTime(4).Year -eq 2024) 'DATE conversion mismatch'
                Assert-True ($reader.GetBoolean(5) -eq ($count -ne 2)) 'Boolean conversion mismatch'
            }
            Assert-True ($count -eq 5) 'Rows were lost across Session batches'
        } finally { $reader.Dispose() }
        $command.CommandText = 'SELECT txt FROM samples WHERE val < 0'
        $reader = $command.ExecuteReader()
        try { Assert-True (-not $reader.Read()) 'Empty query returned a row' }
        finally { $reader.Dispose() }
    } finally { $command.Dispose() }

    # Casting a PowerShell array containing $null to string[] changes nulls to
    # empty strings, which have different ODBC catalog semantics.
    $tableRestrictions = [string[]]::new(3)
    $tableRestrictions[0] = $database
    $tableRestrictions[2] = 'samples'
    $tables = $connection.GetSchema('Tables', $tableRestrictions)
    Assert-True ($tables.Rows.Count -eq 1) '.NET table browsing mismatch'
    $columnRestrictions = [string[]]::new(4)
    $columnRestrictions[0] = $database
    $columnRestrictions[2] = 'samples'
    $columns = $connection.GetSchema('Columns', $columnRestrictions)
    Assert-True ($columns.Rows.Count -eq 6) ('.NET column browsing expected 6 rows, got ' + $columns.Rows.Count)
    $columnRestrictions[2] = 'missing'
    $none = $connection.GetSchema('Columns', $columnRestrictions)
    Assert-True ($none.Rows.Count -eq 0) 'An empty catalog search must succeed'
    Write-Output 'Windows .NET ODBC checks passed'

    # MSDASQL/ADO is also used by Office import workflows. In particular, it
    # requests legacy column attributes and chooses C types from SQL metadata.
    $ado = New-Object -ComObject ADODB.Connection
    $ado.Open('Provider=MSDASQL;' + $options)
    $recordset = $ado.Execute("SELECT time,txt,day FROM $database.samples WHERE val = 1")
    Write-Output 'ADO query opened'
    Assert-True (-not $recordset.EOF) 'ADO query returned no data'
    Assert-True ($recordset.Fields.Item(1).Value -eq '中文') 'ADO Unicode import mismatch'
    Assert-True ($recordset.Fields.Item(2).Value.Year -eq 2024) 'ADO DATE import mismatch'
    $recordset.Close()
    Write-Output 'ADO data checks passed'
    [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($recordset)
    $recordset = $null
    $recordset = $ado.OpenSchema(4, [object[]]@($database, [DBNull]::Value, 'samples', [DBNull]::Value))
    $count = 0
    while (-not $recordset.EOF) { $count++; $recordset.MoveNext() }
    Assert-True ($count -eq 6) 'ADO column browsing mismatch'
    $tree = New-Object System.Data.Odbc.OdbcConnection(
        $options + ';ISTABLEMODEL=0;DATABASE=;')
    $treeDatabase = 'root.' + $database
    $treeCreated = $false
    try {
        $tree.Open()
        $command = $tree.CreateCommand()
        try {
            $command.CommandText = "CREATE DATABASE $treeDatabase"
            [void]$command.ExecuteNonQuery()
            $treeCreated = $true
            $command.CommandText = "INSERT INTO $treeDatabase.d(time,v,txt) VALUES(1,1,'中文'),(2,2,'two'),(3,3,'three')"
            [void]$command.ExecuteNonQuery()
            $tableRestrictions[0] = $treeDatabase
            $tableRestrictions[2] = 'd'
            Assert-True ($tree.GetSchema('Tables', $tableRestrictions).Rows.Count -eq 1) 'Tree table browsing mismatch'
            $columnRestrictions[0] = $treeDatabase
            $columnRestrictions[2] = 'd'
            Assert-True ($tree.GetSchema('Columns', $columnRestrictions).Rows.Count -eq 3) 'Tree column browsing mismatch'
        } finally { $command.Dispose() }
    } finally {
        if ($treeCreated) {
            $command = $tree.CreateCommand()
            try {
                $command.CommandText = "DELETE DATABASE $treeDatabase"
                [void]$command.ExecuteNonQuery()
            } finally { $command.Dispose() }
        }
        $tree.Dispose()
    }
    Write-Output 'Windows .NET ODBC and MSDASQL/ADO consumer checks passed'
} finally {
    if ($recordset) {
        if ($recordset.State) { $recordset.Close() }
        [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($recordset)
    }
    if ($ado) {
        if ($ado.State) { $ado.Close() }
        [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($ado)
    }
    try {
        if ($created -and $connection.State -eq 'Open') {
            $connection.ChangeDatabase('information_schema')
            Execute "DROP DATABASE $database"
        }
    } finally {
        $connection.Dispose()
        [Microsoft.Win32.Registry]::CurrentUser.DeleteSubKeyTree($dsnPath, $false)
    }
}
