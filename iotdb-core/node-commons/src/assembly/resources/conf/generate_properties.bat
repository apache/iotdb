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
setlocal enabledelayedexpansion

set "source_file=src\assembly\resources\conf\iotdb-system.properties"
set "target_template_file=target\conf\iotdb-system.properties.template"
set "target_properties_file=target\conf\iotdb-system.properties"

if exist "%target_template_file%" (
    del "%target_template_file%"
)

if exist "%target_properties_file%" (
    del "%target_properties_file%"
)

mkdir "%target_template_file%\.."

copy "%source_file%" "%target_template_file%"

echo # >> "%target_properties_file%"
echo # Licensed to the Apache Software Foundation (ASF) under one >> "%target_properties_file%"
echo # or more contributor license agreements.  See the NOTICE file >> "%target_properties_file%"
echo # distributed with this work for additional information >> "%target_properties_file%"
echo # regarding copyright ownership.  The ASF licenses this file >> "%target_properties_file%"
echo # to you under the Apache License, Version 2.0 (the >> "%target_properties_file%"
echo # "License"); you may not use this file except in compliance >> "%target_properties_file%"
echo # with the License.  You may obtain a copy of the License at >> "%target_properties_file%"
echo # >> "%target_properties_file%"
echo #     http://www.apache.org/licenses/LICENSE-2.0 >> "%target_properties_file%"
echo # >> "%target_properties_file%"
echo # Unless required by applicable law or agreed to in writing, >> "%target_properties_file%"
echo # software distributed under the License is distributed on an >> "%target_properties_file%"
echo # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY >> "%target_properties_file%"
echo # KIND, either express or implied.  See the License for the >> "%target_properties_file%"
echo # specific language governing permissions and limitations >> "%target_properties_file%"
echo # under the License. >> "%target_properties_file%"
echo # >> "%target_properties_file%"

for /f "usebackq tokens=*" %%i in ("%target_template_file%") do (
    set "line=%%i"
    setlocal enabledelayedexpansion
    if not "!line!"=="" if "!line:~0,1!" NEQ "#" (
        echo !line!>>"%target_properties_file%"
    )
    endlocal
)
powershell -Command "(Get-Content '%target_properties_file%') -join \"`n\" | Set-Content -NoNewline '%target_properties_file%'"
