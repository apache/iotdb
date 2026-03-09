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

:: Initialize the variable to hold the final ips string
set "ips="
set "ip="
set "unreachable="
set "allips="
set "operate="
set "newoperating="
set "operating="
:: Loop through all arguments passed to the script
for %%a in (%*) do (
    :: Check if the current argument is -ips, if so, skip it
    if "%%a"=="-ips" (
        set "processing=1"
    ) else if defined processing (
        :: Check if the argument contains only digits (port)
        if "%%a"=="-o" (
            set "operating=1"
        ) else if defined operating (
            set "operate=%%a"
            set "operating="
        ) else (
            set "isPort=1"
            for /F "delims=0123456789" %%i in ("%%a") do (
                if not "%%i"=="" (
                    set "isPort=0"
                    set "ip=%%a"
                )
            )
            :: Append IP and port to the ips string
            if !isPort! equ 1 (
                set "ips=!ips!,!ip!:%%a"
            )
        )
    )

    if "%%a"=="-o" (
        set "newoperating=1"
    ) else if defined newoperating (
        set "operate=%%a"
        set "newoperating="
        if "%%a"=="-ips" (
           set "newprocessing=1"
        ) else if defined newprocessing (
           set "isPort=1"
           for /F "delims=0123456789" %%i in ("%%a") do (
               if not "%%i"=="" (
                 set "isPort=0"
                 set "ip=%%a"
               )
           )
            :: Append IP and port to the ips string
            if !isPort! equ 1 (
              set "ips=!ips!,!ip!:%%a"
            )
        )
    )
    if "%%a"=="--help" (
           echo Usage: health_check.bat [-ips ^<ip1^> ^<port1^> ^<port2^>,^<ip2^> ^<port3^> ^<port4^>] [-o ^<all/local/remote^>]
           exit /b 1
        )
)

set endpoints=!ips!
set "preip="
set "iplist="
set "allpreip="

set PATH="%JAVA_HOME%\bin\";%PATH%
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="


for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..\..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

pushd %~dp0..\..\..
if NOT DEFINED CONFIGNODE_HOME set CONFIGNODE_HOME=%cd%
popd

pushd %~dp0..\..\..
if NOT DEFINED DATANODE_HOME set DATANODE_HOME=%cd%
popd

if defined operate (
    if !operate! == all (
     if not defined ips (
            echo ips is requred
            exit /b 1
     )
    call :local_jdk_check
    call :local_mem_check
    call :local_dirs_check
    call :local_ports_check
    call :remote_ports_check
    call :system_settings_check
    ) else if !operate! == local (
        call :local_jdk_check
        call :local_mem_check
        call :local_dirs_check
        call :local_ports_check
        call :system_settings_check
    ) else if !operate! == remote (
         if not defined ips (
                echo ips is requred
                exit /b 1
         )
        call :remote_ports_check
    )
) else (
    if not defined ips (
        echo ips is requred
        exit /b 1
    )
    call :local_jdk_check
    call :local_mem_check
    call :local_dirs_check
    call :local_ports_check
    call :remote_ports_check
    call :system_settings_check
)


exit /b


:local_jdk_check
echo Check: Installation Environment(JDK)
echo Requirement: JDK Version ^>=1.8
echo Result: JDK Version %JAVA_VERSION%
exit /b


:local_mem_check
@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
SET IOTDB_CONF=%IOTDB_HOME%\conf
IF EXIST "%IOTDB_CONF%\windows\datanode-env.bat" (
  CALL "%IOTDB_CONF%\windows\datanode-env.bat" > nul 2>&1
) ELSE (
  echo Can't find datanode-env.bat
)

set datanode_mem= %memory_size_in_mb%


IF EXIST "%IOTDB_CONF%\windows\confignode-env.bat" (
  CALL "%IOTDB_CONF%\windows\confignode-env.bat" > nul 2>&1
) ELSE (
   echo Can't find datanode-env.bat
 )

set confignode_mem= %memory_size_in_mb%

set datanode_mem=%datanode_mem%
set confignode_mem=%confignode_mem%

set datanode_mem=%datanode_mem%
set confignode_mem=%confignode_mem%

for /f %%i in ('powershell.exe -Command "[math]::Round(%datanode_mem% / 1024, 2)"') do set datanode_mem=%%i
for /f %%i in ('powershell.exe -Command "[math]::Round(%confignode_mem% / 1024, 2)"') do set confignode_mem=%%i

echo Check: Installation Environment(Memory)
echo Requirement: Allocate sufficient memory for IoTDB

set "totalMemory="

REM Replace wmic with PowerShell for total visible memory size
for /f %%i in ('powershell -NoProfile -Command "$v=$host.Version.Major; $mem=if($v -lt 3){(Get-WmiObject Win32_OperatingSystem).TotalVisibleMemorySize} else{(Get-CimInstance -ClassName Win32_OperatingSystem).TotalVisibleMemorySize}; [math]::Round($mem/1024)"') do (
    set totalMemory=%%i
)

set /A totalMemory=!totalMemory!

if "%confignode_mem%" == "" (
    if "%datanode_mem%" == "" (
        echo Result: Total Memory %totalMemory% GB
    ) else  (
        echo Result: Total Memory %totalMemory% GB, %datanode_mem%GB allocated to IoTDB DataNode
    )
) else (
    if "%datanode_mem%" == "" (
        echo Result: Total Memory %totalMemory% GB, %confignode_mem%GB allocated to IoTDB ConfigNode
    ) else  (
        echo Result: Total Memory %totalMemory% GB, %confignode_mem%GB allocated to IoTDB ConfigNode, %datanode_mem% GB allocated to IoTDB DataNode

    )
)
endlocal
exit /b

:local_dirs_check

setlocal enabledelayedexpansion
if exist "%IOTDB_HOME%\conf\iotdb-system.properties" (
  set "properties_file=%IOTDB_HOME%\conf\iotdb-system.properties"
) ELSE (
  set "properties_file=%IOTDB_HOME%\conf\iotdb-datanode.properties"
)
for /f "usebackq tokens=1,* delims==" %%a in ("%properties_file%") do (
    if "%%a"=="dn_data_dirs" (
        set "dn_data_dirs=%%b"
    ) else if "%%a"=="dn_consensus_dir" (
        set "dn_consensus_dir=%%b"
    ) else if "%%a"=="dn_system_dir" (
        set "dn_system_dir=%%b"
    ) else if "%%a"=="dn_wal_dirs" (
        set "dn_wal_dirs=%%b"
    ) else if "%%a"=="cn_system_dir" (
        set "cn_system_dir=%%b"
    ) else if "%%a"=="cn_consensus_dir" (
        set "cn_consensus_dir=%%b"
    ) else if "%%a"=="pipe_lib_dir" (
        set "pipe_lib_dir=%%b"
    ) else if "%%a"=="udf_lib_dir" (
        set "udf_lib_dir=%%b"
    ) else if "%%a"=="trigger_lib_dir" (
        set "trigger_lib_dir=%%b"
    )
)

IF "%dn_data_dirs%"=="" (
    set "dn_data_dirs=%IOTDB_HOME%\data\datanode\data"
)
IF "%dn_consensus_dir%"=="" (
    set "dn_consensus_dir=%IOTDB_HOME%\data\datanode\consensus"
)

IF "%dn_system_dir%"=="" (
    set "dn_system_dir=%IOTDB_HOME%\data\datanode\system"
)

IF "%dn_wal_dirs%"=="" (
    set "dn_wal_dirs=%IOTDB_HOME%\data\datanode\wal"
)

IF "%cn_system_dir%"=="" (
    set "cn_system_dir=%IOTDB_HOME%\data\confignode\system"
)

IF "%cn_consensus_dir%"=="" (
    set "cn_consensus_dir=%IOTDB_HOME%\data\confignode\consensus"
)

IF "%pipe_lib_dir%"=="" (
    set "pipe_lib_dir=%IOTDB_HOME%\ext\pipe"
)

IF "%udf_lib_dir%"=="" (
    set "udf_lib_dir=%IOTDB_HOME%\ext\udf"
)

IF "%trigger_lib_dir%"=="" (
    set "trigger_lib_dir=%IOTDB_HOME%\ext\trigger"
)

set "dirs=%dn_data_dirs%,%dn_consensus_dir%,%dn_system_dir%,%dn_wal_dirs%,%cn_system_dir%,%cn_consensus_dir%,%pipe_lib_dir%,%udf_lib_dir%,%trigger_lib_dir%"

set "spacedirs=%dn_data_dirs% %dn_consensus_dir% %dn_system_dir% %dn_wal_dirs% %cn_system_dir% %cn_consensus_dir% %pipe_lib_dir% %udf_lib_dir% %trigger_lib_dir%"

for %%a in (%spacedirs%) do (
    set "string=%%a"
    for %%b in ("!string:;=" "!") do (
        set "subString=%%~b"
        for %%c in ("!subString:,=" "!") do (
            if not exist "%%c\" (
                    mkdir "%%c" > nul 2>&1
                    if errorlevel 1 (
                        if defined operation_dirs (
                            set operation_dirs=!operation_dirs!,%%c lacks write permission
                        ) else (
                            set operation_dirs=%%c lacks write permission
                        )
                    ) else (
                        if defined operation_dirs (
                            set operation_dirs=!operation_dirs!,%%c has write permission
                        ) else (
                            set operation_dirs=%%c has write permission
                        )
                    )
                ) else (
                    echo test > "%%c\tempfile.txt"
                    if EXIST "%%c\tempfile.txt" (
                        if defined operation_dirs (
                            set operation_dirs=!operation_dirs!,%%c has write permission
                        ) else (
                            set operation_dirs=%%c has write permission
                        )
                        del "%%c\tempfile.txt" >nul 2>&1

                    ) else (
                        if defined operation_dirs (
                             set operation_dirs=!operation_dirs!,%%c lacks write permission
                        ) else (
                             set operation_dirs=%%c lacks write permission
                        )
                    )
                )
        )
    )
)

echo Check: Installation Environment(Directory Access)
echo Requirement: IoTDB needs %dirs% write permission

if defined operation_dirs (
    echo Result:
    for %%s in ("%operation_dirs:,=" "%") do (
        echo %%~s
    )
) else (
     echo Directory not found
)
exit /b

:local_ports_check
IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  set DATANODE_CONFIG_FILE_PATH="%IOTDB_CONF%\iotdb-system.properties"
) ELSE IF EXIST "%IOTDB_HOME%\conf\iotdb-system.properties" (
  set DATANODE_CONFIG_FILE_PATH="%IOTDB_HOME%\conf\iotdb-system.properties"
) ELSE IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  set DATANODE_CONFIG_FILE_PATH="%IOTDB_CONF%\iotdb-datanode.properties"
) ELSE IF EXIST "%IOTDB_HOME%\conf\iotdb-datanode.properties" (
  set DATANODE_CONFIG_FILE_PATH="%IOTDB_HOME%\conf\iotdb-datanode.properties"
) ELSE (
  set DATANODE_CONFIG_FILE_PATH=
)
IF DEFINED DATANODE_CONFIG_FILE_PATH (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_rpc_port"
    %DATANODE_CONFIG_FILE_PATH%') do (
      set dn_rpc_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_internal_port"
    %DATANODE_CONFIG_FILE_PATH%') do (
      set dn_internal_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_mpp_data_exchange_port"
    %DATANODE_CONFIG_FILE_PATH%') do (
      set dn_mpp_data_exchange_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_schema_region_consensus_port"
    %DATANODE_CONFIG_FILE_PATH%') do (
      set dn_schema_region_consensus_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^dn_data_region_consensus_port"
    %DATANODE_CONFIG_FILE_PATH%') do (
      set dn_data_region_consensus_port=%%i
  )
) ELSE (
  set dn_rpc_port=6667
  set dn_internal_port=10730
  set dn_mpp_data_exchange_port=10740
  set dn_schema_region_consensus_port=10750
  set dn_data_region_consensus_port=10760
)

IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  set CONFIGNODE_CONFIG_FILE_PATH="%IOTDB_CONF%\iotdb-system.properties"
) ELSE IF EXIST "%IOTDB_HOME%\conf\iotdb-system.properties" (
  set CONFIGNODE_CONFIG_FILE_PATH="%IOTDB_HOME%\conf\iotdb-system.properties"
) ELSE IF EXIST "%IOTDB_CONF%\iotdb-system.properties" (
  set CONFIGNODE_CONFIG_FILE_PATH="%IOTDB_CONF%\iotdb-confignode.properties"
) ELSE IF EXIST "%IOTDB_HOME%\conf\iotdb-confignode.properties" (
  set CONFIGNODE_CONFIG_FILE_PATH="%IOTDB_HOME%\conf\iotdb-confignode.properties"
) ELSE (
  set CONFIGNODE_CONFIG_FILE_PATH=
)
IF DEFINED CONFIGNODE_CONFIG_FILE_PATH (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_internal_port"
    %CONFIGNODE_CONFIG_FILE_PATH%') do (
      set cn_internal_port=%%i
  )
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^cn_consensus_port"
    %CONFIGNODE_CONFIG_FILE_PATH%') do (
      set cn_consensus_port=%%i
  )
) ELSE (
  set cn_internal_port=10710
  set cn_consensus_port=10720
)

set "local_ports="
set "local_other_occupied_ports="
set "local_iotdb_occupied_ports="
set "local_free_ports="
set "operation_dirs="
set dn_rpc_port_occupied=0
set dn_internal_port_occupied=0
set dn_mpp_data_exchange_port_occupied=0
set dn_schema_region_consensus_port_occupied=0
set dn_data_region_consensus_port_occupied=0
set cn_internal_port_occupied=0
set cn_consensus_port_occupied=0
set local_ports=%dn_rpc_port% %dn_internal_port% %dn_mpp_data_exchange_port% %dn_schema_region_consensus_port% %dn_data_region_consensus_port% %cn_consensus_port% %cn_internal_port%
for /f  "tokens=1,3,7 delims=: " %%i in ('netstat /ano') do (
    if %%i==TCP (
       if %%j==%dn_rpc_port% (
         if !dn_rpc_port_occupied!==0 (
           set spid=%%k
           call :checkIfIOTDBProcess !spid! is_iotdb
           if !is_iotdb!==1 (
             set local_iotdb_occupied_ports=%dn_rpc_port% !local_iotdb_occupied_ports!
           ) else (
             set local_other_occupied_ports=%dn_rpc_port% !local_other_occupied_ports!
           )
           set dn_rpc_port_occupied=1
         )
       ) else if %%j==%dn_internal_port% (
         if !dn_internal_port_occupied!==0 (
             set spid=%%k
             call :checkIfIOTDBProcess !spid! is_iotdb
             if !is_iotdb!==1 (
                set local_iotdb_occupied_ports=%dn_internal_port% !local_iotdb_occupied_ports!
             ) else (
                set local_other_occupied_ports=%dn_internal_port% !local_other_occupied_ports!
             )
             set dn_internal_port_occupied=1
        )
       ) else if %%j==%dn_mpp_data_exchange_port% (
         if !dn_mpp_data_exchange_port_occupied!==0 (
            set spid=%%k
            call :checkIfIOTDBProcess !spid! is_iotdb
            if !is_iotdb!==1 (
                set local_iotdb_occupied_ports=%dn_mpp_data_exchange_port% !local_iotdb_occupied_ports!
            ) else (
                set local_other_occupied_ports=%dn_mpp_data_exchange_port% !local_other_occupied_ports!
            )
           set dn_mpp_data_exchange_port_occupied=1
         )
       ) else if %%j==%dn_schema_region_consensus_port% (
         if !dn_schema_region_consensus_port_occupied!==0 (
            set spid=%%k
            call :checkIfIOTDBProcess !spid! is_iotdb
            if !is_iotdb!==1 (
              set local_iotdb_occupied_ports=%dn_schema_region_consensus_port% !local_iotdb_occupied_ports!
            ) else (
              set local_other_occupied_ports=%dn_schema_region_consensus_port% !local_other_occupied_ports!
            )
           set dn_schema_region_consensus_port_occupied=1
         )
       ) else if %%j==%dn_data_region_consensus_port% (
         if !dn_data_region_consensus_port_occupied!==0 (
           set spid=%%k
           call :checkIfIOTDBProcess !spid! is_iotdb
           if !is_iotdb!==1 (
              set local_iotdb_occupied_ports=%dn_data_region_consensus_port% !local_iotdb_occupied_ports!

           ) else (
              set local_other_occupied_ports=%dn_data_region_consensus_port% !local_other_occupied_ports!
           )
           set dn_data_region_consensus_port_occupied=1
         )
       ) else if %%j==%cn_internal_port% (
         if !cn_internal_port_occupied!==0 (
             set spid=%%k
             call :checkIfIOTDBProcess !spid! is_iotdb
             if !is_iotdb!==1 (
              set local_iotdb_occupied_ports=%cn_internal_port% !local_iotdb_occupied_ports!
             ) else (
              set local_other_occupied_ports=%cn_internal_port% !local_other_occupied_ports!
             )
             set cn_internal_port_occupied=1
         )
       ) else if %%j==%cn_consensus_port% (
         if !cn_consensus_port_occupied!==0 (
           set spid=%%k
           call :checkIfIOTDBProcess !spid! is_iotdb
           if !is_iotdb!==1 (
               set local_iotdb_occupied_ports=%cn_consensus_port% !local_iotdb_occupied_ports!
           ) else (
               set local_other_occupied_ports=%cn_consensus_port% !local_other_occupied_ports!
           )
           set cn_consensus_port_occupied=1
         )
       )
    )
)

if !dn_rpc_port_occupied!==0 (
    set local_free_ports=%dn_rpc_port% !local_free_ports!
)
if !dn_internal_port_occupied!==0 (
    set local_free_ports=%dn_internal_port% !local_free_ports!
)
if !dn_mpp_data_exchange_port_occupied!==0 (
    set local_free_ports=%dn_mpp_data_exchange_port% !local_free_ports!
)
if !dn_schema_region_consensus_port_occupied!==0 (
    set local_free_ports=%dn_schema_region_consensus_port% !local_free_ports!
)
if !dn_data_region_consensus_port_occupied!==0 (
    set local_free_ports=%dn_data_region_consensus_port% !local_free_ports!
)
if !cn_internal_port_occupied!==0 (
    set local_free_ports=%cn_internal_port% !local_free_ports!
)
if !cn_consensus_port_occupied!==0 (
    set local_free_ports=%cn_consensus_port% !local_free_ports!
)

echo Check: Network(Local Port)
echo Requirement: Port !local_ports! is not occupied
echo Result:

if defined local_other_occupied_ports (
    echo Port !local_other_occupied_ports! occupied by other programs
)

if defined local_iotdb_occupied_ports (
    echo Port !local_iotdb_occupied_ports! is occupied by IoTDB
)

if defined local_free_ports (
    echo Port !local_free_ports! is free
)
exit /b

:remote_ports_check
for %%e in ("%endpoints:,=" "%") do (
    set counter=0
    for /f "tokens=1,2 delims=:" %%i in ("%%~e") do (
        set "ip=%%i"
        set "port=%%j"
        if "!allpreip!" == "!ip!" (
          set "iplist=!iplist! !port!"
        ) else (
          set allpreip=!ip!
          if !counter! EQU 0 (
           set /a counter+=1
           if defined iplist (
           set "iplist=!iplist!,!ip!:!port!"
           ) else (
           set "iplist=!ip!:!port!"
           )
          ) else (
           set "iplist=!iplist!,!ip!:!port!"
          )
        )

        for /f %%r in ('powershell -command "Test-NetConnection -ComputerName !ip! -Port !port! -InformationLevel Quiet"') do (
                    set "result=%%r"
                )
            if /i "!result!" EQU "TRUE" (
@REM                 echo Info: The !port! port on !ip! is reachable.
            ) else (
@REM                 echo Error: The !port! port on !ip! is unreachable.
                if "!preip!" == "!ip!" (
                    set "unreachable=!unreachable!-!port!"
                ) else (
                    set preip=!ip!
                    if "!unreachable!" == "" (
                      set "unreachable=IP:!ip!,Ports:!port!"
                    ) else (
                      set "unreachable=!unreachable!##IP:!ip!,Ports:!port!"
                    )
                )
            )
    )
)

setlocal enabledelayedexpansion
echo Check: Network(Remote Port Connectivity)
echo Requirement: !iplist! need to be accessible
echo Result:
if  defined unreachable (
    echo The following server ports are not sure, please double check:
    for %%a in ("%unreachable:##=" "%") do (
        set "segment=%%~a"
        set "segment=!segment:-= !"
        echo !segment!
    )
 ) else (
    echo All ports are accessible
 )
endlocal
exit /b

:system_settings_check
setlocal enabledelayedexpansion
echo Check: System Settings(Swap)
echo Requirement: disabled

set "check_swap_ps_cmd=$v=$host.Version.Major; if($v -lt 3) { $sys=Get-WmiObject Win32_ComputerSystem; if($sys.AutomaticManagedPagefile) { 'Enabled' } else { $page=Get-WmiObject Win32_PageFileSetting -EA 0; if(!$page) { 'Disabled' } else { $page | %% { if($_.InitialSize -ne 0 -or $_.MaximumSize -ne 0) { 'Enabled'; exit } } 'Disabled' } } } else { $sys=Get-CimInstance Win32_ComputerSystem; if($sys.AutomaticManagedPagefile) { 'Enabled' } else { $page=Get-CimInstance Win32_PageFileSetting -EA 0; if(!$page) { 'Disabled' } else { $page | %% { if($_.InitialSize -ne 0 -or $_.MaximumSize -ne 0) { 'Enabled'; exit } } 'Disabled' } } }"
for /f "delims=" %%a in ('powershell -NoProfile -Command "%check_swap_ps_cmd%"') do set "result=%%a"

echo Result: !result!
endlocal
exit /b

:checkIfIOTDBProcess
setlocal

set "pid_to_check=%~1"
set "is_iotdb=0"

for /f "delims=" %%i in ('powershell -NoProfile -Command "$v=$host.Version.Major; if($v -lt 3) { (Get-WmiObject Win32_Process -Filter \"ProcessId='%pid_to_check%'\").CommandLine } else { (Get-CimInstance Win32_Process -Filter \"ProcessId='%pid_to_check%'\").CommandLine }"') do set "command_line=%%i"

echo %command_line% | findstr /i /c:"iotdb" >nul && set is_iotdb=1
endlocal & set "is_iotdb=%is_iotdb%"
exit /b