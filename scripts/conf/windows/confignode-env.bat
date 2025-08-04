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

@REM You can set datanode memory size, example '2G' or '2048M'
set MEMORY_SIZE=

@REM true or false
@REM DO NOT FORGET TO MODIFY THE PASSWORD FOR SECURITY (%CONFIGNODE_CONF%\jmx.password and %{CONFIGNODE_CONF%\jmx.access)
set JMX_LOCAL="true"
set JMX_PORT="32000"
@REM only take effect when the jmx_local=false
@REM You need to change this IP as a public IP if you want to remotely connect IoTDB ConfigNode by JMX.
@REM  0.0.0.0 is not allowed
set JMX_IP="127.0.0.1"

if %JMX_LOCAL% == "false" (
  echo "setting remote JMX..."
  @REM you may have no permission to run chmod. If so, contact your system administrator.
  set CONFIGNODE_JMX_OPTS=-Dcom.sun.management.jmxremote^
  -Dcom.sun.management.jmxremote.port=%JMX_PORT%^
  -Dcom.sun.management.jmxremote.rmi.port=%JMX_PORT%^
  -Djava.rmi.server.randomIDs=true^
  -Dcom.sun.management.jmxremote.ssl=false^
  -Dcom.sun.management.jmxremote.authenticate=false^
  -Dcom.sun.management.jmxremote.password.file="%CONFIGNODE_CONF%\jmx.password"^
  -Dcom.sun.management.jmxremote.access.file="%CONFIGNODE_CONF%\jmx.access"^
  -Djava.rmi.server.hostname=%JMX_IP%
) else (
  echo "setting local JMX..."
)

set CONFIGNODE_JMX_OPTS=%CONFIGNODE_JMX_OPTS% -Diotdb.jmx.local=%JMX_LOCAL%

REM Replace wmic with PowerShell for CPU core count
for /f %%b in ('powershell -NoProfile -Command "$v=$host.Version.Major; if($v -lt 3) {(Get-WmiObject Win32_Processor).NumberOfCores} else {(Get-CimInstance -ClassName Win32_Processor).NumberOfCores}"') do (
    set system_cpu_cores=%%b
)

if %system_cpu_cores% LSS 1 set system_cpu_cores=1

REM Replace wmic with PowerShell for total physical memory
for /f %%b in ('powershell -NoProfile -Command "$v=$host.Version.Major; $mem=if($v -lt 3){(Get-WmiObject Win32_ComputerSystem).TotalPhysicalMemory} else{(Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory}; [math]::Round($mem/1048576)"') do (
    set system_memory_in_mb=%%b
)

REM Remove VBScript usage for memory calculation
set system_memory_in_mb=%system_memory_in_mb:,=%

@REM suggest using memory, system memory 3 / 10
set /a suggest_=%system_memory_in_mb%/10*3

if "%MEMORY_SIZE%"=="" (
  set /a memory_size_in_mb=%suggest_%
) else (
  if "%MEMORY_SIZE:~-1%"=="M" (
    set /a memory_size_in_mb=%MEMORY_SIZE:~0,-1%
  ) else if "%MEMORY_SIZE:~-1%"=="G" (
    set /a memory_size_in_mb=%MEMORY_SIZE:~0,-1%*1024
  ) else (
    echo "Invalid format of MEMORY_SIZE, please use the format like 2048M or 2G."
    exit /b 1
  )
)

@REM set on heap memory size
@REM when memory_size_in_mb is less than 4 * 1024, we will set on heap memory size to memory_size_in_mb / 4 * 3
@REM when memory_size_in_mb is greater than 4 * 1024 and less than 16 * 1024, we will set on heap memory size to memory_size_in_mb / 5 * 4
@REM when memory_size_in_mb is greater than 16 * 1024 and less than 128 * 1024, we will set on heap memory size to memory_size_in_mb / 8 * 7
@REM when memory_size_in_mb is greater than 128 * 1024, we will set on heap memory size to memory_size_in_mb - 16 * 1024
if %memory_size_in_mb% LSS 4096 (
  set /a on_heap_memory_size_in_mb=%memory_size_in_mb%/4*3
) else if %memory_size_in_mb% LSS 16384 (
  set /a on_heap_memory_size_in_mb=%memory_size_in_mb%/5*4
) else if %memory_size_in_mb% LSS 131072 (
  set /a on_heap_memory_size_in_mb=%memory_size_in_mb%/8*7
) else (
  set /a on_heap_memory_size_in_mb=%memory_size_in_mb%-16384
)
set /a off_heap_memory_size_in_mb=%memory_size_in_mb%-%on_heap_memory_size_in_mb%

set ON_HEAP_MEMORY=%on_heap_memory_size_in_mb%M
set OFF_HEAP_MEMORY=%off_heap_memory_size_in_mb%M

set IOTDB_ALLOW_HEAP_DUMP="true"

@REM on heap memory size
@REM set ON_HEAP_MEMORY=2G
@REM off heap memory size
@REM set OFF_HEAP_MEMORY=512M

if "%OFF_HEAP_MEMORY:~-1%"=="M" (
    set /a off_heap_memory_size_in_mb=%OFF_HEAP_MEMORY:~0,-1%
  ) else if "%OFF_HEAP_MEMORY:~-1%"=="G" (
    set /a off_heap_memory_size_in_mb=%OFF_HEAP_MEMORY:~0,-1%*1024
  ) 

@REM threads number of io
set IO_THREADS_NUMBER=100
@REM Max cached buffer size, Note: unit can only be B!
@REM which equals OFF_HEAP_MEMORY / IO_THREADS_NUMBER
set /a MAX_CACHED_BUFFER_SIZE=%off_heap_memory_size_in_mb%/%IO_THREADS_NUMBER%*1024*1024

set CONFIGNODE_HEAP_OPTS=-Xmx%ON_HEAP_MEMORY% -Xms%ON_HEAP_MEMORY%
set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -XX:MaxDirectMemorySize=%OFF_HEAP_MEMORY%
set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -Djdk.nio.maxCachedBufferSize=%MAX_CACHED_BUFFER_SIZE%
set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -XX:+CrashOnOutOfMemoryError

@REM if you want to dump the heap memory while OOM happening, you can use the following command, remember to replace /tmp/heapdump.hprof with your own file path and the folder where this file is located needs to be created in advance
@REM CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=\tmp\confignode_heapdump.hprof

@REM You can put your env variable here
@REM set JAVA_HOME=%JAVA_HOME%

@REM set gc log.
IF "%1" equ "printgc" (
	IF "%JAVA_VERSION%" == "8" (
	    md "%CONFIGNODE_HOME%\logs"
		set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -Xloggc:"%CONFIGNODE_HOME%\logs\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails  -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
	) ELSE (
		md "%CONFIGNODE_HOME%\logs"
		set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS%  -Xlog:gc=info,heap*=trace,age*=debug,safepoint=info,promotion*=trace:file="%CONFIGNODE_HOME%\logs\gc.log":time,uptime,pid,tid,level:filecount=10,filesize=10485760
	)
)

@REM Add args for Java 11 and above, due to [JEP 396: Strongly Encapsulate JDK Internals by Default] (https://openjdk.java.net/jeps/396)
IF "%JAVA_VERSION%" == "8" (
    set ILLEGAL_ACCESS_PARAMS=
) ELSE (
    set ILLEGAL_ACCESS_PARAMS=--add-opens=java.base/java.util.concurrent=ALL-UNNAMED^
     --add-opens=java.base/java.lang=ALL-UNNAMED^
     --add-opens=java.base/java.util=ALL-UNNAMED^
     --add-opens=java.base/java.nio=ALL-UNNAMED^
     --add-opens=java.base/java.io=ALL-UNNAMED^
     --add-opens=java.base/java.net=ALL-UNNAMED
)

echo ConfigNode on heap memory size = %ON_HEAP_MEMORY%B, off heap memory size = %OFF_HEAP_MEMORY%B
echo If you want to change this configuration, please check conf/windows/confignode-env.bat.
