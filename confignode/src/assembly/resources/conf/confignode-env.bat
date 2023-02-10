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
  -Dcom.sun.management.jmxremote.password.file=%CONFIGNODE_CONF%\jmx.password^
  -Dcom.sun.management.jmxremote.access.file=%CONFIGNODE_CONF%\jmx.access^
  -Djava.rmi.server.hostname=%JMX_IP%
) else (
  echo "setting local JMX..."
)

set CONFIGNODE_JMX_OPTS=%CONFIGNODE_JMX_OPTS% -Diotdb.jmx.local=%JMX_LOCAL%

for /f %%b in ('wmic cpu get numberofcores ^| findstr "[0-9]"') do (
	set system_cpu_cores=%%b
)

if %system_cpu_cores% LSS 1 set system_cpu_cores=1

for /f  %%b in ('wmic ComputerSystem get TotalPhysicalMemory ^| findstr "[0-9]"') do (
	set system_memory=%%b
)

echo wsh.echo FormatNumber(cdbl(%system_memory%)/(1024*1024), 0) > %CONFIGNODE_HOME%\sbin\tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo %CONFIGNODE_HOME%\sbin\tmp.vbs') do set system_memory_in_mb=%%a
del %CONFIGNODE_HOME%\sbin\tmp.vbs
set system_memory_in_mb=%system_memory_in_mb:,=%

set /a half_=%system_memory_in_mb%/2
set /a quarter_=%half_%/2

if %half_% GTR 1024 set half_=1024
if %quarter_% GTR 65536 set quarter_=65536

if %half_% GTR %quarter_% (
	set max_heap_size_in_mb=%half_%
) else set max_heap_size_in_mb=%quarter_%

set MAX_HEAP_SIZE=%max_heap_size_in_mb%M
set max_sensible_yg_per_core_in_mb=100
set /a max_sensible_yg_in_mb=%max_sensible_yg_per_core_in_mb%*%system_cpu_cores%
set /a desired_yg_in_mb=%max_heap_size_in_mb%/4

if %desired_yg_in_mb% GTR %max_sensible_yg_in_mb% (
	set HEAP_NEWSIZE=%max_sensible_yg_in_mb%M
) else set HEAP_NEWSIZE=%desired_yg_in_mb%M

@REM if the heap size is larger than 16GB, we will forbid writing the heap dump file
if %desired_yg_in_mb% GTR 16384 (
	set IOTDB_ALLOW_HEAP_DUMP="false"
) else set IOTDB_ALLOW_HEAP_DUMP="true"

@REM Maximum heap size
@REM set MAX_HEAP_SIZE="2G"
@REM Minimum heap size
@REM set HEAP_NEWSIZE="2G"

@REM maximum direct memory size
set MAX_DIRECT_MEMORY_SIZE=%MAX_HEAP_SIZE%
@REM threads number that may use direct memory, including query threads(8) + merge threads(4) + space left for system(4)
set threads_number=16
@REM the size of buffer cache pool(IOV_MAX) depends on operating system
set temp_buffer_pool_size=1024
@REM Max cached buffer size, Note: unit can only be B!
@REM which equals DIRECT_MEMORY_SIZE / threads_number / temp_buffer_pool_size
set MAX_CACHED_BUFFER_SIZE=%max_heap_size_in_mb%*1024*1024/%threads_number%/%temp_buffer_pool_size%

set CONFIGNODE_HEAP_OPTS=-Xmx%MAX_HEAP_SIZE% -Xms%HEAP_NEWSIZE%
set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -XX:MaxDirectMemorySize=%MAX_DIRECT_MEMORY_SIZE%
set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -Djdk.nio.maxCachedBufferSize=%MAX_CACHED_BUFFER_SIZE%

@REM You can put your env variable here
@REM set JAVA_HOME=%JAVA_HOME%

@REM set gc log.
IF "%1" equ "printgc" (
	IF "%JAVA_VERSION%" == "8" (
	    md %CONFIGNODE_HOME%\logs
		set CONFIGNODE_HEAP_OPTS=%CONFIGNODE_HEAP_OPTS% -Xloggc:"%CONFIGNODE_HOME%\logs\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails  -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
	) ELSE (
		md %CONFIGNODE_HOME%\logs
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

echo Maximum memory allocation pool = %MAX_HEAP_SIZE%, initial memory allocation pool = %HEAP_NEWSIZE%
echo If you want to change this configuration, please check conf/confignode-env.sh(Unix or OS X, if you use Windows, check conf/confignode-env.bat).
