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
@REM DO NOT FORGET TO MODIFY THE PASSWORD FOR SECURITY (%IOTDB_CONF%\jmx.password and %{IOTDB_CONF%\jmx.access)
set JMX_LOCAL="true"
set JMX_PORT="31999"
@REM only take effect when the jmx_local=false
@REM You need to change this IP as a public IP if you want to remotely connect IoTDB by JMX.
@REM  0.0.0.0 is not allowed
set JMX_IP="127.0.0.1"

if %JMX_LOCAL% == "false" (
  echo "setting remote JMX..."
  #you may have no permission to run chmod. If so, contact your system administrator.
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.rmi.port=%JMX_PORT%"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Djava.rmi.server.randomIDs=true"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.authenticate=true"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.ssl=false"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.authenticate=true"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.password.file=%IOTDB_CONF%\jmx.password"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Dcom.sun.management.jmxremote.access.file=%IOTDB_CONF%\jmx.access"
  set IOTDB_JMX_OPTS="%IOTDB_JMX_OPTS% -Djava.rmi.server.hostname=%JMX_IP%"
) else (
  echo "setting local JMX..."
)

for /f %%b in ('wmic cpu get numberofcores ^| findstr "[0-9]"') do (
	set system_cpu_cores=%%b
)

if %system_cpu_cores% LSS 1 set system_cpu_cores=1

for /f  %%b in ('wmic ComputerSystem get TotalPhysicalMemory ^| findstr "[0-9]"') do (
	set system_memory=%%b
)

echo wsh.echo FormatNumber(cdbl(%system_memory%)/(1024*1024), 0) > %temp%\tmp.vbs
for /f "tokens=*" %%a in ('cscript //nologo %temp%\tmp.vbs') do set system_memory_in_mb=%%a
del %temp%\tmp.vbs
set system_memory_in_mb=%system_memory_in_mb:,=%

set /a half_=%system_memory_in_mb%/2
set /a quarter_=%half_%/2

if %half_% GTR 1024 set half_=1024
if %quarter_% GTR 8192 set quarter_=8192

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

@REM Maximum heap size
@REM set MAX_HEAP_SIZE="2G"
@REM Minimum heap size
@REM set HEAP_NEWSIZE="2G"

IF ["%IOTDB_HEAP_OPTS%"] EQU [""] (
	rem detect Java 8 or 11
	IF "%JAVA_VERSION%" == "8" (
		java -d64 -version >nul 2>&1
		set IOTDB_HEAP_OPTS=-Xmx%MAX_HEAP_SIZE% -Xms%HEAP_NEWSIZE% -Xloggc:"%IOTDB_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails
		goto end_config_setting
	) ELSE (
		goto detect_jdk11_bit_version
	)
)

:detect_jdk11_bit_version
for /f "tokens=1-3" %%j in ('java -version 2^>^&1') do (
	@rem echo %%j
	@rem echo %%k
	@rem echo %%l
	set BIT_VERSION=%%l
)

@REM maximum direct memory size
set MAX_DIRECT_MEMORY_SIZE=%MAX_HEAP_SIZE%

set IOTDB_HEAP_OPTS=-Xmx%MAX_HEAP_SIZE% -Xms%HEAP_NEWSIZE% -Xlog:gc:"..\gc.log"
set IOTDB_HEAP_OPTS=%IOTDB_HEAP_OPTS% -XX:MaxDirectMemorySize=%MAX_DIRECT_MEMORY_SIZE%

@REM You can put your env variable here
@REM set JAVA_HOME=%JAVA_HOME%

:end_config_setting
@REM set gc log.
IF "%1" equ "printgc" (
	IF "%JAVA_VERSION%" == "8" (
	    md %IOTDB_HOME%\logs
		set IOTDB_HEAP_OPTS=%IOTDB_HEAP_OPTS% -Xloggc:"%IOTDB_HOME%\logs\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails  -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
	) ELSE (
		md %IOTDB_HOME%\logs
		set IOTDB_HEAP_OPTS=%IOTDB_HEAP_OPTS%  -Xlog:gc=info,heap*=trace,age*=debug,safepoint=info,promotion*=trace:file="%IOTDB_HOME%\logs\gc.log":time,uptime,pid,tid,level:filecount=10,filesize=10485760
	)
)


echo Maximum memory allocation pool = %MAX_HEAP_SIZE%, initial memory allocation pool = %HEAP_NEWSIZE%
echo If you want to change this configuration, please check conf/iotdb-env.sh(Unix or OS X, if you use Windows, check conf/iotdb-env.bat).