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

IF ["%IOTDB_HEAP_OPTS%"] EQU [""] (
	rem detect Java 8 or 11
	IF "%JAVA_VERSION%" == "8" (
		java -d64 -version >nul 2>&1
		IF NOT ERRORLEVEL 1 (
			rem 64-bit Java
			echo Detect 64-bit Java, maximum memory allocation pool = 2GB, initial memory allocation pool = 2GB
			set IOTDB_HEAP_OPTS=-Xmx2G -Xms2G -Xloggc:"%IOTDB_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails
		) ELSE (
			rem 32-bit Java
			echo Detect 32-bit Java, maximum memory allocation pool = 512MB, initial memory allocation pool = 512MB
			set IOTDB_HEAP_OPTS=-Xmx512M -Xms512M -Xloggc:"%IOTDB_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails
		)
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
IF "%BIT_VERSION%" == "64-Bit" (
	rem 64-bit Java
	echo Detect 64-bit Java, maximum memory allocation pool = 2GB, initial memory allocation pool = 2GB
	set IOTDB_HEAP_OPTS=-Xmx2G -Xms2G
) ELSE (
	rem 32-bit Java
	echo Detect 32-bit Java, maximum memory allocation pool = 512MB, initial memory allocation pool = 512MB
	set IOTDB_HEAP_OPTS=-Xmx512M -Xms512M
)

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
echo If you want to change this configuration, please check conf/iotdb-env.sh(Unix or OS X, if you use Windows, check conf/iotdb-env.bat).