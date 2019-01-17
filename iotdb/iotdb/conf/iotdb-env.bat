@REM
@REM Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
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
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM

@echo off
set LOCAL_JMX=no
set JMX_PORT=31999

if "%LOCAL_JMX%" == "yes" (
		set IOTDB_JMX_OPTS="-Diotdb.jmx.local.port=%JMX_PORT%" "-Dcom.sun.management.jmxremote.authenticate=false" "-Dcom.sun.management.jmxremote.ssl=false"
	) else (
		set IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote" "-Dcom.sun.management.jmxremote.authenticate=false"  "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.port=%JMX_PORT%"
	)

IF ["%IOTDB_HEAP_OPTS%"] EQU [""] (
	rem detect Java 32 or 64 bit
	IF %JAVA_VERSION% == 8 (
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
	set IOTDB_HEAP_OPTS=-Xmx2G -Xms2G -Xloggc:"%IOTDB_HOME%\gc.log"
) ELSE (
	rem 32-bit Java
	echo Detect 32-bit Java, maximum memory allocation pool = 512MB, initial memory allocation pool = 512MB
	set IOTDB_HEAP_OPTS=-Xmx512M -Xms512M -Xloggc:"%IOTDB_HOME%\gc.log"
)

:end_config_setting
echo If you want to change this configuration, please check conf/iotdb-env.sh(Unix or OS X, if you use Windows, check conf/iotdb-env.bat).