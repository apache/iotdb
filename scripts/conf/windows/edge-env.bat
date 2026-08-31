@echo off
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

@REM IoTDB Edge runs ConfigNode and DataNode inside ONE JVM with a fixed, small
@REM memory budget (about 512 MB total process RSS by default).

if "%ON_HEAP_MEMORY%"=="" set ON_HEAP_MEMORY=224M
if "%INIT_HEAP_MEMORY%"=="" set INIT_HEAP_MEMORY=64M
if "%OFF_HEAP_MEMORY%"=="" set OFF_HEAP_MEMORY=96M

set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -Diotdb.jmx.local=true
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -Xms%INIT_HEAP_MEMORY%
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -Xmx%ON_HEAP_MEMORY%
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:MaxDirectMemorySize=%OFF_HEAP_MEMORY%
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:+CrashOnOutOfMemoryError
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:+UseSerialGC
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -Xss320k
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:MaxMetaspaceSize=160m
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:CompressedClassSpaceSize=40m
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:ReservedCodeCacheSize=64m
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:ActiveProcessorCount=2
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:+UnlockDiagnosticVMOptions
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -XX:+UseCRC32Intrinsics
set IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8

@REM Load the Maven-filtered locale before expanding its value in a separate command.
if EXIST "%IOTDB_CONF%\windows\iotdb-common.bat" call "%IOTDB_CONF%\windows\iotdb-common.bat"
if DEFINED TSFILE_LOCALE_JVM_OPT set "IOTDB_JMX_OPTS=%IOTDB_JMX_OPTS% %TSFILE_LOCALE_JVM_OPT%"

echo IoTDB Edge on heap memory size = %ON_HEAP_MEMORY%B, off heap memory size = %OFF_HEAP_MEMORY%B
echo If you want to change this configuration, please check conf\windows\edge-env.bat.
