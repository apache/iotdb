@echo off

set LOCAL_JMX=no

set JMX_PORT=31999

if "%LOCAL_JMX%" == "yes" (
		set IOTDB_JMX_OPTS="-Diotdb.jmx.local.port=%JMX_PORT%" "-Dcom.sun.management.jmxremote.authenticate=false" "-Dcom.sun.management.jmxremote.ssl=false"
	) else (
		set IOTDB_JMX_OPTS="-Dcom.sun.management.jmxremote" "-Dcom.sun.management.jmxremote.authenticate=false"  "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.port=%JMX_PORT%"
	)
set IOTDB_DERBY_OPTS= "-Dderby.stream.error.field=cn.edu.tsinghua.iotdb.auth.dao.DerbyUtil.DEV_NULL"


IF ["%IOTDB_HEAP_OPTS%"] EQU [""] (
    rem detect OS architecture
    wmic os get osarchitecture | find /i "32-bit" >nul 2>&1
    IF NOT ERRORLEVEL 1 (
        rem 32-bit OS
        set IOTDB_HEAP_OPTS=-Xmx512M -Xms512M -Xloggc:%IOTDB_HOME%/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails
    ) ELSE (
        rem 64-bit OS
        set IOTDB_HEAP_OPTS=-Xmx2G -Xms2G -Xloggc:%IOTDB_HOME%/logs/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails
    )
)
