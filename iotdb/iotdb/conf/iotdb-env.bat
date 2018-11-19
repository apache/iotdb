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
	java -d64 -version >nul 2>&1
	IF NOT ERRORLEVEL 1 (
		rem 64-bit Java
		echo Detect 64-bit Java, maximum memory allocation pool = 2GB, initial memory allocation pool = 2GB
		set IOTDB_HEAP_OPTS=-Xmx2G -Xms2G -Xloggc:"%IOTDB_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails
	) ELSE (
		rem 32-bit Java
		echo Detect 64-bit Java, maximum memory allocation pool = 512MB, initial memory allocation pool = 512MB
		set IOTDB_HEAP_OPTS=-Xmx512M -Xms512M -Xloggc:"%IOTDB_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails
	)
)
