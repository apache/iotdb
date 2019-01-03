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