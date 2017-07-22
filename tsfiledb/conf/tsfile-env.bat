@echo off

set LOCAL_JMX=yes

set JMX_PORT=31999

if "%LOCAL_JMX%" == "yes" (
		set TSFILEDB_JMX_OPTS="-Dtsfiledb.jmx.local.port=%JMX_PORT%" "-Dcom.sun.management.jmxremote.authenticate=false" "-Dcom.sun.management.jmxremote.ssl=false"
	) else (
		set TSFILEDB_JMX_OPTS="-Dcom.sun.management.jmxremote" "-Dcom.sun.management.jmxremote.authenticate=false"  "-Dcom.sun.management.jmxremote.ssl=false" "-Dcom.sun.management.jmxremote.port=%JMX_PORT%"
	)
set TSFILEDB_DERBY_OPTS= "-Dderby.stream.error.field=cn.edu.thu.tsfiledb.auth.dao.DerbyUtil.DEV_NULL"