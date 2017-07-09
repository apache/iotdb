#!/bin/sh

#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_112.jdk/Contents/Home

JMX_PORT="31999"

TSFILEDB_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
TSFILEDB_JMX_OPTS="$TSFILEDB_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
