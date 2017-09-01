#!/bin/sh

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

TSFILE_CONF=${TSFILE_HOME}/conf
TSFILE_LOGS=${TSFILE_HOME}/logs

MAIN_CLASS=cn.edu.thu.tsfiledb.jdbc.Client


CLASSPATH=""
for f in ${TSFILE_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi


exec "$JAVA" -DTSFILE_HOME=${TSFILE_HOME} -Dlogback.configurationFile=${TSFILE_CONF}/logback.xml -cp "$CLASSPATH" "$MAIN_CLASS" "$@" 

exit $?
