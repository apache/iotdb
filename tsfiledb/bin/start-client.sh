#!/bin/sh
if [ $# -lt 3 ] ; then
    echo "Arguments : -host<host> -port<port> -u<username>"
    exit 0
fi

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi


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


exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$1" "$2" "$3"

exit $?
