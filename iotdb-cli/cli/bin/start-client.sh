#!/bin/sh

echo ---------------------
echo Starting IoTDB Client
echo ---------------------

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi


MAIN_CLASS=org.apache.iotdb.cli.client.Client


CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
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

exec "$JAVA" -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?
