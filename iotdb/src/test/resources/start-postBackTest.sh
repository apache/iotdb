#!/bin/sh

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..;cd ".."; cd ".."; cd "iotdb" ;pwd)"
  echo $IOTDB_HOME
fi

IOTDB_CONF=${IOTDB_HOME}/conf
# IOTDB_LOGS=${IOTDB_HOME}/logs

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

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=cn.edu.tsinghua.iotdb.postback.utils.CreateDataSender1

"$JAVA" -DIOTDB_HOME=${IOTDB_HOME} -DTSFILE_HOME=${IOTDB_HOME} -DIOTDB_CONF=${IOTDB_CONF} -Dlogback.configurationFile=${IOTDB_CONF}/logback.xml $IOTDB_DERBY_OPTS $IOTDB_JMX_OPTS -Dname=postBackTest -cp "$CLASSPATH" "$MAIN_CLASS"

exit $?
