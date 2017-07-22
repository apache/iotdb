#!/bin/sh

if [ -z "${TSFILE_HOME}" ]; then
  export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

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
for f in ${TSFILE_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=cn.edu.thu.tsfiledb.tool.CSVToTsfile

"$JAVA" -DTSFILE_HOME=${TSFILE_HOME} -cp "$CLASSPATH" "$MAIN_CLASS" "$@"
exit $?