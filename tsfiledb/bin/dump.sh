#!/bin/sh
if [ -z "${TSFILE_HOME}" ];then
   export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

MAIN_CLASS=cn.edu.thu.tsfiledb.tool.TsFileDump

CLASSPATH="."

for f in ${TSFILE_HOME}/lib/*.jar; do
    CLASSPATH=${CLASSPATH}":"$f
done

echo $JAVA_HOME
if [ -n "$JAVA_HOME" ];then
   for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
       if [ -x "$java" ]; then
          JAVA="$java"
	     break
	   fi
    done
else
    JAVA=java
	
fi

echo $@
exec "$JAVA" -DTSFILE_HOME=${TSFILE_HOME}  -cp "$CLASSPATH" "$MAIN_CLASS" "$@"

exit $?
