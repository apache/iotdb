#!/bin/sh

mvn clean package -DskipTests

if [ -d "./tsfiledb/lib/" ]; then
   rm -r ./tsfiledb/lib/
fi

if [ -d "./tsfiledb/doc/" ]; then
   rm -r ./tsfiledb/doc/
fi

if [ -d "./tsfiledb/tmp/" ]; then
   rm -r ./tsfiledb/tmp/
fi

if [ -d "./tsfiledb/logs/" ]; then
   rm -r ./tsfiledb/logs/
fi

if [ -f "./tsfiledb/conf/tsfile.yaml" ]; then
   rm ./tsfiledb/conf/tsfile.yaml
fi

if [ -f "./tsfiledb.tar.gz" ]; then
   rm ./tsfiledb.tar.gz
fi

mkdir ./tsfiledb/lib/
mkdir ./tsfiledb/doc/
mkdir ./tsfiledb/tmp/
mkdir ./tsfiledb/logs/

#cp ./tsfile-service/target/lib/* ./delta/lib/
#cp ./tsfile-service/target/tsfile-service-0.0.1-SNAPSHOT.jar ./delta/lib/
#cp ./tsfile-jdbc/target/lib/* ./delta/lib/
#cp ./tsfile-jdbc/target/tsfile-jdbc-0.0.1-SNAPSHOT.jar ./delta/lib/
#cp ./tsfile-common/src/main/resources/tsfile.yaml ./delta/conf/

tar -zcf ./tsfiledb.tar.gz ./tsfiledb/
