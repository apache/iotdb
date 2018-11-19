#!/bin/sh
echo "Package iotdb-jdbc..."
mvn clean package -Dmaven.test.skip=true

if [ -d "./lib/" ]; then
   rm -r ./lib/
fi
mkdir ./lib/

echo "Copy denpendency jars to ./lib"
cp ./target/lib/*.jar ./lib/

file_name=`find ./target -name "iotdb-jdbc-?.?.?.jar"`
version=${file_name#*iotdb-jdbc-}
version=${version%.jar}
# 拷贝到lib目录下
echo "Copy latest iotdb-jdbc-jar to ./lib. version is : $version"
cp ./target/iotdb-jdbc-$version.jar ./lib/

echo "Zip all jars..."
# 压缩成一个zip
cd ./lib
zip iotdb-jdbc-$version.zip ./*
echo "Done. see ./lib/iotdb-jdbc-$version.zip"
