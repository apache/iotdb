#!/bin/sh
echo "Package tsfile..."
mvn clean package -Dmaven.test.skip=true

if [ -d "./lib/" ]; then
   rm -r ./lib/
fi
mkdir ./lib/

echo "Copy denpendency jars to ./lib"
cp ./target/lib/*.jar ./lib/

file_name=`find ./target -name "tsfile-?.?.?.jar"`
version=${file_name#*tsfile-}
version=${version%.jar}
# 拷贝到lib目录下
echo "Copy latest tsfile-jar to ./lib. version is : $version"
cp ./target/tsfile-$version.jar ./lib/

echo "Zip all jars..."
# 压缩成一个zip
cd ./lib
zip tsfile-$version.zip ./*
echo "Done. see ./lib/tsfile-$version.zip"
