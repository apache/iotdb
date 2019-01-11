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
# copy to lib directory
echo "Copy latest tsfile-jar to ./lib. version is : $version"
cp ./target/tsfile-$version.jar ./lib/

echo "Zip all jars..."
# compress to a zip file
cd ./lib
zip tsfile-$version.zip ./*
echo "Done. see ./lib/tsfile-$version.zip"
