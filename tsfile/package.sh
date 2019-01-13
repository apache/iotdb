#!/bin/sh
#
# Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
