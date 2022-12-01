#!/bin/bash
if [[ "$1" == "single" ]]; then
echo "build server"
mvn clean package -pl server -am -DskipTests
elif [[ "$1" == "dist" ]]; then
echo "build dist"
mvn clean package -pl distribution -am -DskipTests
else
echo "build all iotdb"
mvn clean package -DskipTests
fi

