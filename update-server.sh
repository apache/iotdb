#!/bin/bash
git pull
mvn clean package -pl server -am -DskipTests
