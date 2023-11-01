#!/bin/sh
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# Check if Maven is installed on the machine
if command -v mvn >/dev/null 2>&1; then
    MVN_COMMAND="mvn"
else
    MVN_COMMAND="./mvnw"
fi

# Check if JDK version is 21 or above
JDK_VERSION=$(javac -version 2>&1 | awk -F '.' '{print $2}')
if [ "$JDK_VERSION" -ge 21 ]; then
    SPOTLESS_OPTION="-Dspotless.skip=true"
else
    SPOTLESS_OPTION=""
fi

# Build distribution using Maven 
$MVN_COMMAND clean package -pl distribution -am -DskipTests $SPOTLESS_OPTION