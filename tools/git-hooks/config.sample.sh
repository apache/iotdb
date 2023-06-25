#!/bin/sh

################################################################################
##
##  Licensed to the Apache Software Foundation (ASF) under one or more
##  contributor license agreements.  See the NOTICE file distributed with
##  this work for additional information regarding copyright ownership.
##  The ASF licenses this file to You under the Apache License, Version 2.0
##  (the "License"); you may not use this file except in compliance with
##  the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

# enable the pre-commit hooks, 0 - off, 1 - on
export IOTDB_GIT_HOOKS=1
# maven path
# If in Git Bash, replace all '\' to '/', and change the driver path from 'C:\' to '/c/
# No escape character is needed.
# Example:
#   export IOTDB_MAVEN_PATH="/c/Program Files/apache-maven-3.6/bin/mvn"
export IOTDB_MAVEN_PATH="mvn"
# git path
# If in Git Bash, see 'IOTDB_MAVEN_PATH'
export IOTDB_GIT_PATH="git"
# smart select the changed java module, 0 - off, 1 - on
export IOTDB_SMART_JAVA_MODULES=1
# auto spotless:apply, 0 - off, 1 - on
export IOTDB_SPOTLESS_APPLY=1
# maven validate, 0 - off, 1 - on
export IOTDB_VALIDATE=1
# 0 - discard all, 1 - logs on errors, 2 - stdout, 3 - stdout & logs on errors
export IOTDB_VERBOSE=2
