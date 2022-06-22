#!/bin/sh

################################################################################
##  Copyright 2022 leonardodalinky(linkyy2000313@gmail.com)
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

# script directory
cd $(dirname "$0") || exit 1
# project directory
PROJECT_DIR="../.."
# hooks directory
HOOKS_DIR="$PROJECT_DIR/.git/hooks"

# color
Green="\x1B[32m"
NC="\x1B[0m"

rm -f "$HOOKS_DIR/config.sh"
rm -f "$HOOKS_DIR/pre-commit"

printf "${Green}Uninstallation succeed.${NC}\n"
