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
# tool directory rel to project
REL_SCRIPT_DIR="tools/git-hooks"

# config sample filename rel to script dir
REL_CONFIG_SAMPLE_FILE="config.sample.sh"
# config filename rel to script dir
REL_CONFIG_FILE="config.sh"

check_paths() {
    echo "Checking paths..."
    if [ ! -d "$PROJECT_DIR" ]; then
        echo "ERROR: Project directory '$PROJECT_DIR' does not exist." >&2
        exit 1
    fi
    if [ ! -d "$HOOKS_DIR" ]; then
        echo "ERROR: Hooks directory '$HOOKS_DIR' does not exist." >&2
        exit 1
    fi
}

install() {
    # create the relative symlink of pre-commit script
    if [ -f "$PROJECT_DIR/$REL_SCRIPT_DIR/pre-commit" ] || [ -L "$PROJECT_DIR/$REL_SCRIPT_DIR/pre-commit" ]; then
        echo "WARN: Overwriting '$PROJECT_DIR/$REL_SCRIPT_DIR/pre-commit'"
    fi
    ln -sf "$PROJECT_DIR/$REL_SCRIPT_DIR/pre-commit" "$HOOKS_DIR/pre-commit" || exit 1
    if [ -f "pre-commit" ] && [ ! -x "pre-commit" ]; then
        echo "ERROR: 'pre-commit' has no execute permission. Try to grant it the 'x' permission."
        exit 1
    fi
    # create the relative symlink of config file
    if [ ! -f "$REL_CONFIG_FILE" ]; then
        # if `config.sh` not exists in this directory, create it from sample
        test -f "$REL_CONFIG_SAMPLE_FILE" || { echo "ERROR: Could not find '$REL_CONFIG_SAMPLE_FILE'." >&2; exit 1; }
        cp "$REL_CONFIG_SAMPLE_FILE" "$REL_CONFIG_FILE"
    else
        echo "WARN: $REL_CONFIG_FILE is already existed. Ignored."
    fi
    ln -sf "$PROJECT_DIR/$REL_SCRIPT_DIR/$REL_CONFIG_FILE" "$HOOKS_DIR/$REL_CONFIG_FILE" || exit 1
    # finished
    echo "Installed hooks."
}

main() {
    echo "Installing pre-commit hook..."
    check_paths
    install
}

main
