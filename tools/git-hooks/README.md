<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Guide to git hooks

The pre-commit hooks enable auto checks before commits. So next time a single `git commit` should do all the style checks automatically.

Currently, the support functionalities are:
* Turn on/off the hook in local config files.
* Auto `mvn spotless:apply` before commits
* Auto `mvn validate` before commits
* Smart selection of only the changed modules for validation

## Installation

All the files are under `tools/git-hooks`.

### Windows

Double click the `install.ps1`, and allow to execute in admin mode. Make sure that the Git for Windows is the default git on Windows.

**Note**: Windows support is based on the [Git for Windows](https://gitforwindows.org/), which provide a minimal shell environment called Git Bash. Actually, all these hooks on Windows are executed under the Git Bash environment, so the hook script can be somewhat platform-independent.

### Unix

```
./install.sh
```

## Configuration

After installation, a `config.sh` shall be created. Modification to the `config.sh` file would take effect the next time you commit.

All the configurable options is in `config.sample.sh`.

```sh
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
```

**Note 1 for Windows user**: If you are on Windows, pay attention to the `IOTDB_MAVEN_PATH` and `IOTDB_GIT_PATH`. Since the hook script is executed under Git Bash, you must make sure the Git Bash could get to the maven & git binary file correctly. The path expression in Git Bash is slightly different from Windows. Check the examples in config file carefully.

**Note 2 for Windows user**: The easiest way to handle these binary path is to put them into the %PATH% environment variable in Windows. Then the default configuration should work well.

## Uninstall

See `uninstall.ps1` and `uninstall.sh`.

## Pipeline

The overall pipeline of the hook script:
1. `IOTDB_GIT_HOOKS` checks if the hook is on
2. `IOTDB_SMART_JAVA_MODULES` to select only the changed modules
3. `IOTDB_SPOTLESS_APPLY` runs `mvn spotless:apply`
4. `IOTDB_VALIDATE` runs `mvn validate` or `mvn -pl <changed_modules> validate`

The hook is tested on both Linux and Windows.
