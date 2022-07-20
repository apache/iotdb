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

# IoTDB Rewrite-TsFile Tool

## Introduction

The Rewrite-TsFile tool is used to write the data in TsFile to the running IoTDB.

## How to use

Run rewrite-tsfile.bat if you are in a Windows environment, or rewrite-tsfile.sh if you are on Linux or Unix.

```bash
./rewrite-tsfile.bat -f filePath [-h host] [-help] [-p port] [-pw password] -u user
-f File or Dictionary to be loaded.
-h Host Name (optional, default 127.0.0.1)
-help Display help information(optional)
-p Port (optional, default 6667)
-pw password (optional)
-u User name (required)
```

## Example

Assuming that an IoTDB instance is running on server 192.168.0.101:6667, you want to load all TsFile files from the locally saved TsFile backup folder D:\IoTDB\data into this IoTDB instance.

First move to the folder where rewrite-tsfile.bat is located, open the command line, and execute

```bash
./load-rewrite.bat -f "D:\IoTDB\data" -h 192.168.0.101 -p 6667 -u root -pw root
```

After waiting for the script execution to complete, you can check that the data in the IoTDB instance has been loaded correctly.

## Q&A

- Cannot find or load the main class RewriteTsFileTool
  - It may be because the environment variable $IOTDB_HOME is not set, please set the environment variable and try again
- Missing require argument: f or Missing require argument: u
  - The input command is missing the -f field (file or folder path to be loaded) or the -u field (user name), please add it and re-execute
- What if the execution crashes in the middle and you want to reload?
  - The easiest way, you re-execute the command just now, reloading the data will not affect the correctness after loading
  - If you want to save time by avoiding reloading a file that has already been loaded, you can remove the TsFile that the last execution log shows has been loaded from the pending folder and reload that folder
