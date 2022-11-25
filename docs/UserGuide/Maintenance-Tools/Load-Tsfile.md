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

# Load External TsFile Tool

## Introduction

The load external tsfile tool allows users to load tsfiles, delete a tsfile, or move a tsfile to target directory from the running Apache IoTDB instance. Alternatively, you can use scripts to load tsfiles into IoTDB, for more information.

## Load with SQL

The user sends specified commands to the Apache IoTDB system through the Cli tool or JDBC to use the tool.

### load tsfiles

The command to load tsfiles is `load <path/dir> [sglevel=int][verify=true/false][onSuccess=delete/none]`.

This command has two usages:

1. Load a single tsfile by specifying a file path (absolute path). 

The first parameter indicates the path of the tsfile to be loaded. This command has three options: sglevel, verify, onSuccess.

SGLEVEL option. If the database correspond to the tsfile does not exist, the user can set the level of database through the fourth parameter. By default, it uses the database level which is set in `iotdb-datanode.properties`.

VERIFY option. If this parameter is true, All timeseries in this loading tsfile will be compared with the timeseries in IoTDB. If existing a measurement which has different datatype with the measurement in IoTDB, the loading process will be stopped and exit. If consistence can be promised, setting false for this parameter will be a better choice.

ONSUCCESS option. The default value is DELETE, which means  the processing method of successfully loaded tsfiles, and DELETE means  after the tsfile is successfully loaded, it will be deleted. NONE means after the tsfile is successfully loaded, it will be remained in the origin dir.

If the `.resource` file corresponding to the file exists, it will be loaded into the data directory and engine of the Apache IoTDB. Otherwise, the corresponding `.resource` file will be regenerated from the tsfile file.

Examples:

* `load '/Users/Desktop/data/1575028885956-101-0.tsfile'`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' onSuccess=delete`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true onSuccess=none`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1 onSuccess=delete`

2. Load a batch of files by specifying a folder path (absolute path). 

The first parameter indicates the path of the tsfile to be loaded. The options above also works for this command.

Examples:

* `load '/Users/Desktop/data'`
* `load '/Users/Desktop/data' verify=false`
* `load '/Users/Desktop/data' verify=true`
* `load '/Users/Desktop/data' verify=true sglevel=1`
* `load '/Users/Desktop/data' verify=false sglevel=1 onSuccess=delete`

**NOTICE**:  When `$IOTDB_HOME$/conf/iotdb-datanode.properties` has `enable_auto_create_schema=true`, it will automatically create metadata in TSFILE, otherwise it will not be created automatically. 

## Load with Script

Run rewrite-tsfile.bat if you are in a Windows environment, or rewrite-tsfile.sh if you are on Linux or Unix.

```bash
./load-tsfile.bat -f filePath [-h host] [-p port] [-u username] [-pw password] [--sgLevel int] [--verify true/false] [--onSuccess none/delete]
-f 			File/Directory to be load, required
-h 			IoTDB Host address, optional field, 127.0.0.1 by default
-p 			IoTDB port, optional field, 6667 by default
-u 			IoTDB user name, optional field, root by default
-pw 		IoTDB password, optional field, root by default
--sgLevel 	Sg level of loading Tsfile, optional field, default_storage_group_level in 				iotdb-common.properties by default
--verify 	Verify schema or not, optional field, True by default
--onSuccess Delete or remain origin TsFile after loading, optional field, none by default
```

### Example

Assuming that an IoTDB instance is running on server 192.168.0.101:6667, you want to load all TsFile files from the locally saved TsFile backup folder D:\IoTDB\data into this IoTDB instance.

First move to the folder `$IOTDB_HOME/tools/`, open the command line, and execute

```bash
./load-rewrite.bat -f D:\IoTDB\data -h 192.168.0.101 -p 6667 -u root -pw root
```

After waiting for the script execution to complete, you can check that the data in the IoTDB instance has been loaded correctly.

### Q&A

- Cannot find or load the main class
  - It may be because the environment variable $IOTDB_HOME is not set, please set the environment variable and try again
- -f option must be set!
  - The input command is missing the -f field (file or folder path to be loaded) or the -u field (user name), please add it and re-execute
- What if the execution crashes in the middle and you want to reload?
  - You re-execute the command just now, reloading the data will not affect the correctness after loading