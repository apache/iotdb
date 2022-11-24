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

The load external tsfile tool allows users to load tsfiles, delete a tsfile, or move a tsfile to target directory from the running Apache IoTDB instance. Alternatively, you can use scripts to load tsfiles into IoTDB, for more information, [please check this](../Maintenance-Tools/Load-TsFile-Tool.md).

## Usage

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

### remove a tsfile

The command to delete a tsfile is: `remove '<path>'`.

This command deletes a tsfile by specifying the file path. The specific implementation is to delete the tsfile and its corresponding `.resource` and` .modification` files.

Examples:

* `remove '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile'`

### unload a tsfile and move it to a target directory

The command to unload a tsfile and move it to target directory is: `unload '<path>' '<dir>'`.

This command unload a tsfile and move it to a target directory by specifying tsfile path and the target directory(absolute path). The specific implementation is to remove the tsfile from the engine and move the tsfile file and its corresponding `.resource` file to the target directory.

Examples:

* `unload '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile' '/data/data/tmp'`