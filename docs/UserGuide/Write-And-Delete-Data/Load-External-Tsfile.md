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

The load external tsfile tool allows users to load tsfiles, delete a tsfile, or move a tsfile to target directory from the running Apache IoTDB instance.

## Usage

The user sends specified commands to the Apache IoTDB system through the Cli tool or JDBC to use the tool.

### load tsfiles

The command to load tsfiles is `load <path/dir> [autoregister=true/false][,sglevel=int][,verify=true/false]`.

This command has two usages:

1. Load a single tsfile by specifying a file path (absolute path). 

The second parameter indicates the path of the tsfile to be loaded and the name of the tsfile needs to conform to the tsfile naming convention, that is, `{systemTime}-{versionNum}-{in_space_compaction_num}-{cross_space_compaction_num}.tsfile`. This command has three options: autoregister, sglevel and verify.

AUTOREGISTER option. If the metadata correspond to the timeseries in the tsfile to be loaded does not exist, you can choose whether to create the schema automatically. If this parameter is true, the schema is created automatically. If it is false, the schema will not be created. By default, the schema will be created.

SGLEVEL option. If the storage group correspond to the tsfile does not exist, the user can set the level of storage group through the fourth parameter. By default, it uses the storage group level which is set in `iotdb-engine.properties`.

VERIFY option. If this parameter is true, All timeseries in this loading tsfile will be compared with the timeseries in IoTDB. If existing a measurement which has different datatype with the measurement in IoTDB, the loading process will be stopped and exit. If consistence can be promised, setting false for this parameter will be a better choice.

If the `.resource` file corresponding to the file exists, it will be loaded into the data directory and engine of the Apache IoTDB. Otherwise, the corresponding `.resource` file will be regenerated from the tsfile file.

Examples:

* `load '/Users/Desktop/data/1575028885956-101-0.tsfile'`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' autoregister=false`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' autoregister=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' autoregister=true,sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false,sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' autoregister=false,verify=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' autoregister=false,sglevel=1,verify=true`

2. Load a batch of files by specifying a folder path (absolute path). 

The second parameter indicates the path of the tsfile to be loaded and the name of the tsfiles need to conform to the tsfile naming convention, that is, `{systemTime}-{versionNum}-{in_space_compaction_num}-{cross_space_compaction_num}.tsfile`. The options above also works for this command.

Examples:

* `load '/Users/Desktop/data'`
* `load '/Users/Desktop/data' autoregister=false`
* `load '/Users/Desktop/data' autoregister=true`
* `load '/Users/Desktop/data' autoregister=true,sglevel=1`
* `load '/Users/Desktop/data' autoregister=false,sglevel=1,verify=true`

#### Remote Load TsFile

Normally, the file path must be the local file path of the machine where the IoTDB instance is located. In IoTDB 0.13.5 and later, the file path supports for HTTP-style URIs that allow individual files to be loaded remotely via the HTTP protocol. The format is `load 'http://host:port/filePath'`.

For example, if your IoTDB instance is running on machine A with IP address 168.121.0.1 and you want to load the file `/root/data/1-1-0-0.tsfile` from machine B with IP address 168.121.0.2 into your IoTDB instance, you need to follow these steps

1. Start the HTTP service on Machine B. For example, you can use the python command `python -m http.server` to start a simple HTTP service.
2. Use the Cli.sh to connect to the IoTDB instance on Machine A.
3. Enter the SQL command `load 'http://168.121.0.2:8000/root/data/1-1-0-0.tsfile'` 
4. Wait for the load to complete

**Please note**: In the case of remote loading, only a single file is supported, i.e. the path parameter must be a single TsFile file path. It is also not recommended to use remote loading if your TsFile has undergone a delete operation (i.e., the TsFile file has an attached .mods file), which will result in data that should have been deleted not being deleted after the load.

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