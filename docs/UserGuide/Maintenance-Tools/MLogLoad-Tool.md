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

## MLogLoad Tool 

### Introduction

The MLogLoad tool is used to load the metadata from `mlog.bin` and tags and attributes from `tlog.txt` into the running IoTDB.
Metadata operation in `mlog.bin` will be replayed.
Metedata to be loaded that conflicts with metadata in the running IoTDB will be skipped.

If there is already metadata in the running IoTDB, to avoid replaying the delete operation to affect the existing metadata, it needs to be loaded using `mlog.bin` and `tlog.txt` obtained from the [export metadata operation](https://iotdb.apache.org/UserGuide/V0.13.x/Maintenance-Tools/Export-Schema-Tool.html).

### How to Use

Linux/MacOS

> ./mLogLoad.sh -mlog /yourpath/mlog.bin -tlog /yourpath/tlog.txt -h 127.0.0.1 -p 6667 -u root -pw root

Windows

> ./mLogLoad.bat -mlog /yourpath/mlog.bin -tlog /yourpath/tlog.txt -h 127.0.0.1 -p 6667 -u root -pw root

```
usage: MLogLoad -mlog <mlog file> [-tlog <tlog file>] [-h <receiver host>]
       [-p <receiver port>] [-u <user>] [-pw <password>] [-help]
 -mlog <mlog file>    Need to specify a binary mlog.bin file to parse
                      (required)
 -tlog <tlog file>    Could specify a binary tlog.txt file to parse. Tags
                      and attributes will be ignored if not specified
                      (optional)
 -h <receiver host>   Could specify a specify the receiver host, default
                      is 127.0.0.1 (optional)
 -p <receiver port>   Could specify a specify the receiver port, default
                      is 6667 (optional)
 -u <user>            Could specify the user name, default is root
                      (optional)
 -pw <password>       Could specify the password, default is root
                      (optional)
 -help,--help         Display help information
```

Note: If tlog is not specified, the MLogLoad tool will ignore tags and attributes management.

### Example

The purpose is to load the local metadata file `/yourpath/mlog.bin` into IoTDB instance running on server 192.168.0.101:6667.

Enter to the directory where mLogLoad.sh is located and execute the following statement:

```
./mLogLoad.sh -mlog "/yourpath/mlog.bin" -h 192.168.0.101 -p 6667 -u root -pw root
```

After waiting for the script execution to complete, you can check that the metadata in the IoTDB instance has been loaded correctly.

### Q&A

* Cannot find or load the main class MLoadLoader
  * It may be because the environment variable $IOTDB_HOME is not set, please set the environment variable and try again