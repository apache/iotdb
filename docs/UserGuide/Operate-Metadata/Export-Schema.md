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

## Export Schema

The schema export operation exports the information about store group, timeseries, and schema template in the current IoTDB in the form of `mlog.bin` and `tlog.txt` to the specified directory.

The exported `mlog.bin` and `tlog.txt` files can be incrementally loaded into an IoTDB instance.

### Export Schema with SQL

```
EXPORT SCHEMA '<path/dir>' 
```

### Export Schema with Script

Linux/MacOS

> ./exportSchema.sh -d /yourpath/data/system/schema -o /yourpath/targetDir

Windows

> ./exportSchema.bat-d /yourpath/data/system/schema -o /yourpath/targetDir


Source directory and the export destination directory need to be specified when exporting metadata using scripting.
```
usage: ExportSchema -d <source directory path> -o <target directory path>
       [-help]
 -d <source directory path>   Need to specify a source directory path
 -o <target directory path>   Need to specify a target directory path
 -help,--help                 Display help information
```

### Q&A

* Cannot find or load the main class ExportSchema
    * It may be because the environment variable $IOTDB_HOME is not set, please set the environment variable and try again
* Encounter an error, because: File ... already exist.
    * There is already a mlog.bin or tlog.txt file in the target directory, please check the target directory and try again
* Encounter an error, because: ... does not exist or is not a directory.
    * The source directory path does not exist or is not a directory, please check the source directory and try again
* Encounter an error, because: ... is not a valid directory.
    * The source directory is not the schema directory in IoTDB, please check the target directory and try again


### Load Schema

Please refer to [MLogLoad Tool](https://iotdb.apache.org/UserGuide/V0.13.x/Maintenance-Tools/MLogLoad-Tool.html)

