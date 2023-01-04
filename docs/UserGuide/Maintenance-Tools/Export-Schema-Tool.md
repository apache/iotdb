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


Export destination directory on server need to be specified when exporting metadata using scripting. Note that the target directory path must be absolute path.
```
usage: ExportSchema -o <target directory path> [-h <host address>] [-p <port>] [-u <user>] [-pw <password>] [-help]
 -o <target directory path>   Need to specify a absolute target directory
                              path on server（required)
 -h <host address>            Could specify a specify the IoTDB host
                              address, default is 127.0.0.1 (optional)
 -p <port>                    Could specify a specify the IoTDB port,
                              default is 6667 (optional)
 -u <user>                    Could specify the IoTDB user name, default
                              is root (optional)
 -pw <password>               Could specify the IoTDB password, default is
                              root (optional)
 -help,--help                 Display help information
```

### Q&A

* Cannot find or load the main class ExportSchema
    * It may be because the environment variable $IOTDB_HOME is not set, please set the environment variable and try again
* Encounter an error, because: File ... already exist.
    * There is already a mlog.bin or tlog.txt file in the target directory, please check the target directory and try again


### Load Schema

Please refer to [MLogLoad Tool](https://iotdb.apache.org/UserGuide/V0.13.x/Maintenance-Tools/MLogLoad-Tool.html)

