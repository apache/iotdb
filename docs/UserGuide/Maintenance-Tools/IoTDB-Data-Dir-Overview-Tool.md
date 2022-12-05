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

# IoTDB Data Directory Overview Tool

IoTDB data directory overview tool is used to print an overview of the IoTDB data directory structure. The location is tools/tsfile/print-iotdb-data-dir.

## Usage

-   For Windows:

```bash
.\print-iotdb-data-dir.bat <IoTDB data folder path, separated by commas if there are multiple folders> (<storage path of the output overview file>) 
```

-   For Linux or MacOs:

```shell
./print-iotdb-data-dir.sh <IoTDB data folder path, separated by commas if there are multiple folders> (<storage path of the output overview file>) 
```

Note: if the storage path of the output overview file is not set, the default relative path "IoTDB_data_dir_overview.txt" will be used.

## Example

Use Windows in this example:

`````````````````````````bash
.\print-iotdb-data-dir.bat D:\github\master\iotdb\data\datanode\data
````````````````````````
Starting Printing the IoTDB Data Directory Overview
````````````````````````
output save path:IoTDB_data_dir_overview.txt
data dir num:1
143  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
|==============================================================
|D:\github\master\iotdb\data\datanode\data
|--sequence
|  |--root.redirect0
|  |  |--1
|  |  |  |--0
|  |--root.redirect1
|  |  |--2
|  |  |  |--0
|  |--root.redirect2
|  |  |--3
|  |  |  |--0
|  |--root.redirect3
|  |  |--4
|  |  |  |--0
|  |--root.redirect4
|  |  |--5
|  |  |  |--0
|  |--root.redirect5
|  |  |--6
|  |  |  |--0
|  |--root.sg1
|  |  |--0
|  |  |  |--0
|  |  |  |--2760
|--unsequence
|==============================================================
`````````````````````````

