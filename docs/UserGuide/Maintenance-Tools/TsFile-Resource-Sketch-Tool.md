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

# TsFile Resource Sketch Tool

TsFile resource sketch tool is used to print the content of a TsFile resource file. The location is tools/tsfile/print-tsfile-resource-files.

## Usage

-   For Windows:

```bash
.\print-tsfile-resource-files.bat <path of the parent directory of the TsFile resource files, or path of a TsFile resource file>
```

-   For Linux or MacOs:

```
./print-tsfile-resource-files.sh <path of the parent directory of the TsFile resource files, or path of a TsFile resource file> 
```

## Example

Use Windows in this example:

`````````````````````````bash
.\print-tsfile-resource-files.bat D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0
````````````````````````
Starting Printing the TsFileResources
````````````````````````
147  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
230  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-common.properties, use default configuration
231  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-common.properties from any of the known sources.
233  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-datanode.properties, use default configuration
237  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-datanode.properties from any of the known sources.
Analyzing D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile ...

Resource plan index range [9223372036854775807, -9223372036854775808]
device root.sg1.d1, start time 0 (1970-01-01T08:00+08:00[GMT+08:00]), end time 99 (1970-01-01T08:00:00.099+08:00[GMT+08:00])

Analyzing the resource file folder D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0 finished.
`````````````````````````

`````````````````````````bash
.\print-tsfile-resource-files.bat D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile.resource
````````````````````````
Starting Printing the TsFileResources
````````````````````````
178  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-common.properties, use default configuration
186  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
187  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-common.properties from any of the known sources.
188  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-datanode.properties, use default configuration
192  [main] WARN  o.a.iotdb.db.conf.IoTDBDescriptor - Couldn't load the configuration iotdb-datanode.properties from any of the known sources.
Analyzing D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile ...

Resource plan index range [9223372036854775807, -9223372036854775808]
device root.sg1.d1, start time 0 (1970-01-01T08:00+08:00[GMT+08:00]), end time 99 (1970-01-01T08:00:00.099+08:00[GMT+08:00])

Analyzing the resource file D:\github\master\iotdb\data\datanode\data\sequence\root.sg1\0\0\1669359533489-1-0-0.tsfile.resource finished.
`````````````````````````

