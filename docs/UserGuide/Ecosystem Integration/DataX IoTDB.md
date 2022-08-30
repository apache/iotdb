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
# DataX IoTDB

[DataX](https://github.com/alibaba/DataX) iotdbwriter plugin for synchronized DataX other sources of data to IoTDB.



This plugin uses the IotDB Session function for data import. It needs to work with the DataX service.

##  About DataX

DataX is the open source version of Alibaba Cloud DataWorks data integration, an offline data synchronization tool/platform widely used in Alibaba Group. DataX implements MySQL, Oracle, SqlServer, Postgre, HDFS, Hive, ADS, HBase, TableStore(OTS), MaxCompute(ODPS), Hologres, and DRDS Efficient data synchronization between heterogeneous data sources.

For more information, please refer to:`https://github.com/alibaba/DataX/`

## Code description

DataX iotdbwriter plugin code  [here](./iotdbwriter)。

This directory contains the plug-in code and the development environment for the DataX project.



Modules in the DataX code that the iotDBWriter plug-in depends on. These modules are not in the official Maven repository. Therefore, when developing the iotDBWriter plug-in, we need to download the complete DataX code base to compile and develop the plug-in.

### Directory structure

1. `iotdbwriter/`

   This directory is the code directory for the iotdbWriter plug-in. All the code in this directory is hosted in Apache IotDB's code base.

   The help documentation for the iOTdbWriter plug-in is here: `iotdbwriter/doc`

2. `init-env.sh`

   This script is used to build the DataX development environment. It does the following:

   1. Clone the DataX code base to the local PC.
   2. Soft link the 'iotdbWriter/' directory to the' DataX/ iotdbWriter 'directory.
   3. Add ' iotdbWriter ' to the 'DataX/pom. xml' file.
   4. dd iOTDBWriter-related packaging configuration to the 'DataX/package. xml' file.

  Once the script is executed, developers can go to the 'DataX/' directory and start developing or compiling. Because of the soft link, any changes made to files in the 'DataX/ iotdbWriter' directory will be reflected in the 'iotdbWriter/' directory, making it easy for developers to submit code.

### Compile deployment

1. Run `init-env.sh`

2. Modify the code in `DataX/iotdbwriter` as required.

3. Compile iotdbWriter:

   ` mvn -U clean package assembly:assembly -Dmaven.test.skip=true`        

   Output in `target/datax/datax/`.

   If a plug-in dependency download error occurs during compilation, such as hdFSReader, hdFSWriter, tsdbWriter and OscarWriter, it is because additional JAR packages are required. If you do not need these plug-ins, you can remove the modules of these plug-ins in 'DataX/pom.xml'.


### Transfer data using datax

Go to`target/datax/datax/` and run `python bin/datax.py ${USER_DEFINE}.json` can start a Datax transfer job.

${USER_DEFINE}.json is the datax job configuration file.



## Plugin details

###  IOTDBWriter  plugin documentation

#### 1 Quick introduction

IOTDBWriter supports writing large amounts of data to the IOTDB.

####  2 Implementation principle

IOTDBWriter imports data in Session mode through IOTDB. The IOTDBWriter caches the data read by 'Reader' in memory and then imports the data to IOTDB in batches.

####  3  Function Description

#####  3.1 Configuration example

Here is a configuration file that reads data from Stream and imports it into IOTDB.

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "guangzhou",
                "type": "string"
              },
              {
                "random": "2014-07-07 00:00:00, 2016-07-07 00:00:00",
                "type": "date"
              },
              {
                "random": "0, 19890604",
                "type": "long"
              },
              {
                "random": "2, 10",
                "type": "bool"
              },
              {
                "random": "10, 23410",
                "type": "DOUBLE"
              }
            ],
            "sliceRecordCount": 100000
          }
        },
        "writer": {
          "name": "iotdbwriter",
          "parameter": {
            "column": [
              {
                "name": "loc",
                "type": "text"
              },
              {
                "name": "timeseries",
                "type": "date"
              },
              {
                "name": "s2",
                "type": "int64"
              },
              {
                "name": "s3",
                "type": "boolean"
              },
              {
                "name": "s4",
                "type": "double"
              }
            ],
            "username": "root",
            "password": "root",
            "host": "localhost",
            "port": 6667,
            "deviceId": "root.sg4.d1",
            "batchSize": 1000
          }
        }
      }
    ]
  }
}

```

##### 3.2 参数说明

* **username**

  - Description: User name for accessing the IotDB database
  - Mandatory: Yes
  - Default value: None

* **password**

  - Description: Password for accessing the IotDB database
  - Mandatory: Yes
  - Default value: None

* **deviceId**

  - Description: Device ID written to the IotDB.
  - Mandatory: Yes
  - Default value: None


* **host**

  - Description: IP address for accessing the IotDB database.
  - Mandatory: Yes
  - Default value: None

* **port**

  - Description: Port for accessing the IotDB database.
  - Mandatory: Yes
  - Default value: None

* **column**

  - Description: ** Specifies the fields **to be written to. Separate the fields with commas (,). The field must contain a field named **timeseries** of type date. The field types are the write data types supported by iotDB, including: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT. Example: "column": [{"name": "s2", "type": "int64"}].
  - Mandatory: Yes
  - Default value: No

* **batchSize**

  - Description: Specifies the batch number of data to be written at a time
  - Mandatory: No
  - Default value: None


