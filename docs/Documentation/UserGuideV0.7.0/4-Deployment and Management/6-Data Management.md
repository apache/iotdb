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

# Chapter 4: Deployment and Management


## Data Management

In IoTDB, there are many kinds of data needed to be storage. In this section, we will introduce IoTDB's data storage strategy in order to give you an intuitive understanding of IoTDB's data management.

The data that IoTDB stores is divided into three categories, namely data files, system files, and pre-write log files.

### Data Files

Data files store all the data that the user wrote to IoTDB, which contains TsFile and other files. TsFile storage directory can be configured with the `tsfile_dir` configuration item (see [file layer](/#/Documents/latest/chap4/sec2) for details). Other files can be configured through [data_dir](/#/Documents/latest/chap4/sec2) configuration item (see [Engine Layer](/#/Documents/latest/chap4/sec2) for details).

In order to better support users' storage requirements such as disk space expansion, IoTDB supports multiple file directorys storage methods for TsFile storage configuration. Users can set multiple storage paths as data storage locations( see [tsfile_dir](/#/Documents/latest/chap4/sec2) configuration item), and you can specify or customize the directory selection policy (see [mult_dir_strategy](/#/Documents/latest/chap4/sec2) configuration item for details).

### System Files

System files include restore files and schema files, which store metadata information of data in IoTDB. It can be configured through the `sys_dir` configuration item (see [System Layer](/#/Documents/latest/chap4/sec2) for details).

### Pre-write Log Files

Pre-write log files store WAL files. It can be configured through the `wal_dir` configuration item (see [System Layer](/#/Documents/latest/chap4/sec2) for details).

### Example of Setting Data storage Directory

For a clearer understanding of configuring the data storage directory, we will give an excample in this section.

All data directory paths involved in storage directory setting are: data_dir, tsfile_dir, mult_dir_strategy, sys_dir, and wal_dir, which refer to data files, stroage strategy, system files, and pre-write log files. You can choose to configure the items you'd like to change, otherwise, you can use the system default configuration item without any operation.

Here we give an example of a user who configures all five configurations mentioned above. The configuration items are as follow:

```
data_dir = D:\\iotdb\\data\\data  
tsfile_dir = E:\\iotdb\\data\\data1, data\\data2, F:\\data3  mult_dir_strategy = MaxDiskUsableSpaceFirstStrategy sys_dir = data\\system wal_dir = data

```
After setting the configuration, the system will:

* Save all data files except TsFile in D:\\iotdb\\data\\data
* Save TsFile in E:\\iotdb\\data\\data1, $IOTDB_HOME\\data\\data2 and F:\\data3. And the choosing strategy is `MaxDiskUsableSpaceFirstStrategy`, that is every time data writes to the disk, the system will automatically select a directory with the largest remaining disk space to write data.
* Save system data in $IOTDB_HOME\\data\\system
* Save WAL data in $IOTDB_HOME\\data

> Note:
> 
> If you change directory names in tsfile_dir, the newer name and the older name should be one-to-one correspondence. Also, the files in the older directory needs to be moved to the newer directory. 
> 
> If you add some directorys in tsfile_dir, IoTDB will add the path automatically. Nothing needs to do by your own. 

For example, modify the tsfile_dir to:

```
tsfile_dir = D:\\data4, E:\\data5, F:\\data6
```

You need to move files in E:\iotdb\data\data1 to D:\data4, move files in %IOTDB_HOME%\data\data2 to E:\data5, move files in F:\data3 to F:\data6. In this way, the system will operation normally.
