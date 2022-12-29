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

# Backup-Tool
A tool for import and export data from iotdb,including timeseries structure and those data.

## Project structure
This project is divided into two sub-projects:backup-core and backup-command projects;  
The backup-core project contains the main business logic and implementation methods, this project can provides external services in the form of jar packages.    
The backup-command project is a tool class package,The function implementation depends on the core project and provides a command line tool.  

## how to compile
Precondition:
1. Java >= 1.8, the JAVA_HOME environment variable needs to be configured  
2. Maven >= 3.6  
you can use the command below to compile this project or install it:  
````
mvn clean package;  
mvn install;  
mvn test;    
````
ps: you can add '-DskipTests' to skip those tests.    
Since the core-jar has not been placed on the remote repository, to compile the tools smoothly, you first need to execute mvn install -DskipTests.    
The integration tests need a real iotdbserver，you can config the server in "test/resource/sesseinConfig.properties"

## Features    
- based on iotdb-session  
- Pipeline-based import and export tool, easy to expand  
    1. ````
       Pipeline design document: https://apache-iotdb.feishu.cn/wiki/wikcn8zaxOegcQ4vBASwtmbmE4f
       ````
- Based on project reactor framework, multi-track execution, support back pressure mechanism  
- Data export takes the device as the smallest unit  
  1. The data export takes the device as the smallest unit, that is, one device corresponds to one file, and this file contains all the measurement data of the device. Because the smallest export unit is the device, the corresponding path should be the smallest device-level path.  
- Data can be exported from one or more storage groups  
- Guarantee the consistency of time series structure  
  1. It can ensure that the structure of the time series will not change during the import and export process. When exporting data, you can choose whether to export the structure information of the time series. This information will be saved in a file separately; it records the properties of the time series in detail. The type of each field, the compression format, whether it is an aligned time series, etc., create a corresponding time series according to this file when importing, to ensure that the time series structure will not change.  
- Support incremental/full export  
  1. The "where" parameter supports the selected time range, and currently does not support non-time parameters  
- Supports the export of more than 1000 time series  
  1. Since this tool relies on iotdb-session, and session queries are limited, the time series results of a one-time query are not allowed to exceed 1000. This tool solves this problem and is the time series in the query results. More than 1000 can also be exported normally.  
- The core function provides jar package, which is convenient for third-party integration  
- Rich export formats:  
  1. The export file that can be viewed, provides two file formats : sql and csv, which can be viewed directly  
  2. Unviewable export files, export files in compressed format, take up less disk space, suitable for some backup work; now supported compression formats TSFILE、SNAPPY, GZIP, etc.  

## Export
Export command tool: backup-export.bat/backup-export.sh
````
 -h  // iotdb host address
 -p  // port
 -u  // username
 -pw // password
 -f  // fileFolder
 -i  // iotdbPath
 -sy // file generation strategy, exclude export tsfile,others could use this param
 -se // Whether a time series structure is required,if true the tool will export a new file which will record the timeseries structure
 -c  // compress type : SQL、CSV、SNAPPY、GZIP、LZ4
 -w  // where 
 -vn // virtualNum export tsfile could use this param
 -pi // partitionInterval export tsfile could use this param
````

## Import
Import command tool: backup-import.bat/backup-import.sh
````
 -h  // iotdb host address
 -p  // port
 -u  // username
 -pw // password
 -f  // fileFolder
 -se // Whether a time series structure is required,if true the tool will export a new file which will record the timeseries structure
 -c  // compress type : SQL、CSV、SNAPPY、GZIP、LZ4
````

## example
Scenario description:
- Storage group:
  1. root.ln.company1
  2. root.ln.company2
- Timeseries:  
  1.root.ln.company1.diggingMachine.d1.position
  2.root.ln.company1.diggingMachine.d1.weight
  3.root.ln.company1.diggingMachine.d2.position
  4.root.ln.company1.diggingMachine.d2.weight
  5.root.ln.company2.taxiFleet.team1.mileage
  6.root.ln.company2.taxiFleet.team1.oilConsumption
  
 > There are two excavators d1 and d2 under the storage group root.ln.company1
 > There is a taxi fleet team1 under the storage group root.ln.company2
 ---
 > Export the d1 device, export the file to d:/company1/machine, select the second file strategy (extra file), need to generate a time series structure, the file format is gzip
 ````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\company1\\machine -i root.ln.company1.diggingMachine.d1 -sy true -se true -c gzip
 ````
> Export results:
> TIMESERIES_STRUCTURE.STRUCTURE records the time series structure
> CATALOG_COMPRESS.CATALOG records the correspondence between file names and device paths
> 1.gz.bin

--- 
> Export all devices under company1, export the file to d:/company1/machine, select the first file strategy, do not need to generate a time series structure, the file format is csv
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\company1\\machine -i root.ln.company1.** -sy false -se false -c csv
````
> Export results:
> d2.csv
> d1.csv

--- 
> Export all devices under root.ln, export files to d:/all/devices, select the first file strategy, need to generate time series structure, file format csv
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\all\\devices -i root.ln.** -sy false -se true -c csv -w "time > 1651036025230"
````
> Export results:
> TIMESERIES_STRUCTURE.STRUCTURE records the time series structure
> d2.csv
> d1.csv
> team1.csv

--- 
> Import data, specify the folder d:/all/devices to export data, need to import time series, file format csv
````
backup-import.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\all\\devices -se true -c csv
````
--- 
ps: export tsfile
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\tsfileexport\\A106 -i  root.yyy.** -se false -c tsfile -vn 3 -pi 604800
````
if you want to import tsfile,please use [Load External TsFile Tool](https://iotdb.apache.org/zh/UserGuide/V0.13.x/Write-And-Delete-Data/Load-External-Tsfile.html).