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
you can use the command below to compile this project or install it:  
````
mvn clean package;  
mvn install;  
mvn test;   
mvn verify;   
````
ps: you can add '-DskipTests' to skip those tests.    
Since the core-jar has not been placed on the remote repository, to compile the tools smoothly, you first need to execute mvn install -DskipTests.    
The integration tests need a real iotdbserver，you can config the server in "test/resource/sesseinConfig.properties"

## Features    
- Iotdb-session based  
- Pipeline-based import and export tool  
- React based  
- support more situations,such as export and import a storage,export and import the timeseries structure,the device's align properties  

## How to use it
````
-  backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\validate_test\\83 -i root.** -sy true -se true -c gzip  
-  backup-import.1.bat -h 127.0.0.1 -p 6667 -u root -pw root -f D:\validate_test\83 -se true -c gzip
- -h  // iotdb host address
- -p  // port
- -u  // username
- -pw // password
- -f  // fileFolder
- -sy // Whether a time series structure is required,if true the tool will export a new file which will record the timeseries structure
- -se // file generation strategy
- -c  // compress type : SQL、CSV、SNAPPY、GZIP、LZ4
- -w  // where 
- -i  // iotdbPath
````