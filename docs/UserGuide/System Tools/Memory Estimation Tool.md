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

# Memory Estimation Tool

## Introduction

This tool calculates the minimum memory for writing to meet specific workload through a number of parameters input by users. (Memory in IoTDB is divided into three parts: write memory, read memory and reserve memory. Write memory is used for data write allocation. The ratio of the three can be set in the configuration file) The unit of result is in GB.

## Input parameters

When using this tool, the parameters needed to be input are as follows:

<table>
   <tr>
      <td>Parameter</td>
      <td>Parameter Description</td>
      <td>Example</td>
      <td>Necessary</td>
   </tr>
   <tr>
      <td>-sg | --storagegroup &lt;storage group number&gt;</td>
      <td>storage group number</td>
      <td>-sg 20</td>
      <td>true</td>
   </tr>
   <tr>
      <td>-ts | --timeseries &lt;total timeseries number&gt;</td>
      <td>total timeseries number</td>
      <td>-ts 10000</td>
      <td>true</td>
   </tr>
   <tr>
      <td>-mts | --maxtimeseries &lt;max timeseries&gt;</td>
      <td>maximum number of timeseries among storage groups.If the time series are evenly distributed in the storage group, this parameter may not be set.</td>
      <td>-mts 10000</td>
      <td>false</td>
   </tr>

</table>

In memory estimation, if the calculation takes a long time, the tool will show the running progress below, which is convenient for users to master the progress.

## Usage

Users can use the tool using scripts under the `IOTDB_HOME/bin`folder.
For Linux and Mac OS X users:
* Assume that there are 20 storage groups, 100k timeseries and timeseries are evenly distributed in the storage groups:
```
  Shell >$IOTDB_HOME/bin/memory-tool.sh calmem -sg 20 -ts 100000
```
* Assume that there are 20 storage groups, 100k timeseries and maximum timeseries number among storage groups is 50000:
```
  Shell >$IOTDB_HOME/bin/memory-tool.sh calmem -sg 20 -ts 100000 -mts -50000
```

For Windows users:
* Assume that there are 20 storage groups, 100k timeseries and timeseries are evenly distributed in the storage groups:
```
  Shell >$IOTDB_HOME\bin\memory-tool.bat calmem -sg 20 -ts 100000
```
* Assume that there are 20 storage groups, 100k timeseries and maximum timeseries number among storage groups is 50000:
```
  Shell >$IOTDB_HOME\bin\memory-tool.bat calmem -sg 20 -ts 100000 -mts -50000
```

