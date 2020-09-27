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
# Performance Tracing Tool

IoTDB supports the use of `TRACING` statements to enable and disable performance tracing of query statements, which is disabled by default. Users can use performance tracking tool to analyze potential performance problems in some queries. By default, the log files for performance tracing are stored in the directory `./data/tracing`.

Turn on Tracing：

`IoTDB> TRACING ON`

Turn off Tracing：

`IoTDB> TRACING OFF`

Since the cost of an IoTDB query mainly depends on the number of time series queried, the number of tsfile files accessed, the total number of chunks to be scanned and the average size of each chunk (the number of data points contained in the chunk). Therefore, the current performance analysis includes the following contents:

- Start time
- Query statement
- Number of series paths
- Number of tsfiles
- Number of sequence files
- Number of unsequence files
- Number of chunks
- Average size of chunks
- End time

## Example

For example, execute `select * from root`, the contents of the tracing log file will include the following contents:

```
Query Id: 2 - Start time: 2020-06-28 10:53:54.727
Query Id: 2 - Query Statement: select * from root
Query Id: 2 - Number of series paths: 3
Query Id: 2 - Number of tsfiles: 2
Query Id: 2 - Number of sequence files: 2
Query Id: 2 - Number of unsequence files: 0
Query Id: 2 - Number of chunks: 3
Query Id: 2 - Average size of chunks: 4113
Query Id: 2 - End time: 2020-06-28 10:54:44.059
```

In order to avoid disordered output information caused by multiple queries being executed at the same time, the Query ID is added before each output information. Users can use `grep "Query ID: 2" tracing.txt` to extract all tracing information of one query.
