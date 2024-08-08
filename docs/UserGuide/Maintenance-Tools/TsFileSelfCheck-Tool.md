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

# TsFileSelfCheck Tool
IoTDB Server provides the TsFile self check tool. At present, the tool can check the basic format of the TsFile file, the correctness of TimeseriesMetadata, and the correctness and consistency of the Statistics stored in each part of the TsFile.

## Use
Step 1：Create an object instance of TsFileSelfCheckTool class.

``` java
TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
```

Step 2：Call the check method of the self check tool. The first parameter path is the path of the TsFile to be checked. The second parameter is whether to check only the Magic String and Version Number at the beginning and end of TsFile.

``` java
tool.check(path, false);
```

* There are four return values of the check method.
* The return value is 0, which means that the TsFile self check is error-free.
* The return value is -1, which means that TsFile has inconsistencies in Statistics. There will be two specific exceptions, one is that the Statistics of TimeSeriesMetadata is inconsistent with the Statistics of the aggregated statistics of ChunkMetadata. The other is that the Statistics of ChunkMetadata is inconsistent with the Statistics of Page aggregation statistics in the Chunk indexed by it.
* The return value is -2, which means that the TsFile version is not compatible.
* The return value is -3, which means that the TsFile file does not exist in the given path.