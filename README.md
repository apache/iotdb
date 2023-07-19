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
    "AS IS" BAstepSIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# M4-LSM 
- The codes for two deployments, M4 and M4-LSM, are available in this repository.
    - M4: M4 is implemented in [server/src/main/java/org/apache/iotdb/db/query/udf/builtin/UDTFM4MAC.java](server/src/main/java/org/apache/iotdb/db/query/udf/builtin/UDTFM4MAC.java).
    - M4-LSM: The candidate generation and verification framework is implemented in [server/src/main/java/org/apache/iotdb/db/query/dataset/groupby/LocalGroupByExecutor4CPV.java](server/src/main/java/org/apache/iotdb/db/query/dataset/groupby/LocalGroupByExecutor4CPV.java). The time index with step regression is implemented in [tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/StepRegress.java](tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/StepRegress.java). The point pruning with value regression is implemented in [tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/ValueIndex.java](tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/ValueIndex.java). 
    - Some integration tests for correctness are in [server/src/test/java/org/apache/iotdb/db/integration/m4/MyTest1.java](server/src/test/java/org/apache/iotdb/db/integration/m4/MyTest1.java).
- For the README of [Apache IoTDB](https://iotdb.apache.org/) itself, please see [README_IOTDB.md](README_IOTDB.md). To build this repository, run `mvn clean package -DskipTests -pl -distribution`.
