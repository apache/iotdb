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
- The codes for two deployments, M4-UDF and M4-LSM, are available in this repository.
    - M4-UDF: M4-UDF is implemented in [server/src/main/java/org/apache/iotdb/db/query/udf/builtin/UDTFM4MAC.java](server/src/main/java/org/apache/iotdb/db/query/udf/builtin/UDTFM4MAC.java). The document of the M4 function is available on the product [website](https://iotdb.apache.org/UserGuide/Master/Operators-Functions/Sample.html#m4-function) of Apache IoTDB.
    - M4-LSM: The candidate generation and verification framework is implemented in [server/src/main/java/org/apache/iotdb/db/query/dataset/groupby/LocalGroupByExecutor4CPV.java](server/src/main/java/org/apache/iotdb/db/query/dataset/groupby/LocalGroupByExecutor4CPV.java). The step regression index is implemented in [tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/StepRegress.java](tsfile/src/main/java/org/apache/iotdb/tsfile/file/metadata/statistics/StepRegress.java).
    - Some integration tests for correctness are in [server/src/test/java/org/apache/iotdb/db/integration/m4/MyTest1.java](server/src/test/java/org/apache/iotdb/db/integration/m4/MyTest1.java).
- The codes, data and scripts for experiments are in [another GitHub repository](https://anonymous.4open.science/r/M4-visualization-exp-D659) for reproducibility.
- For the README of [Apache IoTDB](https://iotdb.apache.org/) itself, please see [README_IOTDB.md](README_IOTDB.md). To build this repository, run `mvn clean package -DskipTests -pl -distribution`.
