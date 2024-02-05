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
- The codes for ILTS and baselines used in comparative experiments are available in this repository.
    - ILTS is implemented in [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_ILTS.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_ILTS.java).
    - LTTB is implemented in [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_LTTB.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_LTTB.java).
    - MinMax is implemented in [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_MinMax.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_MinMax.java).
    - M4 is implemented in [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_M4.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_M4.java).
    - MinMaxLTTB is implemented in [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_MinMaxPreselection.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\LocalGroupByExecutorTri_MinMaxPreselection.java) and [server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\GroupByWithoutValueFilterDataSet.java](server\src\main\java\org\apache\iotdb\db\query\dataset\groupby\GroupByWithoutValueFilterDataSet.java).
    - Some integration tests for correctness are in [server/src/test/java/org/apache/iotdb/db/integration/tri/MyTest_ILTS.java](server/src/test/java/org/apache/iotdb/db/integration/tri/MyTest_ILTS.java).
- For the README of [Apache IoTDB](https://iotdb.apache.org/) itself, please see [README_IOTDB.md](README_IOTDB.md). To build this repository, run `mvn clean package -DskipTests -pl -distribution`.
