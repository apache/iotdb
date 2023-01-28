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

# M4-LSM 
- The code of two M4-LSM deployments, M4-UDF and M4-LSM, is available in this repository.
    - M4-UDF: `org.apache.iotdb.db.query.udf.builtin.UDTFM4MAC`.  
    The document of the M4 aggregation function (implemented as a UDF based on MA) has been released on the official [website](https://iotdb.apache.org/UserGuide/Master/UDF-Library/M4.html#m4-2) of Apache IoTDB.
    - M4-LSM: `org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor4CPV`
    - Some integration tests for correctness are in `org.apache.iotdb.db.integration.m4.MyTest1/2/3/4`.
- The experiment-related code, data and scripts are in [another GitHub repository](https://anonymous.4open.science/r/M4-visualization-exp-42E6/README.md) for easier reproducibility.
- For the README of Apache IoTDB itself, please see [README_IOTDB.md](README_IOTDB.md).
