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
# Overview

## About UDF Library

For applications based on time series data, data quality is vital.
**UDF Library** is IoTDB User Defined Functions (UDF) about data quality, including data profiling, data quality evalution and data repairing.
It effectively meets the demand for data quality in the industrial field.

## Quick Start

1. Download the JAR with all dependencies and the script of registering UDF.
2. Copy the JAR package to `ext\udf` under the directory of IoTDB system.
3. Run `sbin\start-server.bat` (for Windows) or `sbin\start-server.sh` (for Linux or MacOS) to start IoTDB server.
4. Copy the script to the directory of IoTDB system (under the root directory, at the same level as `sbin`), modify the parameters in the script if needed and run it to register UDF.

## Contact

+ Email: iotdb-quality@protonmail.com

# Download

You can download the following files:

<table>
    <tr>
        <th align="center">Version</th>
        <th align="center">Jar with all dependencies</th>
        <th align="center" colspan="2">Script of registering UDF</th>
        <th align="center" colspan="2">User Manual</th>
        <th align="center">Supported IoTDB Version</th>
    </tr>
    <tr>
        <td align="center">In progress</td>
        <td align="center"><a href="https://thulab.github.io/iotdb-quality/download/iotdb-quality-2.0.0-SNAPSHOT-jar-with-dependencies.jar">Jar with all dependencies</a></td>
        <td align="center"><a href="https://thulab.github.io/iotdb-quality/download/register-UDF.bat">Windows</a></td>
        <td align="center"><a href="https://thulab.github.io/iotdb-quality/download/register-UDF.sh">Linux/MacOS</a></td>
        <td align="center"><a href="https://thulab.github.io/iotdb-quality/download/UserManual_en.pdf">English</a></td>
        <td align="center"><a href="https://thulab.github.io/iotdb-quality/download/UserManual_zh.pdf">Chinese</a></td>
        <td align="center">>= 0.12.0</td>
    </tr>
</table>
