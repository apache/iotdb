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

# Batch Data Load

In different scenarios, the IoTDB provides a variety of methods for importing data in batches. This section describes the two most common methods for importing data in CSV format and TsFile format.

## TsFile Batch Load

TsFile is the file format of time series used in IoTDB. You can directly import one or more TsFile files with time series into another running IoTDB instance through tools such as CLI. For details, see [TsFile Load Tool](../Maintenance-Tools/Load-Tsfile.md) [TsFile Export Tools](../Maintenance-Tools/TsFile-Load-Export-Tool.md).

## CSV Batch Load

CSV stores table data in plain text. You can write multiple formatted data into a CSV file and import the data into the IoTDB in batches. Before importing data, you are advised to create the corresponding metadata in the IoTDB. Don't worry if you forget to create one, the IoTDB can automatically infer the data in the CSV to its corresponding data type, as long as you have a unique data type for each column. In addition to a single file, the tool supports importing multiple CSV files as folders and setting optimization parameters such as time precision. For details, see [CSV Load Export Tools](../Maintenance-Tools/CSV-Tool.md).
