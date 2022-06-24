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

# Apache IoTDB

[![Main Mac and Linux](https://github.com/apache/iotdb/actions/workflows/main-unix.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-unix.yml)
[![Main Win](https://github.com/apache/iotdb/actions/workflows/main-win.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-win.yml)
[![coveralls](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)


Apache IoTDB (Database for Internet of Things) is an IoT native database with high performance for 
data management and analysis, deployable on the edge and the cloud. Due to its light-weight 
architecture, high performance and rich feature set together with its deep integration with 
Apache Hadoop, Spark and Flink, Apache IoTDB can meet the requirements of massive data storage, 
high-speed data ingestion and complex data analysis in the IoT industrial fields.

# Node.js Native API

## How to Use the Session Module

### Prerequisites

`Node` version 16.15.0 or later and `tsc` version 4.7.2 or later are preferred.

`Thrift` version 0.16.0 or later is required for Unix.

### Compile the thrift library

In the root of IoTDB's source code folder,  run `mvn clean generate-sources -pl client-nodejs -am`.

This will automatically delete and repopulate the folder `iotdb/thrift` with the generated rpc files.
This folder is ignored from git and should **never be pushed to git!**

**Notice:** Since the version 0.14.1 thrift will compile binary type declaration to string rather than Buffer, we need to set the property <thrift.version> in pom.xml file to 0.16.0 for Windows. 

### Build the client-nodejs module

In the root of client-nodejs module,  run `npm install` to download dependencies of the project.

In the root of client-nodejs module,  run `tsc -p iotdb && Xcopy /E /I /Y iotdb\thrift dist\thrift && copy /Y package.json dist && copy /Y LICENSE dist` for Windows or `tsc -p iotdb && cp -r iotdb/thrift dist && cp package.json dist && cp LICENSE dist` for Unix.

### Session Client & Example

Thrift interface is packed up in the file `client-nodejs/dist/Session.js`. Additionally, an example file `client-nodejs/example/SessionExample.js`
of how to use the session module is also provided.
The following is a snippet to demonstrate how to use the session module:

```javascript
const {TSDataType, TSEncoding, Compressor} = require("./dist/utils/IoTDBConstants");
const session = require("./dist/Session");

async function sessionExample() {

    const s = new session.Session("127.0.0.1", 6667);
    await s.open(false);

    await s.execute_non_query_statement(
        "insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)"
    );
    
    const res = await s.execute_query_statement("SELECT * FROM root.*");
    while (res.has_next()){
        console.log(res.next());
    }
    
    s.close();
}

sessionExample();
```

### Creating Session Connection

* Initialize a Session

```javascript
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
const s = new session.Session(ip, port_, username_, password_);
```

* Open a session, with a parameter to specify whether to enable RPC compression

```javascript
await s.open(false);
```

**Notice:** this RPC compression status of client must comply with that of IoTDB server

### Set Storage Group

This API can create a storage group.

```javascript
// set storage group root.sg_test_01
s.set_storage_group("root.sg_test_01"); 
```

### Delete Storage Group

This API can delete a single storage group.

```javascript
// delete storage group root.sg_test_01
s.delete_storage_group("root.sg_test_01"); 
```

### Delete Storage Groups

This API can delete multiple storage groups.

```javascript
// delete storage groups root.sg_test_02 and root.sg_test_03
s.delete_storage_groups(["root.sg_test_02", "root.sg_test_03"]);
```

### Set Time Series

This API can create a time series and set datatype, encoding and compressor methods.

```javascript
// create time series root.sg_test_01.d_01.s_01 and set datatype, encoding and compressor methods.
s.create_time_series(
    "root.sg_test_01.d_01.s_01", 
    TSDataType.BOOLEAN, 
    TSEncoding.PLAIN, 
    Compressor.SNAPPY
);
```

### Set Multiple Time Series with One API Call

This API can set multiple time series with a single method call.

```javascript
// list of time series path
let ts_path_lst_ = [
    "root.sg_test_01.d_01.s_03",
    "root.sg_test_01.d_01.s_04",
    "root.sg_test_01.d_01.s_05",
    "root.sg_test_01.d_01.s_06",
    "root.sg_test_01.d_01.s_07",
    "root.sg_test_01.d_01.s_08"
];
// list of date type
let data_type_lst_ = [
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT
];
// list of encoding
let encoding_lst_ = [];
for(let i in data_type_lst_){
    encoding_lst_.push(TSEncoding.PLAIN);
}
// list of compressor
let compressor_lst_ = [];
for(let i in data_type_lst_){
    compressor_lst_.push(Compressor.SNAPPY);
}
// set multiple time series with API:create_multi_time_series
s.create_multi_time_series(
    ts_path_lst_, 
    data_type_lst_, 
    encoding_lst_, 
    compressor_lst_
);
```
### Delete Time Series

This API can delete multiple time series.

```javascript
// delete time series root.sg_test_01.d_01.s_07 and root.sg_test_01.d_01.s_08
s.delete_time_series(
    [
        "root.sg_test_01.d_01.s_07",
        "root.sg_test_01.d_01.s_08"
    ]
);
```

### Check the Existence of Time Series

This API can verify the existence of time series and return boolean data.

```javascript
// time series root.sg_test_01.d_01.s_09 does not exist, return false
console.log(
    "s_09 expecting false, checking result: ",
    s.check_time_series_exists("root.sg_test_01.d_01.s_09")
);
```

### Insert One Record into the Database

This API can insert one record into the database.

```javascript
let measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"];
let values_ = [true, 188, 188888888n, 188.88, 18888.8888, "test_record"];
let data_types_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT
];
await s.insert_record("root.sg_test_01.d_01", 1, measurements_, data_types_, values_);
```

### Insert Multiple Records into the Database

This API can insert multiple records into the database.

```javascript
let measurements_list_ = [
    ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
    ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"],
];
let values_list_ = [
    [false, 22, 3333333n, 4.44, 55.111111111, "test_records01"],
    [true, 77, 8888888n, 1.25, 88.125555555, "test_records02"],
];
let data_types_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT
];
let data_type_list_ = [data_types_, data_types_];
let device_ids_ = ["root.sg_test_01.d_01", "root.sg_test_01.d_01"];
await s.insert_records(
    device_ids_, [2, 3], measurements_list_, data_type_list_, values_list_
);
```

### Insert Records of One Device

This API can insert multiple rows, thereby reducing the overhead of networking. Specifically, this method will pack some insert requests in batch and send them to server.

```javascript
let time_list = [8, 7, 6];
let measurements_list = [
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
    ["s_01", "s_02", "s_03"],
];
let data_types_list = [
    [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
    [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
    [TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64],
];
let values_list = [[false, 22, 3333333333n], [true, 1, 6666666666n], [false, 15, 99999999999n]];

await s.insert_records_of_one_device(
    "root.sg_test_01.d_01", time_list, measurements_list, data_types_list, values_list
);
```

### Insert One Tablet

It is recommended to use insertTablet for higher write efficiency. 

Furthermore, this API supports inserting null values.

```javascript
let measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"];
let data_types_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
];
let values_ = [
    [false, 10, 11n, 1.1, 10011.1, "test01"],
    [true, 100, 11111n, 1.25, 101.0, "test02"],
    [false, 100, 1n, 188.1, 688.25, "test03"],
    [true, 0, 0n, 0, 6.25, "test04"],
];
let timestamps_ = [7n, 6n, 5n, 4n];
let tablet_ = new Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
);
await s.insert_tablet(tablet_);
```

### Insert Multiple Tablets

This API can insert multiple tablets.

```javascript
let measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"];
let data_types_ = [
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
];
let values_ = [
    [false, 10, 11n, 1.1, 10011.1, "test01"],
    [true, 100, 11111n, 1.25, 101.0, "test02"],
    [false, 100, 1n, 188.1, 688.25, "test03"],
    [true, 0, 0n, 0, 6.25, "test04"],
];
let tablet_01 = new Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, [9n, 8n, 11n, 10n]
);
let tablet_02 = new Tablet(
    "root.sg_test_01.d_01", measurements_, data_types_, values_, [15n, 14n, 13n, 12n]
);
await s.insert_tablets([tablet_01, tablet_02]);
```

### Delete Data

This API can delete all data before a specific timestamp in multiple time series.

```javascript
let paths_list = [
    "root.sg_test_01.d_01.s_01",
    "root.sg_test_01.d_01.s_02"
];
let timestamp = 10;
await s.delete_data(paths_list, timestamp);
```

### Execute Non-Query SQL Statement

The following statement executes non-query statement.

```javascript
// insert a record into root.sg_test_01.d_01 with timestamp=16 and s_02=188 
await s.execute_non_query_statement(
    "insert into root.sg_test_01.d_01(timestamp, s_02) values(16, 188)"
);
```

### Execute Query Statement

The following statement executes query statement and demonstrates the results.

```javascript
// execute query statement and return SessionDataSet
const res = await s.execute_query_statement("SELECT * FROM root.*");  
// display the result
while (res.has_next()){         
    console.log(res.next());
}
```

### Close Session Connection

The following statement will close session with the name 's'. 

```javascript
// close the session 's'
s.close();  
```