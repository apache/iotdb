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
# nifi-iotdb-bundle

## Apache NiFi Introduction

Apache NiFi is an easy to use, powerful, and reliable system to process and distribute data.

Apache NiFi supports powerful and scalable directed graphs of data routing, transformation, and system mediation logic.

Apache NiFi includes the following capabilities:

* Browser-based user interface
    * Seamless experience for design, control, feedback, and monitoring
* Data provenance tracking
    * Complete lineage of information from beginning to end
* Extensive configuration
    * Loss-tolerant and guaranteed delivery
    * Low latency and high throughput
    * Dynamic prioritization
    * Runtime modification of flow configuration
    * Back pressure control
* Extensible design
    * Component architecture for custom Processors and Services
    * Rapid development and iterative testing
* Secure communication
    * HTTPS with configurable authentication strategies
    * Multi-tenant authorization and policy management
    * Standard protocols for encrypted communication including TLS and SSH

## PutIoTDBRecord

This is a processor that reads the content of the incoming FlowFile as individual records using the configured 'Record Reader' and writes them to Apache IoTDB using native interface.

### Properties of PutIoTDBRecord

| property      | description                                                                                                                                                                                                                                                                                                   | default value | necessary |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------- | --------- |
| Host          | The host of IoTDB.                                                                                                                                                                                                                                                                                            | null          | true      |
| Port          | The port of IoTDB.                                                                                                                                                                                                                                                                                            | 6667          | true      |
| Username      | Username to access the IoTDB.                                                                                                                                                                                                                                                                                 | null          | true      |
| Password      | Password to access the IoTDB.                                                                                                                                                                                                                                                                                 | null          | true      |
| Prefix        | The Prefix begin with root. that will be add to the tsName in data.  <br /> It can be updated by expression language.                                                                                                                                                                                                | null          | true      |
| Time          | The name of time field                                                                                          | null          | true      |
| Record Reader | Specifies the type of Record Reader controller service to use <br />for parsing the incoming data and determining the schema.                                                                                                                                                                                 | null          | true      |
| Schema        | The schema that IoTDB needs doesn't support good by NiFi.<br/>Therefore, you can define the schema here. <br />Besides, you can set encoding type and compression type by this method.<br />If you don't set this property, the inferred schema will be used.<br /> It can be updated by expression language. | null          | false     |
| Aligned       | Whether using aligned interface?  It can be updated by expression language.                                                                                                                                                                                                                                   | false         | false     |
| MaxRowNumber  | Specifies the max row number of each tablet.  It can be updated by expression language.                                                                                                                                                                                                                       | 1024          | false     |

### Inferred Schema of Flowfile

There are a couple of rules about flowfile:

1. The flowfile can be read by `Record Reader`.
2. The schema of flowfile must contain a time field with name set in Time property.
3. The data type of time must be `STRING` or `LONG`.
4. Fields excepted time must start with `root.`.
5. The supported data types are `INT`, `LONG`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`.

### Convert Schema by property

As mentioned above, converting schema by property which is more flexible and stronger than inferred schema. 

The structure of property `Schema`:

```json
{
	"fields": [{
		"tsName": "s1",
		"dataType": "INT32",
		"encoding": "RLE",
		"compressionType": "GZIP"
	}, {
		"tsName": "s2",
		"dataType": "INT64",
		"encoding": "RLE",
		"compressionType": "GZIP"
	}]
}
```

**Note**

1. The first column must be `Time`. The rest must be arranged in the same order as in `field` of JSON.
1. The JSON of schema must contain `timeType` and `fields`.
2. There are only two options `LONG` and `STRING` for `timeType`.
3. The columns `tsName` and `dataType` must be set.
4. The property `Prefix` will be added to tsName as the field name when add data to IoTDB.
5. The supported `dataTypes` are `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TEXT`.
6. The supported `encoding` are `PLAIN`, `DICTIONARY`, `RLE`, `DIFF`, `TS_2DIFF`, `BITMAP`, `GORILLA_V1`, `REGULAR`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE`.
7. The supported `compressionType` are `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, `SDT`, `PAA`, `PLA`, `LZ4`, `ZSTD`, `LZMA2`.

## Relationships

| relationship | description                                          |
| ------------ | ---------------------------------------------------- |
| success      | Data can be written correctly or flow file is empty. |
| failure      | The shema or flow file is abnormal.                  |


## QueryIoTDBRecord

This is a processor that reads the sql query from the incoming FlowFile and using it to query the result from IoTDB using native interface. Then it use the configured 'Record Writer' to generate the flowfile

### Properties of QueryIoTDBRecord

| property      | description                                                                                                                                                                                                                                                                                                | default value | necessary |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------| --------- |
| Host          | The host of IoTDB.                                                                                                                                                                                                                                                                                         | null      | true      |
| Port          | The port of IoTDB.                                                                                                                                                                                                                                                                                         | 6667      | true      |
| Username      | Username to access the IoTDB.                                                                                                                                                                                                                                                                              | null      | true      |
| Password      | Password to access the IoTDB.                                                                                                                                                                                                                                                                              | null      | true      |
| Record Writer | Specifies the Controller Service to use for writing results to a FlowFile. The Record Writer may use Inherit Schema to emulate the inferred schema behavior, i.e. An explicit schema need not be defined in the writer, and will be supplied by the same logic used to infer the schema from the column types. | null      | true      |
| iotdb-query        | The IoTDB query to execute. <br> Note: If there are incoming connections, then the query is created from incoming FlowFile's content otherwise"it is created from this property.                                                                                                                          | null      | false     |
| iotdb-query-chunk-size  | Chunking can be used to return results in a stream of smaller batches (each has a partial results up to a chunk size) rather than as a single response. Chunking queries can return an unlimited number of rows. Note: Chunking is enable when result chunk size is greater than 0                         | 0         | false     |


## Relationships

| relationship | description                                          |
| ------------ | ---------------------------------------------------- |
| success      | Data can be written correctly or flow file is empty. |
| failure      | The shema or flow file is abnormal.                  |