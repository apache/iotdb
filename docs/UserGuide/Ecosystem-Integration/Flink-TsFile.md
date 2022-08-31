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

## Flink-TsFile-Connector

### About Flink-TsFile-Connector

Flink-TsFile-Connector implements the support of Flink for external data sources of Tsfile type. 
This enables users to read and write  Tsfile by Flink via DataStream/DataSet API.

With this connector, you can

* load a single TsFile or multiple TsFiles(only for DataSet), from either the local file system or hdfs, into Flink
* load all files in a specific directory, from either the local file system or hdfs, into Flink

### Quick Start

#### TsFileInputFormat Example

1. create TsFileInputFormat with default RowRowRecordParser.

```java
String[] filedNames = {
	QueryConstant.RESERVED_TIME,
	"device_1.sensor_1",
	"device_1.sensor_2",
	"device_1.sensor_3",
	"device_2.sensor_1",
	"device_2.sensor_2",
	"device_2.sensor_3"
};
TypeInformation[] typeInformations = new TypeInformation[] {
	Types.LONG,
	Types.FLOAT,
	Types.INT,
	Types.INT,
	Types.FLOAT,
	Types.INT,
	Types.INT
};
List<Path> paths = Arrays.stream(filedNames)
	.filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
	.map(Path::new)
	.collect(Collectors.toList());
RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
QueryExpression queryExpression = QueryExpression.create(paths, null);
RowRowRecordParser parser = RowRowRecordParser.create(rowTypeInfo, queryExpression.getSelectedSeries());
TsFileInputFormat inputFormat = new TsFileInputFormat<>(queryExpression, parser);
```

2. Read data from the input format and print to stdout:

DataStream:

```java
StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
inputFormat.setFilePath("source.tsfile");
DataStream<Row> source = senv.createInput(inputFormat);
DataStream<String> rowString = source.map(Row::toString);
Iterator<String> result = DataStreamUtils.collect(rowString);
while (result.hasNext()) {
	System.out.println(result.next());
}
```

DataSet:

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
inputFormat.setFilePath("source.tsfile");
DataSet<Row> source = env.createInput(inputFormat);
List<String> result = source.map(Row::toString).collect();
for (String s : result) {
	System.out.println(s);
}
```

#### Example of TSRecordOutputFormat 

1. create TSRecordOutputFormat with default RowTSRecordConverter.

```java
String[] filedNames = {
	QueryConstant.RESERVED_TIME,
	"device_1.sensor_1",
	"device_1.sensor_2",
	"device_1.sensor_3",
	"device_2.sensor_1",
	"device_2.sensor_2",
	"device_2.sensor_3"
};
TypeInformation[] typeInformations = new TypeInformation[] {
	Types.LONG,
	Types.LONG,
	Types.LONG,
	Types.LONG,
	Types.LONG,
	Types.LONG,
	Types.LONG
};
RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
Schema schema = new Schema();
schema.extendTemplate("template", new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.TS_2DIFF));
schema.extendTemplate("template", new MeasurementSchema("sensor_2", TSDataType.INT64, TSEncoding.TS_2DIFF));
schema.extendTemplate("template", new MeasurementSchema("sensor_3", TSDataType.INT64, TSEncoding.TS_2DIFF));
RowTSRecordConverter converter = new RowTSRecordConverter(rowTypeInfo);
TSRecordOutputFormat<Row> outputFormat = new TSRecordOutputFormat<>(schema, converter);
```

2. write data via the output format:

DataStream:

```java
StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
senv.setParallelism(1);
List<Tuple7> data = new ArrayList<>(7);
data.add(new Tuple7(1L, 2L, 3L, 4L, 5L, 6L, 7L));
data.add(new Tuple7(2L, 3L, 4L, 5L, 6L, 7L, 8L));
data.add(new Tuple7(3L, 4L, 5L, 6L, 7L, 8L, 9L));
data.add(new Tuple7(4L, 5L, 6L, 7L, 8L, 9L, 10L));
data.add(new Tuple7(6L, 6L, 7L, 8L, 9L, 10L, 11L));
data.add(new Tuple7(7L, 7L, 8L, 9L, 10L, 11L, 12L));
data.add(new Tuple7(8L, 8L, 9L, 10L, 11L, 12L, 13L));
outputFormat.setOutputFilePath(new org.apache.flink.core.fs.Path(path));
DataStream<Tuple7> source = senv.fromCollection(
	data, Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG));
source.map(t -> {
	Row row = new Row(7);
	for (int i = 0; i < 7; i++) {
		row.setField(i, t.getField(i));
	}
	return row;
}).returns(rowTypeInfo).writeUsingOutputFormat(outputFormat);
senv.execute();
```

DataSet:

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
List<Tuple7> data = new ArrayList<>(7);
data.add(new Tuple7(1L, 2L, 3L, 4L, 5L, 6L, 7L));
data.add(new Tuple7(2L, 3L, 4L, 5L, 6L, 7L, 8L));
data.add(new Tuple7(3L, 4L, 5L, 6L, 7L, 8L, 9L));
data.add(new Tuple7(4L, 5L, 6L, 7L, 8L, 9L, 10L));
data.add(new Tuple7(6L, 6L, 7L, 8L, 9L, 10L, 11L));
data.add(new Tuple7(7L, 7L, 8L, 9L, 10L, 11L, 12L));
data.add(new Tuple7(8L, 8L, 9L, 10L, 11L, 12L, 13L));
DataSet<Tuple7> source = env.fromCollection(
	data, Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG));
source.map(t -> {
	Row row = new Row(7);
	for (int i = 0; i < 7; i++) {
		row.setField(i, t.getField(i));
	}
	return row;
}).returns(rowTypeInfo).write(outputFormat, path);
env.execute();
```


