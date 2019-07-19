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

# Chapter 3: Operation Manual

## Data Model Selection

Before importing data to IoTDB, we first select the appropriate data storage model according to the [sample data](/#/Documents/latest/chap3/sec1), and then create the storage group and timeseries using [SET STORAGE GROUP](/#/Documents/latest/chap5/sec1) statement and [CREATE TIMESERIES](/#/Documents/latest/chap5/sec1) statement respectively.

### Storage Model Selection
According to the data attribute layers described in [sample data](/#/Documents/latest/chap3/sec1), we can express it as an attribute hierarchy structure based on the coverage of attributes and the subordinate relationship between them, as shown in Figure 3.1 below. Its hierarchical relationship is: power group layer - power plant layer - device layer - sensor layer. ROOT is the root node, and each node of sensor layer is called a leaf node. In the process of using IoTDB, you can directly connect the attributes on the path from ROOT node to each leaf node with ".", thus forming the name of a timeseries in IoTDB. For example, The left-most path in Figure 3.1 can generate a timeseries named `ROOT.ln.wf01.wt01.status`.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577327-7aa50780-1ef4-11e9-9d75-cadabb62444e.jpg">

**Figure 3.1 Attribute hierarchy structure**</center>

After getting the name of the timeseries, we need to set up the storage group according to the actual scenario and scale of the data. Because in the scenario of this chapter data is usually arrived in the unit of groups (i.e., data may be across electric fields and devices), in order to avoid frequent switching of IO when writing data, and to meet the user's requirement of physical isolation of data in the unit of  groups, we set the storage group at the group layer.

### Storage Group Creation
After selecting the storage model, according to which we can set up the corresponding storage group. The SQL statements for creating storage groups are as follows:

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already set as a storage group, the path is then not allowed to be set as a storage group. For example, it is not feasible to set `root.ln.wf01` as a storage group when there exist two storage groups `root.ln` and `root.sgcc`. The system will give the corresponding error prompt as shown below:

```
IoTDB> set storage group to root.ln.wf01
Msg: org.apache.iotdb.exception.MetadataErrorException: org.apache.iotdb.exception.PathErrorException: The prefix of root.ln.wf01 has been set to the storage group.
```

### Show Storage Group
After the storage group is created, we can use the [SHOW STORAGE GROUP](/#/Documents/latest/chap5/sec1) statement to view all the storage groups. The SQL statement is as follows:

```
IoTDB> show storage group
```

The result is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577338-84c70600-1ef4-11e9-9dab-605b32c02836.jpg"></center>

### Timeseries Creation
According to the storage model selected before, we can create corresponding timeseries in the two storage groups respectively. The SQL statements for creating timeseries are as follows:

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

It is worth noting that when in the CRATE TIMESERIES statement the encoding method conflicts with the data type, the system will give the corresponding error prompt as shown below:

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

Please refer to [Encoding](/#/Documents/latest/chap2/sec3) for correspondence between data type and encoding.

### Show Timeseries

Currently, IoTDB supports two ways of viewing timeseries:

* SHOW TIMESERIES statement presents all timeseries information in JSON form 
* SHOW TIMESERIES <`Path`> statement returns all timeseries information and the total number of timeseries under the given <`Path`>  in tabular form. timeseries information includes: timeseries path, storage group it belongs to, data type, encoding type.  <`Path`> needs to be a prefix path or a path with star or a timeseries path. SQL statements are as follows:

```
IoTDB> show timeseries root
IoTDB> show timeseries root.ln
```

The results are shown below respectly:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577347-8db7d780-1ef4-11e9-91d6-764e58c10e94.jpg"></center>
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577359-97413f80-1ef4-11e9-8c10-53b291fc10a5.jpg"></center>

It is worth noting that when the queried path does not exist, the system will return no timeseries.  

### Precautions

Version 0.7.0 imposes some limitations on the scale of data that users can operate:

Limit 1: Assuming that the JVM memory allocated to IoTDB at runtime is p and the user-defined size of data in memory written to disk ([group\_size\_in\_byte](/#/Documents/latest/chap4/sec2)) is Q, then the number of storage groups should not exceed p/q.

Limit 2: The number of timeseries should not exceed the ratio of JVM memory allocated to IoTDB at run time to 20KB.
