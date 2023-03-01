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

# Node Management
## Show Child Paths

```
SHOW CHILD PATHS pathPattern
```

Return all child paths and their node types of all the paths matching pathPattern.

node types: ROOT -> DB INTERNAL -> DATABASE -> INTERNAL -> DEVICE -> TIMESERIES


Example：

* return the child paths of root.ln：show child paths root.ln

```
+------------+----------+
| child paths|node types|
+------------+----------+
|root.ln.wf01|  INTERNAL|
|root.ln.wf02|  INTERNAL|
+------------+----------+
Total line number = 2
It costs 0.002s
```

> get all paths in form of root.xx.xx.xx：show child paths root.xx.xx

## Show Child Nodes

```
SHOW CHILD NODES pathPattern
```

Return all child nodes of the pathPattern.

Example：

* return the child nodes of root：show child nodes root

```
+------------+
| child nodes|
+------------+
|          ln|
+------------+
```

* return the child nodes of root.ln：show child nodes root.ln

```
+------------+
| child nodes|
+------------+
|        wf01|
|        wf02|
+------------+
```

## Count Nodes

IoTDB is able to use `COUNT NODES <PathPattern> LEVEL=<INTEGER>` to count the number of nodes at
 the given level in current Metadata Tree considering a given pattern. IoTDB will find paths that
  match the pattern and counts distinct nodes at the specified level among the matched paths.
  This could be used to query the number of devices with specified measurements. The usage are as
   follows:

```
IoTDB > COUNT NODES root.** LEVEL=2
IoTDB > COUNT NODES root.ln.** LEVEL=2
IoTDB > COUNT NODES root.ln.wf01.** LEVEL=3
IoTDB > COUNT NODES root.**.temperature LEVEL=3
```

As for the above mentioned example and Metadata tree, you can get following results:

```
+------------+
|count(nodes)|
+------------+
|           4|
+------------+
Total line number = 1
It costs 0.003s

+------------+
|count(nodes)|
+------------+
|           2|
+------------+
Total line number = 1
It costs 0.002s

+------------+
|count(nodes)|
+------------+
|           1|
+------------+
Total line number = 1
It costs 0.002s

+------------+
|count(nodes)|
+------------+
|           2|
+------------+
Total line number = 1
It costs 0.002s
```

> Note: The path of timeseries is just a filter condition, which has no relationship with the definition of level.

## Show Devices

* SHOW DEVICES pathPattern? (WITH DATABASE)? limitClause? #showDevices

Similar to `Show Timeseries`, IoTDB also supports two ways of viewing devices:

* `SHOW DEVICES` statement presents all devices' information, which is equal to `SHOW DEVICES root.**`.
* `SHOW DEVICES <PathPattern>` statement specifies the `PathPattern` and returns the devices information matching the pathPattern and under the given level.

SQL statement is as follows:

```
IoTDB> show devices
IoTDB> show devices root.ln.**
```

You can get results below:

```
+-------------------+---------+
|            devices|isAligned|
+-------------------+---------+
|  root.ln.wf01.wt01|    false|
|  root.ln.wf02.wt02|    false|
|root.sgcc.wf03.wt01|    false|
|    root.turbine.d1|    false|
+-------------------+---------+
Total line number = 4
It costs 0.002s

+-----------------+---------+
|          devices|isAligned|
+-----------------+---------+
|root.ln.wf01.wt01|    false|
|root.ln.wf02.wt02|    false|
+-----------------+---------+
Total line number = 2
It costs 0.001s
```

`isAligned` indicates whether the timeseries under the device are aligned.

To view devices' information with database, we can use `SHOW DEVICES WITH DATABASE` statement.

* `SHOW DEVICES WITH DATABASE` statement presents all devices' information with their database.
* `SHOW DEVICES <PathPattern> WITH DATABASE` statement specifies the `PathPattern` and returns the 
devices' information under the given level with their database information.

SQL statement is as follows:

```
IoTDB> show devices with database
IoTDB> show devices root.ln.** with database
```

You can get results below:

```
+-------------------+-------------+---------+
|            devices|     database|isAligned|
+-------------------+-------------+---------+
|  root.ln.wf01.wt01|      root.ln|    false|
|  root.ln.wf02.wt02|      root.ln|    false|
|root.sgcc.wf03.wt01|    root.sgcc|    false|
|    root.turbine.d1| root.turbine|    false|
+-------------------+-------------+---------+
Total line number = 4
It costs 0.003s

+-----------------+-------------+---------+
|          devices|     database|isAligned|
+-----------------+-------------+---------+
|root.ln.wf01.wt01|      root.ln|    false|
|root.ln.wf02.wt02|      root.ln|    false|
+-----------------+-------------+---------+
Total line number = 2
It costs 0.001s
```

## Count Devices

* COUNT DEVICES \<PathPattern\>

The above statement is used to count the number of devices. At the same time, it is allowed to specify `PathPattern` to count the number of devices matching the `PathPattern`.

SQL statement is as follows:

```
IoTDB> show devices
IoTDB> count devices
IoTDB> count devices root.ln.**
```

You can get results below:

```
+-------------------+---------+
|            devices|isAligned|
+-------------------+---------+
|root.sgcc.wf03.wt03|    false|
|    root.turbine.d1|    false|
|  root.ln.wf02.wt02|    false|
|  root.ln.wf01.wt01|    false|
+-------------------+---------+
Total line number = 4
It costs 0.024s

+--------------+
|count(devices)|
+--------------+
|             4|
+--------------+
Total line number = 1
It costs 0.004s

+--------------+
|count(devices)|
+--------------+
|             2|
+--------------+
Total line number = 1
It costs 0.004s
```
