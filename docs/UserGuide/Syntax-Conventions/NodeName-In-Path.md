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

# Node Name in Path

Node name is a special identifier, it can also be wildcard `*` and `**`. When creating timeseries, node name can not be wildcard. In query statment, you can use wildcard to match one or more nodes of path.

## Wildcard

`*` represents one node. For example, `root.vehicle.*.sensor1` represents a 4-node path which is prefixed with `root.vehicle` and suffixed with `sensor1`.

`**` represents (`*`)+, which is one or more nodes of `*`. For example, `root.vehicle.device1.**` represents all paths prefixed by `root.vehicle.device1` with nodes num greater than or equal to 4, like `root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`, etc; `root.vehicle.**.sensor1` represents a path which is prefixed with `root.vehicle` and suffixed with `sensor1` and has at least 4 nodes.

As `*` can also be used in expressions of select clause to represent multiplication, below are examples to help you better understand the usage of `* `:

```sql
# create timeseries root.sg.`a*b`
create timeseries root.sg.`a*b` with datatype=FLOAT,encoding=PLAIN;

# As described in Identifier part, a*b should be quoted.
# "create timeseries root.sg.a*b with datatype=FLOAT,encoding=PLAIN" is wrong. 

# create timeseries root.sg.a
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN;

# create timeseries root.sg.b
create timeseries root.sg.b with datatype=FLOAT,encoding=PLAIN;

# query data of root.sg.`a*b`
select `a*b` from root.sg
# Header of result dataset
|Time|root.sg.a*b|

# multiplication of root.sg.a and root.sg.b
select a*b from root.sg
# Header of result dataset
|Time|root.sg.a * root.sg.b|
```

## Identifier

When node name is not wildcard, it is a identifier, which means the constraints on it is the same as described in Identifier part.

- Create timeseries statement:

```sql
# Node name contains special characters like ` and .,all nodes of this timeseries are: ["root","sg","www.`baidu.com"]
create timeseries root.sg.`www.``baidu.com`.a with datatype=FLOAT,encoding=PLAIN;

# Node name is a real number.
create timeseries root.sg.`111`.a with datatype=FLOAT,encoding=PLAIN;
```

After executing above statments, execute "show timeseries"，below is the result：

```sql
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                 timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|            root.sg.`111`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.sg.`www.``baidu.com`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

- Insert statment:

```sql
# Node name contains special characters like . and `
insert into root.sg.`www.``baidu.com`(timestamp, a) values(1, 2);

# Node name is a real number.
insert into root.sg(timestamp, `111`) values (1, 2);
```

- Query statement:

```sql
# Node name contains special characters like . and `
select a from root.sg.`www.``baidu.com`;

# Node name is a real number.
select `111` from root.sg
```

Results:

```sql
# select a from root.sg.`www.``baidu.com`
+-----------------------------+---------------------------+
|                         Time|root.sg.`www.``baidu.com`.a|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                        2.0|
+-----------------------------+---------------------------+

# select `111` from root.sg
+-----------------------------+-----------+
|                         Time|root.sg.111|
+-----------------------------+-----------+
|1970-01-01T08:00:00.001+08:00|        2.0|
+-----------------------------+-----------+
```
