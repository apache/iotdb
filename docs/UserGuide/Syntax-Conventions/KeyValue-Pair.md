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

# Key-Value Pair

**The key/value of an attribute can be constant(including string) and identifier.** 

Below are usage scenarios of key-value pair:

- Attributes fields of trigger. See the attributes after `With` clause in the example below:

```SQL
# 以字符串形式表示键值对
CREATE TRIGGER `alert-listener-sg1d1s1`
AFTER INSERT
ON root.sg1.d1.s1
AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
WITH (
  'lo' = '0', 
  'hi' = '100.0'
)

# 以标识符和常量形式表示键值对
CREATE TRIGGER `alert-listener-sg1d1s1`
AFTER INSERT
ON root.sg1.d1.s1
AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
WITH (
  lo = 0, 
  hi = 100.0
)
```

- Key-value pair to represent tag/attributes in timeseries:

```sql
# create timeseries using string as key/value
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, 'max_point_number' = '5'
TAGS('tag1' = 'v1', 'tag2'= 'v2') ATTRIBUTES('attr1' = 'v1', 'attr2' = 'v2')

# create timeseries using constant as key/value
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, max_point_number = 5
TAGS(tag1 = v1, tag2 = v2) ATTRIBUTES(attr1 = v1, attr2 = v2)
```

```sql
# alter tags and attributes of timeseries
ALTER timeseries root.turbine.d1.s1 SET 'newTag1' = 'newV1', 'attr1' = 'newV1'

ALTER timeseries root.turbine.d1.s1 SET newTag1 = newV1, attr1 = newV1
```

```sql
# rename tag
ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'newTag1'

ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```

```sql
# upsert alias, tags, attributes
ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS='newAlias' TAGS('tag2' = 'newV2', 'tag3' = 'v3') ATTRIBUTES('attr3' ='v3', 'attr4'='v4')

ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS = newAlias TAGS(tag2 = newV2, tag3 = v3) ATTRIBUTES(attr3 = v3, attr4 = v4)
```

```sql
# add new tags
ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3' = 'v3', 'tag4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3 = v3, tag4 = v4
```

```sql
# add new attributes
ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3' = 'v3', 'attr4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3 = v3, attr4 = v4
```

```sql
# query for timeseries
SHOW timeseries root.ln.** WHRER 'unit' = 'c'

SHOW timeseries root.ln.** WHRER unit = c
```

- Attributes fields of Pipe and PipeSink.

```SQL
# PipeSink example 
CREATE PIPESINK my_iotdb AS IoTDB ('ip' = '输入你的IP')

# Pipe example 
CREATE PIPE my_pipe TO my_iotdb FROM 
(select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
```
