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

# 键值对

**键值对的键和值可以被定义为标识符或者常量。**

下面将介绍键值对的使用场景。

- 触发器中表示触发器属性的键值对。参考示例语句中 WITH 后的属性键值对。

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

- 时间序列中用于表示标签和属性的键值对。

```sql
# 创建时间序列时设定标签和属性，用字符串来表示键值对。
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, 'max_point_number' = '5'
TAGS('tag1' = 'v1', 'tag2'= 'v2') ATTRIBUTES('attr1' = 'v1', 'attr2' = 'v2')

# 创建时间序列时设定标签和属性，用标识符和常量来表示键值对。
CREATE timeseries root.turbine.d1.s1(temprature) 
WITH datatype = FLOAT, encoding = RLE, compression = SNAPPY, max_point_number = 5
TAGS(tag1 = v1, tag2 = v2) ATTRIBUTES(attr1 = v1, attr2 = v2)
```

```sql
# 修改时间序列的标签和属性
ALTER timeseries root.turbine.d1.s1 SET 'newTag1' = 'newV1', 'attr1' = 'newV1'

ALTER timeseries root.turbine.d1.s1 SET newTag1 = newV1, attr1 = newV1
```

```sql
# 修改标签名
ALTER timeseries root.turbine.d1.s1 RENAME 'tag1' TO 'newTag1'

ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```

```sql
# 插入别名、标签、属性
ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS='newAlias' TAGS('tag2' = 'newV2', 'tag3' = 'v3') ATTRIBUTES('attr3' ='v3', 'attr4'='v4')

ALTER timeseries root.turbine.d1.s1 UPSERT 
ALIAS = newAlias TAGS(tag2 = newV2, tag3 = v3) ATTRIBUTES(attr3 = v3, attr4 = v4)
```

```sql
# 添加新的标签
ALTER timeseries root.turbine.d1.s1 ADD TAGS 'tag3' = 'v3', 'tag4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3 = v3, tag4 = v4
```

```sql
# 添加新的属性
ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES 'attr3' = 'v3', 'attr4' = 'v4'

ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3 = v3, attr4 = v4
```

```sql
# 查询符合条件的时间序列信息
SHOW timeseries root.ln.** WHRER 'unit' = 'c'

SHOW timeseries root.ln.** WHRER unit = c
```

- 创建 Pipe 以及 PipeSink 时表示属性的键值对。

```SQL
# 创建 PipeSink 时表示属性
CREATE PIPESINK my_iotdb AS IoTDB ('ip' = '输入你的IP')

# 创建 Pipe 时在 WITH 子句中表示属性
CREATE PIPE my_pipe TO my_iotdb FROM 
(select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
```
