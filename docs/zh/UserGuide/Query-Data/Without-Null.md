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

## 结果集空值过滤

在实际应用中，用户可能希望对查询的结果集中某些存在空值的行进行过滤。在 IoTDB 中，可以使用  `WITHOUT NULL`  子句对结果集中的空值进行过滤，有两种过滤策略：`WITHOUT NULL ANY`和`WITHOUT NULL ALL`。不仅如此， `WITHOUT NULL`  子句支持指定相应列进行过滤。

> WITHOUT NULL ANY: 指定的列集中，存在一列为NULL,则满足条件进行过滤
> 
> WITHOUT NULL ALL: 指定的列集中，所有列都为NULL,才满足条件进行过滤

### 不指定列

> 默认就是对结果集中的所有列生效，即指定的列集为结果集中的所有列

1. 在下列查询中，如果结果集中某一行任意一列为 null，则过滤掉该行；即获得的结果集不包含任何空值。

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ANY
```

2. 在下列查询中，如果结果集的某一行所有列都为 null，则过滤掉该行；即获得的结果集不包含所有值都为 null 的行。

```sql
select * from root.ln.** where group by ([1,10), 2ms) WITHOUT NULL ALL
```

### 指定列

> 只对指定的列集生效

使用 `WITHOUT NULL`子句对结果集中指定列名的空值进行过滤，下面是一些例子及其说明:

> 注意，如果指定的列在当前元数据里不存在则会直接过滤掉，这与目前查询列的输出结果是一致的;
> 如果指定的列存在，但与结果集中输出的列名不匹配，则会报错: `The without null columns don't match the columns queried.If has alias, please use the alias.`

比如下面的例子

1. 比如`without null`指定的列集中`root.test.sg1.s1`列在元数据中存在，`root.test.sg1.usag`列在元数据不存在，则下面查询的without null子句的作用相当于without null all(s1)

```sql
select * from root.test.sg1 without null all (s1, usag)
```

2. 比如`without null`指定的列集中`root.test.sg1.s2`列在元数据中存在，但查询的结果集中输出列不包括，所以会报错:`The without null columns don't match the columns queried.If has alias, please use the alias.`

```sql
select s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.test.sg1 without null all (s1+s2, s2)
```

#### 原始数据查询

1. 如果查询的结果集中, root.ln.sg1.s1这一列如果为null,则过滤掉该行

```sql
select * from root.ln.sg1 WITHOUT NULL ANY(s1)
```

2. 如果查询的结果集中, root.ln.sg1.s1和root.ln.sg1.s2中只要存在至少一列为null,则过滤掉该行

```sql
select * from root.ln.sg1 WITHOUT NULL ANY(s1, s2)
```

3. 如果查询的结果集中, 只要root.ln.sg1.s1和root.ln.sg1.s2这两列都为null,则过滤掉该行

```sql
select * from root.ln.sg1 WITHOUT NULL ALL(s1, s2)
```

#### 带表达式查询

指定的列可以为表达式

1. 计算s2+s4和s2这两列是否都为null,如果是则过滤

```sql
select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (s2+s4, s2)
``` 

2. 计算s2+s4和s2这两列是否至少有一列为null,如果是则过滤

```sql
select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null any (s2+s4, s2)
``` 

#### 别名

指定的列可以为别名

1. 计算t2和t1这两列是否都为null,如果是则过滤

```sql
select s2 as t1, - s2, s4, + s4, s2 + s4 as t2, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (t2, t1)
```

```sql
select s1, sin(s2) + cos(s2) as t1, cos(sin(s2 + s4) + s2) as t2 from root.test.sg1 without null all (t1, t2)
```

2. 当你指定了别名，如果在without null指定的列集中再使用原始列名，则会报错：`The without null columns don't match the columns queried.If has alias, please use the alias.` 比如下面同时使用了tan(s1)和t

```sql
select s1 as d, sin(s1), cos(s1), tan(s1) as t, s2 from root.test.sg1 without null all(d,  tan(s1), t) limit 5
```

#### 带函数查询

```sql
select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 without null all (sin(s2) + cos(s2), cos(sin(s2 + s4) + s2))
```

#### 按设备对齐查询

```sql
select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) align by device
```

结果示例如下：

```
IoTDB> select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) align by device
+-----------------------------+-----------+--------------+--------------+--------------+
|                         Time|     Device|last_value(s1)|last_value(s2)|last_value(s3)|
+-----------------------------+-----------+--------------+--------------+--------------+
|1970-01-01T08:00:00.001+08:00|root.sg1.d1|           1.0|           2.0|          null|
|1970-01-01T08:00:00.003+08:00|root.sg1.d1|           3.0|           4.0|          null|
|1970-01-01T08:00:00.001+08:00|root.sg1.d2|           1.0|           1.0|           1.0|
+-----------------------------+-----------+--------------+--------------+--------------+
Total line number = 3
It costs 0.007s
```

指定的列名是与输出结果的列名对应，目前`without null`子句不支持指定某设备的某列，会报错:`The without null columns don't match the columns queried.If has alias, please use the alias.` 比如下面这个查询例子,指定last_value(root.sg1.d1.s3)为null的行进行过滤，目前是不支持的。

```sql
select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null all(last_value(`root.sg1.d1.s3`)) align by device
```

#### 聚合查询

```sql
select avg(s4), sum(s2) from root.test.sg1 group by ([1,10), 2ms) without null all(sum(s2))
```

```sql
select avg(s4), sum(s2), count(s3) from root.test.sg1 group by ([1,10), 2ms) without null all(avg(s4), sum(s2))
```

#### 指定全路径列名

假设下面的查询输出的结果列为"root.test.sg1.s2", "root.test.sg1.s3", "root.test.sg2.s2", "root.test.sg2.s3"四个，可以使用全路径名指定相应列进行过滤，比如下面的例子：

1. 指定`root.test.sg1.s2`, `root.test.sg2.s3`两列都为null则过滤

```sql
select s2, s3 from root.test.** without null all(`root.test.sg1.s2`, `root.test.sg2.s3`)
```

2. 指定`root.test.sg1.s2`, `root.test.sg1.s3`, `root.test.sg2.s3`三列都为null则过滤

```sql
select s2, s3 from root.test.** without null all(`root.test.sg1.s2`, s3)
```

#### 对齐序列查询

1. 可以指定`without null` 子句后的列名为对齐序列列名

```sql
CREATE ALIGNED TIMESERIES root.test.sg3(s5 INT32, s6 BOOLEAN, s7 DOUBLE, s8 INT32)
select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test without null all (sg3.s5, sg3.s6, sg2.s3)
```