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

# 常序列生成函数

常序列生成函数用于生成所有数据点的值都相同的时间序列。

常序列生成函数接受一个或者多个时间序列输入，其输出的数据点的时间戳集合是这些输入序列时间戳集合的并集。

目前 IoTDB 支持如下常序列生成函数：

| 函数名 | 必要的属性参数                                               | 输出序列类型               | 功能描述                                                     |
| ------ | ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ |
| CONST  | `value`: 输出的数据点的值 <br />`type`: 输出的数据点的类型，只能是 INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | 由输入属性参数 `type` 决定 | 根据输入属性 `value` 和 `type` 输出用户指定的常序列。        |
| PI     | 无                                                           | DOUBLE                     | 常序列的值：`π` 的 `double` 值，圆的周长与其直径的比值，即圆周率，等于 *Java标准库* 中的`Math.PI`。 |
| E      | 无                                                           | DOUBLE                     | 常序列的值：`e` 的 `double` 值，自然对数的底，它等于 *Java 标准库*  中的 `Math.E`。 |

例如：

```   sql
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
```

结果：

```
select s1, s2, const(s1, 'value'='1024', 'type'='INT64'), pi(s2), e(s1, s2) from root.sg1.d1; 
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|const(root.sg1.d1.s1, "value"="1024", "type"="INT64")|pi(root.sg1.d1.s2)|e(root.sg1.d1.s1, root.sg1.d1.s2)|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
|1970-01-01T08:00:00.000+08:00|           0.0|           0.0|                                                 1024| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.001+08:00|           1.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.002+08:00|           2.0|          null|                                                 1024|              null|                2.718281828459045|
|1970-01-01T08:00:00.003+08:00|          null|           3.0|                                                 null| 3.141592653589793|                2.718281828459045|
|1970-01-01T08:00:00.004+08:00|          null|           4.0|                                                 null| 3.141592653589793|                2.718281828459045|
+-----------------------------+--------------+--------------+-----------------------------------------------------+------------------+---------------------------------+
Total line number = 5
It costs 0.005s
```
