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

## 条件表达式

### CASE

#### 介绍

CASE表达式的功能类似于其它语言中的if/else语句。

语法示例如下：

- 格式1：
    ```sql
    CASE 
        WHEN condition1 THEN expression1
        [WHEN condition2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
    逐个计算condition，condition首次为真时返回对应的expression。
- 格式2：
    ```sql
    CASE caseValue
        WHEN whenValue1 THEN expression1
        [WHEN whenValue2 THEN expression2] ...
        [ELSE expression_end]
    END
    ```
    计算出caseValue，然后逐个计算whenValue，首次满足caseValue=whenValue时返回对应的expression。
    
    格式2会被iotdb转换成等效的格式1，例如以上sql语句会转换成
    ```sql
    CASE 
        WHEN caseValue=whenValue1 THEN expression1
        [WHEN caseValue=whenValue1 THEN expression1] ...
        [ELSE expression_end]
    END
    ```

如果condition均不为真，或均不满足caseVaule=whenValue，则返回expression_end；不存在ELSE子句则返回null。

#### 使用示例

##### 测试数据

```
IoTDB> select ts from root.test;
+-----------------------------+------------+
|                         Time|root.test.ts|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|           1|
|1970-01-01T08:00:00.002+08:00|           2|
|1970-01-01T08:00:00.003+08:00|           3|
|1970-01-01T08:00:00.004+08:00|           4|
+-----------------------------+------------+
```

##### 示例1

SQL语句：

```sql
select ts, case when ts<=1 then "ts <= 1" else "1 < ts" end from root.test
```

输出：

```
+-----------------------------+------------+--------------------------------------------------------+
|                         Time|root.test.ts|CASE WHEN root.test.ts <= 1 THEN "ts <= 1" ELSE "1 < ts"|
+-----------------------------+------------+--------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                                 ts <= 1|
|1970-01-01T08:00:00.002+08:00|         2.0|                                                  1 < ts|
|1970-01-01T08:00:00.003+08:00|         3.0|                                                  1 < ts|
|1970-01-01T08:00:00.004+08:00|         4.0|                                                  1 < ts|
+-----------------------------+------------+--------------------------------------------------------+
```

##### 示例2

Case表达式可嵌套在其他表达式中。

SQL语句：

```sql
select ts, case ts when 2 then 20 else 30 end + case ts when 3 then 40 else 50 end as data from root.test
```

输出：

```
+-----------------------------+------------+----+
|                         Time|root.test.ts|data|
+-----------------------------+------------+----+
|1970-01-01T08:00:00.001+08:00|         1.0|80.0|
|1970-01-01T08:00:00.002+08:00|         2.0|70.0|
|1970-01-01T08:00:00.003+08:00|         3.0|70.0|
|1970-01-01T08:00:00.004+08:00|         4.0|80.0|
+-----------------------------+------------+----+
```

##### 示例3

条件均不匹配，且没有ELSE子句时，输出null。

SQL语句：

```sql
select ts, case ts when 1 then true end from root.test
```

输出：

```
+-----------------------------+------------+-------------------------------------+
|                         Time|root.test.ts|CASE WHEN root.test.ts = 1 THEN true |
+-----------------------------+------------+-------------------------------------+
|1970-01-01T08:00:00.001+08:00|         1.0|                                 true|
|1970-01-01T08:00:00.002+08:00|         2.0|                                 null|
|1970-01-01T08:00:00.003+08:00|         3.0|                                 null|
|1970-01-01T08:00:00.004+08:00|         4.0|                                 null|
+-----------------------------+------------+-------------------------------------+
```


