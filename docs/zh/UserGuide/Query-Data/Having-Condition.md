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

# 聚合结果过滤

如果想对聚合查询的结果进行过滤，可以在 `GROUP BY` 子句之后使用 `HAVING` 子句。

**注意：** 

1. `HAVING`子句中的过滤条件必须由聚合值构成，原始序列不能单独出现。

    下列使用方式是不正确的：
    ```sql
    select count(s1) from root.** group by ([1,3),1ms) having sum(s1) > s1
    select count(s1) from root.** group by ([1,3),1ms) having s1 > 1
    ```
   
2. 对`GROUP BY LEVEL`结果进行过滤时，`SELECT`和`HAVING`中出现的PATH只能有一级。 

   下列使用方式是不正确的：
    ```sql
    select count(s1) from root.** group by ([1,3),1ms), level=1 having sum(d1.s1) > 1
    select count(d1.s1) from root.** group by ([1,3),1ms), level=1 having sum(s1) > 1
    ```

**SQL 示例：**

- **示例 1：**

    对于以下聚合结果进行过滤：

    ```
    +-----------------------------+---------------------+---------------------+
    |                         Time|count(root.test.*.s1)|count(root.test.*.s2)|
    +-----------------------------+---------------------+---------------------+
    |1970-01-01T08:00:00.001+08:00|                    4|                    4|
    |1970-01-01T08:00:00.003+08:00|                    1|                    0|
    |1970-01-01T08:00:00.005+08:00|                    2|                    4|
    |1970-01-01T08:00:00.007+08:00|                    3|                    2|
    |1970-01-01T08:00:00.009+08:00|                    4|                    4|
    +-----------------------------+---------------------+---------------------+
    ```

    ```sql
     select count(s1) from root.** group by ([1,11),2ms), level=1 having count(s2) > 2;
    ```

    执行结果如下：

    ```
    +-----------------------------+---------------------+
    |                         Time|count(root.test.*.s1)|
    +-----------------------------+---------------------+
    |1970-01-01T08:00:00.001+08:00|                    4|
    |1970-01-01T08:00:00.005+08:00|                    2|
    |1970-01-01T08:00:00.009+08:00|                    4|
    +-----------------------------+---------------------+
    ```

- **示例 2：**

  对于以下聚合结果进行过滤：
    ```
    +-----------------------------+-------------+---------+---------+
    |                         Time|       Device|count(s1)|count(s2)|
    +-----------------------------+-------------+---------+---------+
    |1970-01-01T08:00:00.001+08:00|root.test.sg1|        1|        2|
    |1970-01-01T08:00:00.003+08:00|root.test.sg1|        1|        0|
    |1970-01-01T08:00:00.005+08:00|root.test.sg1|        1|        2|
    |1970-01-01T08:00:00.007+08:00|root.test.sg1|        2|        1|
    |1970-01-01T08:00:00.009+08:00|root.test.sg1|        2|        2|
    |1970-01-01T08:00:00.001+08:00|root.test.sg2|        2|        2|
    |1970-01-01T08:00:00.003+08:00|root.test.sg2|        0|        0|
    |1970-01-01T08:00:00.005+08:00|root.test.sg2|        1|        2|
    |1970-01-01T08:00:00.007+08:00|root.test.sg2|        1|        1|
    |1970-01-01T08:00:00.009+08:00|root.test.sg2|        2|        2|
    +-----------------------------+-------------+---------+---------+
    ```

    ```sql
     select count(s1), count(s2) from root.** group by ([1,11),2ms) having count(s2) > 1 align by device;
    ```

    执行结果如下：

    ```
    +-----------------------------+-------------+---------+---------+
    |                         Time|       Device|count(s1)|count(s2)|
    +-----------------------------+-------------+---------+---------+
    |1970-01-01T08:00:00.001+08:00|root.test.sg1|        1|        2|
    |1970-01-01T08:00:00.005+08:00|root.test.sg1|        1|        2|
    |1970-01-01T08:00:00.009+08:00|root.test.sg1|        2|        2|
    |1970-01-01T08:00:00.001+08:00|root.test.sg2|        2|        2|
    |1970-01-01T08:00:00.005+08:00|root.test.sg2|        1|        2|
    |1970-01-01T08:00:00.009+08:00|root.test.sg2|        2|        2|
    +-----------------------------+-------------+---------+---------+
    ```