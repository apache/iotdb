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



# 查询写回（SELECT ... INTO ...）



## SQL



### 语法



### ${i} 模式符号



### 支持写回的查询类型

* 原始序列查询

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  ```

* 时间序列生成函数查询（UDF查询）

  ```sql
  select s1, sin(s2) 
  into t1, t2 
  from root.sg.d1
  ```

* 数学表达式查询

  ```sql
  select s1, sin(s2), s1 + s3 
  into t1, t2, t3 
  from root.sg.d1
  ```

* Fill 查询

  ```sql
  select s1 
  into fill_s1 
  from root.sg.d1 
  where time = 10 
  fill(float [linear, 1ms, 1ms])
  ```

* Group By 查询

  ```sql
  select count(s1) 
  into group_by_s1 
  from root.sg.d1 
  group by ([1, 5), 1ms)
  ```

* Group By Fill 查询

	```sql
  select last_value(s1) 
  into group_by_fill_s1 
  from root.sg.d1 
  group by ([1, 10),1ms) 
  fill (float[PREVIOUS])
  ```



### 支持写回的查询子句

* 支持值过滤

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  where s1 > 0 and s2 < 0
  ```

* 支持时间过滤

    ```sql
    select s1, s1 
    into t1, t2 
    from root.sg.d1
    where time > 0
    ```

* LIMIT / OFFSET

  ```sql
  select s1, s1 
  into t1, t2 
  from root.sg.d1
  limit 5 offset 1000
  ```



### 其他限制

* `select`子句中的源序列和`into`子句中的目标序列数量必须相同
* `into`子句中的目标序列不必预先创建（可使用自动创建schema功能）
* 当`into`子句中的目标序列已存在时，您需要保证`select`子句中的源序列和`into`子句中的目标序列的数据类型一致
* `select`子句不支持带 `*` 查询
* `into`子句中的目标序列必须是互不相同的



## 权限

