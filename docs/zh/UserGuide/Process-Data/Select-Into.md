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



## 查询写回（SELECT ... INTO ...）

`SELECT ... INTO ...` 语句允许您将查询结果集写回到指定序列上。



### SQL

#### 语法

**下面是 `select` 语句的语法定义：**

```sql
selectClause 
intoClause? 
fromClause 
whereClause? 
specialClause?
```

如果去除 `intoClause` 子句，那么 `select` 语句即是单纯的查询语句。

`intoClause` 子句是写回功能的标记语句。



**下面是 `intoClause` 子句的定义：**

```sql
intoClause
  : INTO ALIGNED? intoPath (COMMA intoPath)*
  ;

intoPath
  : fullPath
  | nodeNameWithoutStar (DOT nodeNameWithoutStar)*
  ;
```

`intoPath`（目标序列）支持两种方式指定：

* 以 `root` 开头的完整序列名指定

  * 例子：

    ```sql
    select s1, s1 
    into root.sg.d1.t1, root.sg.d1.t2 
    from root.sg.d1
    ```

* 不以 `root` 开头的部分序列名指定，此时目标序列由 `from` 子句中的序列前缀和`intoPath`拼接而成

  * 例子：

    ```sql
    select s1, s1 
    into t1, t2 
    from root.sg.d1
    ```
    
    这等价于
    
    ```sql
    select s1, s1 
    into root.sg.d1.t1, root.sg.d1.t2 
    from root.sg.d1
    ```



**在`intoPath` 中，您还可以使用 `${i}`风格的路径匹配符来表示`from`子句中的部分路径。**

比如，对于路径`root.sg1.d1.v1`而言，`${1}`表示`sg1`，`${2}`表示`d1`，`${3}`表示`v1`。


  * 例子：

    ```sql
    select s1, s1, s1
    into ${1}_t1, ${2}, root.${2}.${1}.t2
    from root.sg.d1
    ```
    
    这等价于
    
    ```sql
    select s1, s1, s1
    into root.sg.d1.sg_t1, root.sg.d1.d1, root.d1.sg.t2
    from root.sg.d1
    ```



**您可以通过关键词  `ALIGNED` 指定 `intoPath`（目标序列）是否为一个对齐时间序列。**

当目标序列存在时，您需要保证源序列和目标时间序列的类型匹配。

当目标序列不存在时，系统将自动创建一个新的目标对齐时间序列。


  * 例子：

    ```sql
    select s1, s2, s3
    into aligned root.sg.d2.t1, root.sg.d2.t2, root.sg.d2.t3
    from root.sg.d1
    ```




#### 支持写回的查询类型

**注意，除了下述类型的查询，其余类型的查询（如`LAST`查询和原始聚合查询）都不被支持。**

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

* 嵌套查询

  ```sql
  select -s1, sin(cos(tan(s1 + s2 * s3))) + cos(s3), top_k(s1 + s3, 'k'='1') 
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



#### 支持写回的查询子句

**注意，除了下述子句，其余查询子句（如 `DESC` / `SOFFSET` 等）都不被支持。**

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



#### 其他限制

* `select`子句中的源序列和`into`子句中的目标序列数量必须相同
* `select`子句不支持带 `*`/`**` 查询
* `into`子句中的目标序列不必预先创建（可使用自动创建schema功能），但是当`into`子句中的目标序列已存在时，您需要保证`select`子句中的源序列和`into`子句中的目标序列的数据类型一致
* `into`子句中的目标序列必须是互不相同的
* `from`子句只允许有一列序列前缀
* `from`子句不支持带 `*`/`**`
* 由于时间序列生成函数查询（UDF查询）/ 数学表达式查询 / 嵌套查询 尚不支持对齐时间序列（Aligned Timeseries），所以如果您在`select`子句中使用了上述查询，并且对应操作数包含对齐时间序列，会提示错误



### 权限

用户必须有下列权限才能正常执行查询写回语句：

* 所有 `select` 子句中源序列的 `READ_TIMESERIES` 权限
* 所有 `into` 子句中目标序列 `INSERT_TIMESERIES` 权限

更多用户权限相关的内容，请参考[权限管理语句](../Administration-Management/Administration.md)。



### 配置参数

* `select_into_insert_tablet_plan_row_limit`：执行 select-into 语句时，一个 insert-tablet-plan 中可以处理的最大行数。 默认为 10000。
