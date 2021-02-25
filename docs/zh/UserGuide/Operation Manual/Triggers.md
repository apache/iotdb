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



# 触发器



## SQL管理语句

您可以通过SQL语句注册、卸载、启动或停止一个触发器实例，您也可以通过SQL语句查询到所有已经注册的触发器。

触发器有两种运行状态：`STARTED`和`STOPPED`，您可以通过执行`START TRIGGER`或者`STOP TRIGGER`启动或者停止一个触发器。注意，通过`CREATE TRIGGER`语句注册的触发器默认是`STARTED`的。



### 注册触发器

注册触发器的SQL语法如下：

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

同时，您还可以通过`WITH`子句传入任意数量的自定义属性值：

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
WITH (
  <KEY-1>=<VALUE-1>, 
  <KEY-2>=<VALUE-2>, 
  ...
)
```

注意，`CLASSNAME`以及属性值中的`KEY`和`VALUE`都需要被单引号或者双引号引用起来。



### 卸载触发器

卸载触发器的SQL语法如下：

```sql
DROP TRIGGER <TRIGGER-NAME>
```



### 启动触发器

启动触发器的SQL语法如下：

```sql
START TRIGGER <TRIGGER-NAME>
```



### 停止触发器

停止触发器的SQL语法如下：

```sql
STOP TRIGGER <TRIGGER-NAME>
```



### 查询所有注册的触发器

``` sql
SHOW TRIGGERS
```

