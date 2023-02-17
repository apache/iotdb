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

# 权限管理

IoTDB 为用户提供了权限管理操作，从而为用户提供对于数据的权限管理功能，保障数据的安全。

我们将通过以下几个具体的例子为您示范基本的用户权限操作，详细的 SQL 语句及使用方式详情请参见本文 [数据模式与概念章节](../Data-Concept/Data-Model-and-Terminology.md)。同时，在 JAVA 编程环境中，您可以使用 [JDBC API](../API/Programming-JDBC.md) 单条或批量执行权限管理类语句。

## 基本概念

### 用户

用户即数据库的合法使用者。一个用户与一个唯一的用户名相对应，并且拥有密码作为身份验证的手段。一个人在使用数据库之前，必须先提供合法的（即存于数据库中的）用户名与密码，使得自己成为用户。

### 权限

数据库提供多种操作，并不是所有的用户都能执行所有操作。如果一个用户可以执行某项操作，则称该用户有执行该操作的权限。权限可分为数据管理权限（如对数据进行增删改查）以及权限管理权限（用户、角色的创建与删除，权限的赋予与撤销等）。数据管理权限往往需要一个路径来限定其生效范围，可使用[路径模式](../Data-Concept/Data-Model-and-Terminology.md)灵活管理权限。

### 角色

角色是若干权限的集合，并且有一个唯一的角色名作为标识符。用户通常和一个现实身份相对应（例如交通调度员），而一个现实身份可能对应着多个用户。这些具有相同现实身份的用户往往具有相同的一些权限。角色就是为了能对这样的权限进行统一的管理的抽象。

### 默认用户及其具有的角色

初始安装后的 IoTDB 中有一个默认用户：root，默认密码为 root。该用户为管理员用户，固定拥有所有权限，无法被赋予、撤销权限，也无法被删除。

## 权限操作示例 

根据本文中描述的 [样例数据](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt) 内容，IoTDB 的样例数据可能同时属于 ln, sgcc 等不同发电集团，不同的发电集团不希望其他发电集团获取自己的数据库数据，因此我们需要将不同的数据在集团层进行权限隔离。

### 创建用户

使用 `CREATE USER <userName> <password>` 创建用户。例如，我们可以使用具有所有权限的root用户为 ln 和 sgcc 集团创建两个用户角色，名为 ln_write_user, sgcc_write_user，密码均为 write_pwd。建议使用反引号(`)包裹用户名。SQL 语句为：

```
CREATE USER `ln_write_user` 'write_pwd'
CREATE USER `sgcc_write_user` 'write_pwd'
```
此时使用展示用户的 SQL 语句：

```
LIST USER
```
我们可以看到这两个已经被创建的用户，结果如下：

```
IoTDB> CREATE USER `ln_write_user` 'write_pwd'
Msg: The statement is executed successfully.
IoTDB> CREATE USER `sgcc_write_user` 'write_pwd'
Msg: The statement is executed successfully.
IoTDB> LIST USER
+---------------+
|           user|
+---------------+
|  ln_write_user|
|           root|
|sgcc_write_user|
+---------------+
Total line number = 3
It costs 0.157s
```

### 赋予用户权限

此时，虽然两个用户已经创建，但是他们不具有任何权限，因此他们并不能对数据库进行操作，例如我们使用 ln_write_user 用户对数据库中的数据进行写入，SQL 语句为：

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
此时，系统不允许用户进行此操作，会提示错误：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
Msg: 602: No permissions for this operation, please add privilege INSERT_TIMESERIES.
```

现在，我们用root用户分别赋予他们向对应 database 数据的写入权限.

我们使用 `GRANT USER <userName> PRIVILEGES <privileges> ON <nodeName>` 语句赋予用户权限(注：其中，创建用户权限无需指定路径)，例如：

```
GRANT USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
GRANT USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
GRANT USER `ln_write_user` PRIVILEGES CREATE_USER
```
执行状态如下所示：

```
IoTDB> GRANT USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
Msg: The statement is executed successfully.
IoTDB> GRANT USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
Msg: The statement is executed successfully.
IoTDB> GRANT USER `ln_write_user` PRIVILEGES CREATE_USER
Msg: The statement is executed successfully.
```

接着使用ln_write_user再尝试写入数据
```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: The statement is executed successfully.
```

### 撤销用户权限

授予用户权限后，我们可以使用 `REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeName>` 来撤销已授予的用户权限(注：其中，撤销创建用户权限无需指定路径)。例如，用root用户撤销ln_write_user和sgcc_write_user的权限：

```
REVOKE USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
REVOKE USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
REVOKE USER `ln_write_user` PRIVILEGES CREATE_USER
```

执行状态如下所示：

```
REVOKE USER `ln_write_user` PRIVILEGES INSERT_TIMESERIES on root.ln.**
Msg: The statement is executed successfully.
REVOKE USER `sgcc_write_user` PRIVILEGES INSERT_TIMESERIES on root.sgcc1.**, root.sgcc2.**
Msg: The statement is executed successfully.
REVOKE USER `ln_write_user` PRIVILEGES CREATE_USER
Msg: The statement is executed successfully.
```

撤销权限后，ln_write_user就没有向root.ln.**写入数据的权限了。
```
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: 602: No permissions for this operation, please add privilege INSERT_TIMESERIES.
```

### SQL 语句

与权限相关的语句包括：

* 创建用户

```
CREATE USER <userName> <password>;  
Eg: IoTDB > CREATE USER `thulab` 'passwd';
```

* 删除用户

```
DROP USER <userName>;  
Eg: IoTDB > DROP USER `xiaoming`;
```

* 创建角色

```
CREATE ROLE <roleName>;  
Eg: IoTDB > CREATE ROLE `admin`;
```

* 删除角色

```
DROP ROLE <roleName>;  
Eg: IoTDB > DROP ROLE `admin`;
```

* 赋予用户权限

```
GRANT USER <userName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES INSERT_TIMESERIES, DELETE_TIMESERIES on root.ln.**, root.sgcc.**;
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES CREATE_ROLE;
```

- 赋予用户全部的权限

```
GRANT USER <userName> PRIVILEGES ALL; 
Eg: IoTDB > GRANT USER `tempuser` PRIVILEGES ALL;
```

* 赋予角色权限

```
GRANT ROLE <roleName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES INSERT_TIMESERIES, DELETE_TIMESERIES ON root.sgcc.**, root.ln.**;
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES CREATE_ROLE;
```

- 赋予角色全部的权限

```
GRANT ROLE <roleName> PRIVILEGES ALL;  
Eg: IoTDB > GRANT ROLE `temprole` PRIVILEGES ALL;
```

* 赋予用户角色

```
GRANT <roleName> TO <userName>;  
Eg: IoTDB > GRANT `temprole` TO tempuser;
```

* 撤销用户权限

```
REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeNames>;   
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES DELETE_TIMESERIES on root.ln.**;
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES CREATE_ROLE;
```

- 移除用户所有权限

```
REVOKE USER <userName> PRIVILEGES ALL; 
Eg: IoTDB > REVOKE USER `tempuser` PRIVILEGES ALL;
```

* 撤销角色权限

```
REVOKE ROLE <roleName> PRIVILEGES <privileges> ON <nodeNames>;  
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES DELETE_TIMESERIES ON root.ln.**;
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES CREATE_ROLE;
```

- 撤销角色全部的权限

```
REVOKE ROLE <roleName> PRIVILEGES ALL;  
Eg: IoTDB > REVOKE ROLE `temprole` PRIVILEGES ALL;
```

* 撤销用户角色

```
REVOKE <roleName> FROM <userName>;
Eg: IoTDB > REVOKE `temprole` FROM tempuser;
```

* 列出所有用户

```
LIST USER
Eg: IoTDB > LIST USER
```

* 列出指定角色下所有用户

```
LIST USER OF ROLE <roleName>;
Eg: IoTDB > LIST USER OF ROLE `roleuser`;
```

* 列出所有角色

```
LIST ROLE
Eg: IoTDB > LIST ROLE
```

* 列出指定用户下所有角色

```
LIST ROLE OF USER <username> ;  
Eg: IoTDB > LIST ROLE OF USER `tempuser`;
```

* 列出用户所有权限

```
LIST PRIVILEGES USER <username>;   
Eg: IoTDB > LIST PRIVILEGES USER `tempuser`;
```

* 列出用户在具体路径上相关联的权限

```    
LIST PRIVILEGES USER <username> ON <paths>;
Eg: IoTDB> LIST PRIVILEGES USER `tempuser` ON root.ln.**, root.ln.wf01.**;
+--------+-----------------------------------+
|    role|                          privilege|
+--------+-----------------------------------+
|        |      root.ln.** : ALTER_TIMESERIES|
|temprole|root.ln.wf01.** : CREATE_TIMESERIES|
+--------+-----------------------------------+
Total line number = 2
It costs 0.005s
IoTDB> LIST PRIVILEGES USER `tempuser` ON root.ln.wf01.wt01.**;
+--------+-----------------------------------+
|    role|                          privilege|
+--------+-----------------------------------+
|        |      root.ln.** : ALTER_TIMESERIES|
|temprole|root.ln.wf01.** : CREATE_TIMESERIES|
+--------+-----------------------------------+
Total line number = 2
It costs 0.005s
```

* 列出角色所有权限

```
LIST PRIVILEGES ROLE <roleName>;
Eg: IoTDB > LIST PRIVILEGES ROLE `actor`;
```

* 列出角色在具体路径上相关联的权限

```
LIST PRIVILEGES ROLE <roleName> ON <paths>;    
Eg: IoTDB> LIST PRIVILEGES ROLE `temprole` ON root.ln.**, root.ln.wf01.wt01.**;
+-----------------------------------+
|                          privilege|
+-----------------------------------+
|root.ln.wf01.** : CREATE_TIMESERIES|
+-----------------------------------+
Total line number = 1
It costs 0.005s
IoTDB> LIST PRIVILEGES ROLE `temprole` ON root.ln.wf01.wt01.**;
+-----------------------------------+
|                          privilege|
+-----------------------------------+
|root.ln.wf01.** : CREATE_TIMESERIES|
+-----------------------------------+
Total line number = 1
It costs 0.005s
```

* 更新密码

```
ALTER USER <username> SET PASSWORD <password>;
Eg: IoTDB > ALTER USER `tempuser` SET PASSWORD 'newpwd';
```


## 其他说明

### 用户、权限与角色的关系

角色是权限的集合，而权限和角色都是用户的一种属性。即一个角色可以拥有若干权限。一个用户可以拥有若干角色与权限（称为用户自身权限）。

目前在 IoTDB 中并不存在相互冲突的权限，因此一个用户真正具有的权限是用户自身权限与其所有的角色的权限的并集。即要判定用户是否能执行某一项操作，就要看用户自身权限或用户的角色的所有权限中是否有一条允许了该操作。用户自身权限与其角色权限，他的多个角色的权限之间可能存在相同的权限，但这并不会产生影响。

需要注意的是：如果一个用户自身有某种权限（对应操作 A），而他的某个角色有相同的权限。那么如果仅从该用户撤销该权限无法达到禁止该用户执行操作 A 的目的，还需要从这个角色中也撤销对应的权限，或者从这个用户将该角色撤销。同样，如果仅从上述角色将权限撤销，也不能禁止该用户执行操作 A。  

同时，对角色的修改会立即反映到所有拥有该角色的用户上，例如对角色增加某种权限将立即使所有拥有该角色的用户都拥有对应权限，删除某种权限也将使对应用户失去该权限（除非用户本身有该权限）。 

### 系统所含权限列表

**系统所含权限列表**

| 权限名称                      | 说明                                      | 示例                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|:--------------------------|:----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CREATE\_DATABASE          | 创建 database。包含设置 database 的权限和TTL。路径相关  | Eg1: `CREATE DATABASE root.ln;`<br />Eg2:`set ttl to root.ln 3600000;`<br />Eg3:`unset ttl to root.ln;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| DELETE\_DATABASE          | 删除 database。路径相关                        | Eg: `delete database root.ln;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| CREATE\_TIMESERIES        | 创建时间序列。路径相关                             | Eg1: 创建时间序列<br />`create timeseries root.ln.wf02.status with datatype=BOOLEAN,encoding=PLAIN;`<br />Eg2: 创建对齐时间序列<br />`create aligned timeseries root.ln.device1(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY);`                                                                                                                                                                                                                                                                                                   |
| INSERT\_TIMESERIES        | 插入数据。路径相关                               | Eg1: `insert into root.ln.wf02(timestamp,status) values(1,true);`<br />Eg2: `insert into root.sg1.d1(time, s1, s2) aligned values(1, 1, 1)`                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ALTER\_TIMESERIES         | 修改时间序列标签。路径相关                           | Eg1: `alter timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4;`<br />Eg2: `ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4);`                                                                                                                                                                                                                                                                                                                                                                    |
| READ\_TIMESERIES          | 查询数据。路径相关                               | Eg1: `SHOW DATABASES;` <br />Eg2: `show child paths root.ln, show child nodes root.ln;`<br />Eg3: `show devices;`<br />Eg4: `show timeseries root.**;`<br />Eg5: `show schema templates;`<br />Eg6: `show all ttl`<br />Eg7: [数据查询](../Query-Data/Overview.md)（这一节之下的查询语句均使用该权限）<br />Eg8: CVS格式数据导出<br />`./export-csv.bat -h 127.0.0.1 -p 6667 -u tempuser -pw root -td ./`<br />Eg9: 查询性能追踪<br />`tracing select * from root.**`<br />Eg10: UDF查询<br />`select example(*) from root.sg.d1`<br />Eg11: 查询触发器<br />`show triggers`<br />Eg12: 统计查询<br />`count devices` |
| DELETE\_TIMESERIES        | 删除数据或时间序列。路径相关                          | Eg1: 删除时间序列<br />`delete timeseries root.ln.wf01.wt01.status`<br />Eg2: 删除数据<br />`delete from root.ln.wf02.wt02.status where time < 10`<br />Eg3: 使用DROP关键字<br />`drop timeseries root.ln.wf01.wt01.status`                                                                                                                                                                                                                                                                                                                                                             |
| CREATE\_USER              | 创建用户。路径无关                               | Eg: `create user thulab 'passwd';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| DELETE\_USER              | 删除用户。路径无关                               | Eg: `drop user xiaoming;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| MODIFY\_PASSWORD          | 修改所有用户的密码。路径无关。（没有该权限者仍然能够修改自己的密码。)     | Eg: `alter user tempuser SET PASSWORD 'newpwd';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| LIST\_USER                | 列出所有用户，列出具有某角色的所有用户，列出用户在指定路径下相关权限。路径无关 | Eg1: `list user;`<br />Eg2: `list user of role 'wirte_role';`<br />Eg3: `list privileges user admin;`<br />Eg4: `list privileges user 'admin' on root.sgcc.**;`                                                                                                                                                                                                                                                                                                                                                                                                          |
| GRANT\_USER\_PRIVILEGE    | 赋予用户权限。路径无关                             | Eg:  `grant user tempuser privileges DELETE_TIMESERIES on root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| REVOKE\_USER\_PRIVILEGE   | 撤销用户权限。路径无关                             | Eg:  `revoke user tempuser privileges DELETE_TIMESERIES on root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| GRANT\_USER\_ROLE         | 赋予用户角色。路径无关                             | Eg:  `grant temprole to tempuser;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| REVOKE\_USER\_ROLE        | 撤销用户角色。路径无关                             | Eg:  `revoke temprole from tempuser;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| CREATE\_ROLE              | 创建角色。路径无关                               | Eg:  `create role admin;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| DELETE\_ROLE              | 删除角色。路径无关                               | Eg: `drop role admin;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| LIST\_ROLE                | 列出所有角色，列出某用户下所有角色，列出角色在指定路径下相关权限。路径无关   | Eg1: `list role`<br />Eg2: `list role of user 'actor';`<br />Eg3: `list privileges role wirte_role;`<br />Eg4: `list privileges role wirte_role ON root.sgcc;`                                                                                                                                                                                                                                                                                                                                                                                                           |
| GRANT\_ROLE\_PRIVILEGE    | 赋予角色权限。路径无关                             | Eg: `grant role temprole privileges DELETE_TIMESERIES ON root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| REVOKE\_ROLE\_PRIVILEGE   | 撤销角色权限。路径无关                             | Eg: `revoke role temprole privileges DELETE_TIMESERIES ON root.ln.**;`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| CREATE_FUNCTION           | 注册 UDF。路径无关                             | Eg: `create function example AS 'org.apache.iotdb.udf.UDTFExample';`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| DROP_FUNCTION             | 卸载 UDF。路径无关                             | Eg: `drop function example`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| CREATE_TRIGGER            | 创建触发器。路径相关                              | Eg1: `CREATE TRIGGER <TRIGGER-NAME> BEFORE INSERT ON <FULL-PATH> AS <CLASSNAME>`<br />Eg2: `CREATE TRIGGER <TRIGGER-NAME> AFTER INSERT ON <FULL-PATH> AS <CLASSNAME>`                                                                                                                                                                                                                                                                                                                                                                                                    |
| DROP_TRIGGER              | 卸载触发器。路径相关                              | Eg: `drop trigger 'alert-listener-sg1d1s1'`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| CREATE_CONTINUOUS_QUERY   | 创建连续查询。路径无关                             | Eg: `select s1, s1 into t1, t2 from root.sg.d1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| DROP_CONTINUOUS_QUERY     | 卸载连续查询。路径无关                             | Eg1: `DROP CONTINUOUS QUERY cq3`<br />Eg2: `DROP CQ cq3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| SHOW_CONTINUOUS_QUERIES   | 展示所有连续查询。路径无关                           | Eg1: `SHOW CONTINUOUS QUERIES`<br />Eg2: `SHOW cqs`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| UPDATE_TEMPLATE           | 创建、删除模板。路径无关。                           | Eg1: `create schema template t1(s1 int32)`<br />Eg2: `drop schema template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| READ_TEMPLATE             | 查看所有模板、模板内容。 路径无关                       | Eg1: `show schema templates`<br/>Eg2: `show nodes in template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| APPLY_TEMPLATE            | 挂载、卸载、激活、解除模板。路径有关。                     | Eg1: `set schema template t1 to root.sg.d`<br/>Eg2: `unset schema template t1 from root.sg.d`<br/>Eg3: `create timeseries of schema template on root.sg.d`<br/>Eg4: `delete timeseries of schema template on root.sg.d`                                                                                                                                                                                                                                                                                                                                                  |
| READ_TEMPLATE_APPLICATION | 查看模板的挂载路径和激活路径。路径无关                     | Eg1: `show paths set schema template t1`<br/>Eg2: `show paths using schema template t1`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

注意： 路径无关的权限只能在路径root.**下赋予或撤销；

注意: 下述sql语句需要赋予多个权限才可以使用：

- 导入数据，需要赋予`READ_TIMESERIES`，`INSERT_TIMESERIES`两种权限。

```
Eg: IoTDB > ./import-csv.bat -h 127.0.0.1 -p 6667 -u renyuhua -pw root -f dump0.csv
```

- 查询写回(SELECT_INTO)
  - 需要所有 `select` 子句中源序列的 `READ_TIMESERIES` 权限
  - 需要所有 `into` 子句中目标序列 `INSERT_TIMESERIES` 权限

```
Eg: IoTDB > select s1, s1 into t1, t2 from root.sg.d1 limit 5 offset 1000
```

### 用户名限制

IoTDB 规定用户名的字符长度不小于 4，其中用户名不能包含空格。

### 密码限制

IoTDB 规定密码的字符长度不小于 4，其中密码不能包含空格，密码默认采用 MD5 进行加密。

### 角色名限制

IoTDB 规定角色名的字符长度不小于 4，其中角色名不能包含空格。

### 权限管理中的路径模式

一个路径模式的结果集包含了它的子模式的结果集的所有元素。例如，`root.sg.d.*`是`root.sg.*.*`的子模式，而`root.sg.**`不是`root.sg.*.*`的子模式。当用户被授予对某个路径模式的权限时，在他的DDL或DML中使用的模式必须是该路径模式的子模式，这保证了用户访问时间序列时不会超出他的权限范围。

### 权限缓存

在分布式相关的权限操作中，在进行除了创建用户和角色之外的其他权限更改操作时，都会先清除与该用户（角色）相关的所有的`dataNode`的缓存信息，如果任何一台`dataNode`缓存信息清楚失败，这个权限更改的任务就会失败。

### 非root用户限制进行的操作

目前以下IoTDB支持的sql语句只有`root`用户可以进行操作，且没有对应的权限可以赋予新用户。

###### TsFile管理

- 加载TsFile

```
Eg: IoTDB > load '/Users/Desktop/data/1575028885956-101-0.tsfile'
```

- 删除TsFile文件

```
Eg: IoTDB > remove '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile'
```

- 卸载TsFile文件到指定目录

```
Eg: IoTDB > unload '/Users/Desktop/data/data/root.vehicle/0/0/1575028885956-101-0.tsfile' '/data/data/tmp'
```

###### 删除时间分区（实验性功能）

- 删除时间分区（实验性功能）

```
Eg: IoTDB > DELETE PARTITION root.ln 0,1,2
```

###### 连续查询

- 连续查询(CQ)

```
Eg: IoTDB > CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.* GROUP BY time(10s) END
```

###### 运维命令

- FLUSH

```
Eg: IoTDB > flush
```

- MERGE

```
Eg: IoTDB > MERGE
Eg: IoTDB > FULL MERGE
```

- CLEAR CACHE

```sql
Eg: IoTDB > CLEAR CACHE
```

- SET STSTEM TO READONLY / WRITABLE

```
Eg: IoTDB > SET STSTEM TO READONLY / WRITABLE
```

- SCHEMA SNAPSHOT

```sql
Eg: IoTDB > CREATE SNAPSHOT FOR SCHEMA
```

- 查询终止

```
Eg: IoTDB > KILL QUERY 1
```

###### 水印工具

- 为新用户施加水印

```
Eg: IoTDB > grant watermark_embedding to Alice
```

- 撤销水印

```
Eg: IoTDB > revoke watermark_embedding from Alice
```
