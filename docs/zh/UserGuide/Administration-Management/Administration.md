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

IoTDB为用户提供了权限管理操作，从而为用户提供对于数据的权限管理功能，保障数据的安全。

我们将通过以下几个具体的例子为您示范基本的用户权限操作，详细的SQL语句及使用方式详情请参见本文[数据模式与概念章节](../Data-Concept/Data-Model-and-Terminology.md)。同时，在JAVA编程环境中，您可以使用[JDBC API](../API/Programming-JDBC.md)单条或批量执行权限管理类语句。

## 基本概念

### 用户

用户即数据库的合法使用者。一个用户与一个唯一的用户名相对应，并且拥有密码作为身份验证的手段。一个人在使用数据库之前，必须先提供合法的（即存于数据库中的）用户名与密码，使得自己成为用户。

### 权限

数据库提供多种操作，并不是所有的用户都能执行所有操作。如果一个用户可以执行某项操作，则称该用户有执行该操作的权限。权限可分为数据管理权限（如对数据进行增删改查）以及权限管理权限（用户、角色的创建与删除，权限的赋予与撤销等）。数据管理权限往往需要一个路径来限定其生效范围，它的生效范围是以该路径对应的节点为根的一棵子树（具体请参考IoTDB的数据组织）。

### 角色

角色是若干权限的集合，并且有一个唯一的角色名作为标识符。用户通常和一个现实身份相对应（例如交通调度员），而一个现实身份可能对应着多个用户。这些具有相同现实身份的用户往往具有相同的一些权限。角色就是为了能对这样的权限进行统一的管理的抽象。

### 默认用户及其具有的角色

初始安装后的IoTDB中有一个默认用户：root，默认密码为root。该用户为管理员用户，固定拥有所有权限，无法被赋予、撤销权限，也无法被删除。

## 权限操作示例 

根据本文中描述的[样例数据](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt)内容，IoTDB的样例数据可能同时属于ln, sgcc等不同发电集团，不同的发电集团不希望其他发电集团获取自己的数据库数据，因此我们需要将不同的数据在集团层进行权限隔离。

### 创建用户

我们可以为ln和sgcc集团创建两个用户角色，名为ln_write_user, sgcc_write_user，密码均为write_pwd。SQL语句为：

```
CREATE USER ln_write_user 'write_pwd'
CREATE USER sgcc_write_user 'write_pwd'
```
此时使用展示用户的SQL语句：

```
LIST USER
```
我们可以看到这两个已经被创建的用户，结果如下：

```
IoTDB> CREATE USER ln_write_user 'write_pwd'
Msg: The statement is executed successfully.
IoTDB> CREATE USER sgcc_write_user 'write_pwd'
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

此时，虽然两个用户已经创建，但是他们不具有任何权限，因此他们并不能对数据库进行操作，例如我们使用ln_write_user用户对数据库中的数据进行写入，SQL语句为：

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
此时，系统不允许用户进行此操作，会提示错误：

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
Msg: 602: No permissions for this operation INSERT
```


现在，我们分别赋予他们向对应存储组数据的写入权限，并再次尝试向对应的存储组进行数据写入。SQL语句为：
```
GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
```
执行状态如下所示：

```
IoTDB> GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
Msg: The statement is executed successfully.
IoTDB> GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
Msg: The statement is executed successfully.
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: The statement is executed successfully.
```

## 其他说明

### 用户、权限与角色的关系

角色是权限的集合，而权限和角色都是用户的一种属性。即一个角色可以拥有若干权限。一个用户可以拥有若干角色与权限（称为用户自身权限）。

目前在IoTDB中并不存在相互冲突的权限，因此一个用户真正具有的权限是用户自身权限与其所有的角色的权限的并集。即要判定用户是否能执行某一项操作，就要看用户自身权限或用户的角色的所有权限中是否有一条允许了该操作。用户自身权限与其角色权限，他的多个角色的权限之间可能存在相同的权限，但这并不会产生影响。

需要注意的是：如果一个用户自身有某种权限（对应操作A），而他的某个角色有相同的权限。那么如果仅从该用户撤销该权限无法达到禁止该用户执行操作A的目的，还需要从这个角色中也撤销对应的权限，或者从这个用户将该角色撤销。同样，如果仅从上述角色将权限撤销，也不能禁止该用户执行操作A。  

同时，对角色的修改会立即反映到所有拥有该角色的用户上，例如对角色增加某种权限将立即使所有拥有该角色的用户都拥有对应权限，删除某种权限也将使对应用户失去该权限（除非用户本身有该权限）。 

### 系统所含权限列表

<center>**系统所含权限列表**

|权限名称|说明|
|:---|:---|
|SET\_STORAGE\_GROUP|创建存储组。包含设置存储组的权限。路径相关|
|CREATE\_TIMESERIES|创建时间序列。路径相关|
|INSERT\_TIMESERIES|插入数据。路径相关|
|READ\_TIMESERIES|查询数据。路径相关|
|DELETE\_TIMESERIES|删除数据或时间序列。路径相关|
|CREATE\_USER|创建用户。路径无关|
|DELETE\_USER|删除用户。路径无关|
|MODIFY\_PASSWORD|修改所有用户的密码。路径无关。(没有该权限者仍然能够修改自己的密码。)|
|LIST\_USER|列出所有用户，列出某用户权限，列出某用户具有的角色三种操作的权限。路径无关|
|GRANT\_USER\_PRIVILEGE|赋予用户权限。路径无关|
|REVOKE\_USER\_PRIVILEGE|撤销用户权限。路径无关|
|GRANT\_USER\_ROLE|赋予用户角色。路径无关|
|REVOKE\_USER\_ROLE|撤销用户角色。路径无关|
|CREATE\_ROLE|创建角色。路径无关|
|DELETE\_ROLE|删除角色。路径无关|
|LIST\_ROLE|列出所有角色，列出某角色拥有的权限，列出拥有某角色的所有用户三种操作的权限。路径无关|
|GRANT\_ROLE\_PRIVILEGE|赋予角色权限。路径无关|
|REVOKE\_ROLE\_PRIVILEGE|撤销角色权限。路径无关|
|CREATE_FUNCTION|注册UDF。路径无关|
|DROP_FUNCTION|卸载UDF。路径无关|
|CREATE_TRIGGER|创建触发器。路径无关|
|DROP_TRIGGER|卸载触发器。路径无关|
|START_TRIGGER|启动触发器。路径无关|
|STOP_TRIGGER|停止触发器。路径无关|
</center>

### 用户名限制

IoTDB规定用户名的字符长度不小于4，其中用户名不能包含空格。

### 密码限制

IoTDB规定密码的字符长度不小于4，其中密码不能包含空格，密码采用MD5进行加密。

### 角色名限制

IoTDB规定角色名的字符长度不小于4，其中角色名不能包含空格。
