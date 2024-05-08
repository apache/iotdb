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



# 管理触发器

您可以通过 SQL 语句注册和卸载一个触发器实例，您也可以通过 SQL 语句查询到所有已经注册的触发器。

**我们建议您在注册触发器时停止写入。**

## 注册触发器

触发器可以注册在任意路径模式上。被注册有触发器的序列将会被触发器侦听，当序列上有数据变动时，触发器中对应的触发方法将会被调用。

注册一个触发器可以按如下流程进行：

1. 按照编写触发器章节的说明，实现一个完整的 Trigger 类，假定这个类的全类名为 `org.apache.iotdb.trigger.ClusterAlertingExample`
2. 将项目打成 JAR 包。
3. 使用 SQL 语句注册该触发器。注册过程中会仅只会调用一次触发器的 `validate` 和 `onCreate` 接口，具体请参考编写触发器章节。

完整 SQL 语法如下：

```sql
// Create Trigger
createTrigger
    : CREATE triggerType TRIGGER triggerName=identifier triggerEventClause ON pathPattern AS className=STRING_LITERAL uriClause? triggerAttributeClause?
    ;

triggerType
    : STATELESS | STATEFUL
    ;

triggerEventClause
    : (BEFORE | AFTER) INSERT
    ;

uriClause
    : USING URI uri
    ;

uri
    : STRING_LITERAL
    ;

triggerAttributeClause
    : WITH LR_BRACKET triggerAttribute (COMMA triggerAttribute)* RR_BRACKET
    ;

triggerAttribute
    : key=attributeKey operator_eq value=attributeValue
    ;
```

下面对 SQL 语法进行说明，您可以结合使用说明章节进行理解：

- triggerName：触发器 ID，该 ID 是全局唯一的，用于区分不同触发器，大小写敏感。
- triggerType：触发器类型，分为无状态（STATELESS）和有状态（STATEFUL）两类。
- triggerEventClause：触发时机，目前仅支持写入前（BEFORE INSERT）和写入后（AFTER INSERT）两种。
- pathPattern：触发器侦听的路径模式，可以包含通配符 * 和 **。
- className：触发器实现类的类名。
- uriClause：可选项，当不指定该选项时，我们默认 DBA 已经在各个 DataNode 节点的 trigger_root_dir 目录（配置项，默认为 IOTDB_HOME/ext/trigger）下放置好创建该触发器需要的 JAR 包。当指定该选项时，我们会将该 URI 对应的文件资源下载并分发到各 DataNode 的 trigger_root_dir/install 目录下。
- triggerAttributeClause：用于指定触发器实例创建时需要设置的参数，SQL 语法中该部分是可选项。

下面是一个帮助您理解的 SQL 语句示例：

```sql
CREATE STATELESS TRIGGER triggerTest
BEFORE INSERT
ON root.sg.**
AS 'org.apache.iotdb.trigger.ClusterAlertingExample'
USING URI 'http://jar/ClusterAlertingExample.jar'
WITH (
    "name" = "trigger",
    "limit" = "100"
)
```

上述 SQL 语句创建了一个名为 triggerTest 的触发器：

- 该触发器是无状态的（STATELESS）
- 在写入前触发（BEFORE INSERT）
- 该触发器侦听路径模式为 root.sg.**
- 所编写的触发器类名为 org.apache.iotdb.trigger.ClusterAlertingExample
- JAR 包的 URI 为 http://jar/ClusterAlertingExample.jar
- 创建该触发器实例时会传入 name 和 limit 两个参数。

## 卸载触发器

可以通过指定触发器 ID 的方式卸载触发器，卸载触发器的过程中会且仅会调用一次触发器的 `onDrop` 接口。

卸载触发器的 SQL 语法如下：

```sql
// Drop Trigger
dropTrigger
  : DROP TRIGGER triggerName=identifier
;
```

下面是示例语句：

```sql
DROP TRIGGER triggerTest1
```

上述语句将会卸载 ID 为 triggerTest1 的触发器。

## 查询触发器

可以通过 SQL 语句查询集群中存在的触发器的信息。SQL 语法如下：

```sql
SHOW TRIGGERS
```

该语句的结果集格式如下：

| TriggerName  | Event                        | Type                 | State                                       | PathPattern | ClassName                               | NodeId                                  |
| ------------ | ---------------------------- | -------------------- | ------------------------------------------- | ----------- | --------------------------------------- | --------------------------------------- |
| triggerTest1 | BEFORE_INSERT / AFTER_INSERT | STATELESS / STATEFUL | INACTIVE / ACTIVE / DROPPING / TRANSFFERING | root.**     | org.apache.iotdb.trigger.TriggerExample | ALL(STATELESS) / DATA_NODE_ID(STATEFUL) |


## 触发器状态说明

在集群中注册以及卸载触发器的过程中，我们维护了触发器的状态，下面是对这些状态的说明：

| 状态         | 描述                                                         | 是否建议写入进行 |
| ------------ | ------------------------------------------------------------ | ---------------- |
| INACTIVE     | 执行 `CREATE TRIGGER` 的中间状态，集群刚在 ConfigNode 上记录该触发器的信息，还未在任何 DataNode 上激活该触发器 | 否               |
| ACTIVE       | 执行 `CREATE TRIGGE` 成功后的状态，集群所有 DataNode 上的该触发器都已经可用 | 是               |
| DROPPING     | 执行 `DROP TRIGGER` 的中间状态，集群正处在卸载该触发器的过程中 | 否               |
| TRANSFERRING | 集群正在进行该触发器实例位置的迁移                           | 否               |

