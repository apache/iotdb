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



# Triggers Management

You can create and drop a trigger through an SQL statement, and you can also query all registered triggers through an SQL statement.

**We recommend that you stop insertion while creating triggers.**

## Create Trigger

Triggers can be registered on arbitrary path patterns. The time series registered with the trigger will be listened to by the trigger. When there is data change on the series, the corresponding fire method in the trigger will be called.

Registering a trigger can be done as follows:

1. Implement a Trigger class as described in the How to implement a Trigger chapter, assuming the class's full class name is `org.apache.iotdb.trigger.ClusterAlertingExample`
2. Package the project into a JAR package.
3. Register the trigger with an SQL statement. During the creation process, the `validate` and `onCreate` interfaces of the trigger will only be called once. For details, please refer to the chapter of How to implement a Trigger.

The complete SQL syntax is as follows:

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

Below is the explanation for the SQL syntax:

- triggerName: The trigger ID, which is globally unique and used to distinguish different triggers, is case-sensitive.
- triggerType: Trigger types are divided into two categories, STATELESS and STATEFUL.
- triggerEventClause: when the trigger fires, BEFORE INSERT and AFTER INSERT are supported now.
- pathPattern：The path pattern the trigger listens on, can contain wildcards * and **.
- className：The class name of the Trigger class.
- jarLocation: Optional. When this option is not specified, by default, we consider that the DBA has placed the JAR package required to create the trigger in the trigger_root_dir directory (configuration item, default is IOTDB_HOME/ext/trigger) of each DataNode node. When this option is specified, we will download and distribute the file resource corresponding to the URI to the trigger_root_dir/install directory of each DataNode.
- triggerAttributeClause: It is used to specify the parameters that need to be set when the trigger instance is created. This part is optional in the SQL syntax.

Here is an example SQL statement to help you understand:

```sql
CREATE STATELESS TRIGGER triggerTest
BEFORE INSERT
ON root.sg.**
AS 'org.apache.iotdb.trigger.ClusterAlertingExample'
USING URI '/jar/ClusterAlertingExample.jar'
WITH (
    "name" = "trigger",
    "limit" = "100"
)
```

The above SQL statement creates a trigger named triggerTest:

- The trigger is stateless.
- Fires before insertion.
- Listens on path pattern root.sg.**
- The implemented trigger class is named `org.apache.iotdb.trigger.ClusterAlertingExample`
- The JAR package URI is http://jar/ClusterAlertingExample.jar
- When creating the trigger instance, two parameters, name and limit, are passed in.

## Drop Trigger

The trigger can be dropped by specifying the trigger ID. During the process of dropping the trigger, the `onDrop` interface of the trigger will be called only once.

The SQL syntax is:

```sql
// Drop Trigger
dropTrigger
  : DROP TRIGGER triggerName=identifier
;
```

Here is an example statement:

```sql
DROP TRIGGER triggerTest1
```

The above statement will drop the trigger with ID triggerTest1.

## Show Trigger

You can query information about triggers that exist in the cluster through an SQL statement.

The SQL syntax is as follows:

```sql
SHOW TRIGGERS
```

The result set format of this statement is as follows:

| TriggerName  | Event                        | Type                 | State                                       | PathPattern | ClassName                               | NodeId                                  |
| ------------ | ---------------------------- | -------------------- | ------------------------------------------- | ----------- | --------------------------------------- | --------------------------------------- |
| triggerTest1 | BEFORE_INSERT / AFTER_INSERT | STATELESS / STATEFUL | INACTIVE / ACTIVE / DROPPING / TRANSFFERING | root.**     | org.apache.iotdb.trigger.TriggerExample | ALL(STATELESS) / DATA_NODE_ID(STATEFUL) |

## Trigger State

During the process of creating and dropping triggers in the cluster, we maintain the states of the triggers. The following is a description of these states:

| State        | Description                                                  | Is it recommended to insert data? |
| ------------ | ------------------------------------------------------------ | --------------------------------- |
| INACTIVE     | The intermediate state of executing `CREATE TRIGGER`, the cluster has just recorded the trigger information on the ConfigNode, and the trigger has not been activated on any DataNode. | NO                                |
| ACTIVE       | Status after successful execution of `CREATE TRIGGE`, the trigger is available on all DataNodes in the cluster. | YES                               |
| DROPPING     | Intermediate state of executing `DROP TRIGGER`, the cluster is in the process of dropping the trigger. | NO                                |
| TRANSFERRING | The cluster is migrating the location of this trigger instance. | NO                                |
