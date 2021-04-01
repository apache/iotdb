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



# Triggers



## Triggers Management

You can register, deregister, start or stop a trigger instance through SQL statements, and you can also query all registered triggers through SQL statements.

Triggers have two states: `STARTED` and `STOPPED`. You can start or stop a trigger by executing `START TRIGGER` or `STOP TRIGGER`. Note that the triggers registered by the `CREATE TRIGGER` statement are `STARTED` by default.



### Create Triggers

The following shows the SQL syntax of how to register a trigger.

```sql
CREATE TRIGGER <TRIGGER-NAME>
(BEFORE | AFTER) INSERT
ON <FULL-PATH>
AS <CLASSNAME>
```

You can also set any number of key-value pair attributes for the trigger through the `WITH` clause:

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

Note that `CLASSNAME`, `KEY` and `VALUE` in key-value pair attributes need to be quoted in single or double quotes.



### Drop Triggers

The following shows the SQL syntax of how to deregister a trigger.

```sql
DROP TRIGGER <TRIGGER-NAME>
```



### Start Triggers

The following shows the SQL syntax of how to start a trigger.

```sql
START TRIGGER <TRIGGER-NAME>
```



### Stop Triggers

The following shows the SQL syntax of how to stop a trigger.

```sql
STOP TRIGGER <TRIGGER-NAME>
```



### Show All Registered Triggers

``` sql
SHOW TRIGGERS
```

