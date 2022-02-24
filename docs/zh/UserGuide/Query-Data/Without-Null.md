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

# 结果集空值过滤

在 IoTDB 中，可以使用  `WITHOUT NULL`  子句对结果集中的空值进行过滤，有两种过滤策略：

1. 如果结果集中，任意一列为 null，则过滤掉该行；即获得的结果集不包含任何空值。

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ANY
```

2. 在降采样查询中，如果结果集的某一行所有列都为 null，则过滤掉该行；即获得的结果集不包含所有值都为 null 的行。

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ALL
```