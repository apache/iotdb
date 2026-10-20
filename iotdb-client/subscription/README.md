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

# Subscription column filters

Table topics support a `column-filter` expression over column metadata:
`database`, `table_name`, `column_name`, `datatype`, and `category`.
String literals use double quotes, and comparisons are case insensitive.
The default expression is `true`, which includes every column.

## Streaming schema semantics

Column filters follow schema changes while the subscription runs. When a
table has a matching column, its TAG columns are retained automatically,
including TAGs added after topic creation or subscription. For example:

```sql
CREATE TOPIC selected_measurements WITH (
  'database' = 'factory',
  'table' = 'sensors',
  'column-filter' = 'column_name IN ("temperature", "pressure")'
);
```

This selects `temperature` and `pressure` together with all TAGs. Adding a
`location` TAG later includes it in subsequent data without altering the
topic or restarting the consumer. FIELDs outside the predicate are excluded.
A predicate such as `category = "FIELD"` also picks up new FIELDs, while
`column_name LIKE "sensor%"` picks up new matching column names.
New tables matching the topic's database and table patterns are included
automatically. Datatype predicates use the latest committed table schema.
Tree view predicates use view column names and return the corresponding
source FIELDs. ATTRIBUTE columns are excluded from filtered payloads even
when they match; matching them still retains the table's TAGs. Tables with
no matching column produce no rows. TIME selection remains per table and
uses its declared time column name.

The predicate is applied when data is prepared for delivery, for both
historical and incremental data and for record and TsFile payloads. Data
already prepared for delivery keeps its existing projection. There is no
fixed schema snapshot option for column filtering. The topic's
`mode = snapshot` still controls the historical data cutoff; it does not
freeze the column schema.

## Evaluation cost

Each topic parses its expression once. Before pruning a Tablet, the
DataNode reads the committed schema from its local table cache. It
reuses the compiled column selection while that schema instance is
unchanged and recompiles only the affected table after a schema update.
This adds one local cache lookup per Tablet, without remote schema
requests or expression evaluation per row. If a table's schema is not
available locally, its data is skipped until the schema becomes available.
