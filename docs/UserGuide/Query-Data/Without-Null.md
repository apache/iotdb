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

# Null Value Filter

In IoTDB, the `WITHOUT NULL` clause can be used to filter null values in the result set. There are two filtering strategies:

1. IoTDB will join all the sensor value by its time, and if some sensors don't have values in that timestamp, we will fill it with null. In some analysis scenarios, we only need the row if all the columns of it have value.

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ANY
```

2. In group by query, we will fill null for any group by interval if the columns don't have values in that group by interval. However, if all columns in that group by interval are null, maybe users don't need that RowRecord, so we can use `WITHOUT NULL ALL` to filter that row.

```sql
select * from root.ln.** where time <= 2017-11-01T00:01:00 WITHOUT NULL ALL
```

