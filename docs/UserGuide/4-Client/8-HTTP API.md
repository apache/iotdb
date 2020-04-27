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

### Queries

IoTDB can be queried by JSON objects. Queries can be posted like this:

```
curl -X POST '<host>:<port>/rest/?' -H 'Content-Type:application/json' -H 'Accept:application/json' -H 'Authorization:Basic root:root'-d @<query_json_file>
```

Note: "root:root" should be encoded by Base64.

The query language is JSON over HTTP, The Content-Type/Accept Headers can also take 'text/plain'.

```
curl -X POST '<queryable_host>:<port>/druid/v2/?pretty' -H 'Content-Type:application/json' -H 'Accept:text/plain' -H 'Authorization:Basic root:root' -d @<query_json_file>
```

Note: If Accept header is not provided, it defaults to value of 'Content-Type' header.

### Available queries

* [Set Storage Group](./8-1-Set%20Storage%20Group.md)
* [Create TimeSeries](./8-2-Create%20TimeSeries.md)
* [Insert](8-3-Insert.md)
* [Query](8-4-Query.md)


