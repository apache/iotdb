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
### Data queries

These types of queries take a query object and return an array of JSON objects where data
is queried successfully or not.

An example set storage group query object is shown below:

```json
{
  "type": "query",
  "range": {
    "from": "0",
    "to": "6"
  },
  "targets": [
    {
    "target": "root.ln.wf01.wt01.temperature",
    "type": "timeserie"
    }
  ]
}
```

If successfully, you will get a result below:

```json
[{"datapoints":[[1,"1.1"],[2,"2.2"],[3,"3.3"],[4,"4.4"],[5,"5.5"]],"target":"root.ln.wf01.wt01.temperature"}]
```

| property | description | required? | 
| --- | --- | --- | 
| type | describe query type | yes | 
| targets | data will be queried | yes |
| target | one time series | yes |
| type | table or timeserie | yes |

You possibly get several errors below:

| HTTP status | error | description |
| --- | --- | --- |
| 500 | Get request body JSON failed | Get request body JSON failed |
| 500 | <storage group> : errorMessage | you will find the wrong message in each time series if it encounters an exception|
| 500 | Type is wrong | occur when url isn't compatible with key "type" in json |    