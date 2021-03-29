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

# OpenApi

OpenAPI interface uses basic authentication. Every URL request needs to carry 'authorization':'basic '+ Base64. Encode (user name +': '+ password) in the header

## grafana interface

##Check if the iotdb service is working
Request method：get
Url：http://ip:port/ping
```
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
$ {"code":4,"type":"ok","message":"login success!"}
```
Response examples
```json
{
"code": 4,
"type": "ok",
"message": "login success!"
}
```
Example of user name password authentication failure
```json
{
  "code": 1,
  "type": "error",
  "message": "username or passowrd is incorrect!"
}
```
##Serve for getting time series name level by level by Grafana
Request method：post
content-type：application/json
url：http://ip:port/v1/grafana/node
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '["root","sg5"]' http://127.0.0.1:18080/v1/grafana/node
$ {"internal":["b"],"series":[{"name":"a1","leaf":true},{"name":"b","leaf":false}]}
```
Request example：
```json
["root","sg5"]
```
Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------ |
|  internal |  array | Returned node value  |
| series  |  array |  Returns the node name and type |
|  name |  string | node name|
|  leaf | boolean  |  The leaf node is true and the non leaf node is false |
Response examples：
```json
{
  "internal":[
    "b"
  ],
  "series":[
    {
      "name":"a1",
      "leaf":true
    },
    {
      "name":"b",
      "leaf":false
    }
  ]
}
```

##Auto Downsampling data query for Grafana
Request method：post
content-type：application/json
url：http://ip:port/v1/grafana/query/json
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"interval":"1s","stime":"1616554359000","etime":"1616554369000","paths":["root","sg6","b2"]}' http://127.0.0.1:18080/v1/grafana/query/json
$ [{"datapoints":[null,1616554359000,5.0,1616554360000,7.0,1616554361000,7.0,1616554362000,null,1616554363000,7.0,1616554364000,7.0,1616554365000,null,1616554366000,null,1616554367000,null,1616554368000],"target":"root.sg6.b2"}]
```
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | true  |  interval |
| stime  |  number |  true |  Start time (timestamp) |
|  etime | number|  true |  End time (timestamp) |
| paths  |  array|  true |  Timeseries is root.sg Convert to paths to ["root", "sg"] |
| fills  |  object | false  |  fill |
| dtype  |  string |  false |  data type |
| fun  |  string |  false |  function |
Request example：
```json
{"interval":"1s","stime":"1616554359000","etime":"1616554368000","paths":["root","sg6","b2"]}
```
Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------ |
|  datapoints |  array | Returns an array with the first value, the second time, and so on  |
| target  |  string |  return timeseries |

Response examples：
```json
[
  {
    "datapoints":[
      null,
      1616554359000,
      5,
      1616554360000,
      7,
      1616554361000,
      7,
      1616554362000,
      null,
      1616554363000,
      7,
      1616554364000,
      7,
      1616554365000,
      null,
      1616554366000,
      null,
      1616554367000,
      null,
      1616554368000
    ],
    "target":"root.sg6.b2"
  }
]
```
##Auto Downsampling data query for Grafana（DataFrame）
Request method：post
content-type：application/json
url：http://ip:port/v1/grafana/query/frame
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"interval":"1s","stime":"1616554359000","etime":"1616554369000","paths":["root","sg6"]}' http://127.0.0.1:18080/v1/grafana/query/frame
$ [{"values":[1616554359000,1616554360000,1616554361000,1616554362000,1616554363000,1616554364000,1616554365000,1616554366000,1616554367000,1616554368000],"name":"Time","type":"time"},{"values":[5.0,7.0,7.0,null,7.0,7.0,null,null,null],"name":"root.sg6.b2","type":"DOUBLE"},{"values":[5.0,null,null,null,null,null,null,null,null],"name":"root.sg6.b3","type":"DOUBLE"}]
```

Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | true  |  interval |
| stime  |  number |  true |  Start time (timestamp) |
|  etime | number|  true |  End time (timestamp) |
| paths  |  array|  true |  Timeseries is root.sg Convert path to ["root", "sg"] |
| fills  |  object | false  |  fill |
| dtype  |  string |  false |  data type |
| fun  |  string |  false |  function |
Request example：
```json
{"interval":"1s","stime":"1000","etime":"5000","paths":["root","sg6"]}
```
Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------ |
|  name |  String | name  |
| type  |  string |  type|
| values  |  array |  value|

Response examples：
```json
[
  {
    "values":[
      1616554359000,
      1616554360000,
      1616554361000,
      1616554362000,
      1616554363000,
      1616554364000,
      1616554365000,
      1616554366000,
      1616554367000,
      1616554368000
    ],
    "name":"Time",
    "type":"time"
  },
  {
    "values":[
      5,
      7,
      7,
      null,
      7,
      7,
      null,
      null,
      null
    ],
    "name":"root.sg6.b2",
    "type":"DOUBLE"
  },
  {
    "values":[
      5,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ],
    "name":"root.sg6.b3",
    "type":"DOUBLE"
  }
]
```
## prometheus

### prometheus wirte 
Prometheus data is transmitted after protobuf (3.12.3) encoding and snappy compression

Request method：post
content-type：application/x-protobuf
url：http://ip:port/v1/prometheus/write
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  Timeseries | array | true  |   |
| Labels  |  array |  true |  tag info |
|  Name | String|  true |  tagKey |
| Value  |  array|  true | tagValue |
| Samples  |  array | true  |   |
| Timestamp  |  number |  true |  timestamp |
| Value  |  number |  true |  true |

Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------ |
|  code |  number | 200：sucess，500：fail，401：No permissions for this operation  |
| message  |  string | message|


Response examples：
```json
{"code": 200,
"message": "write data success"
}
```

### prometheus read 
Prometheus data is transmitted after protobuf (3.12.3) encoding and snappy compression

Request method：post
content-type：application/x-protobuf
url：http://ip:port/v1/prometheus/query
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  Queries | array | true  |   |
| StartTimestampMs  |  number |  true |  Start time (timestamp) |
| EndTimestampMs | number|  true |  End time (timestamp) |
| Matchers  |  array|  true |  |
| Name  |  String |  true |  name |
| Value  |  String |  true |  value |
| Hints  |  array|  true |  |
| StepMs  |  number |  false |  interval |
| Func  |  String |  false |  function |
| StartMs  |  number |  true |  Start time (timestamp) |
| EndMs  |  number |  true |  End time (timestamp) |
| Grouping  |  String |  false |  Aggregate fields |
| By  |  boolean |  false |  Enable aggregation |
| RangeMs  |  number |  false |  sliding step same |

Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
|  Timeseries | array |   |
| Labels  |  array | tag info |
|  Name | String|  tagKey |
| Value  |  array| tagValue |
| Samples  |  array |   |
| Timestamp  |  number|  timestamp |
| Value  |  number | value |