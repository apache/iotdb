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

## Rest API

IoTDB's Rest API is designed for supporting integration with Grafana and Prometheus， and also provides query, insert and non query interfaces. It uses OpenAPI standard to define the interfaces and generate framework source codes.

In order to secure the IoTDB database, we recommend using a non IoTDB server to install a reverse proxy service (such as nginx) to forward HTTP requests to iotdb. Of course, you can directly use rest of OpenAPI without using the reverse proxy service. If you are using grafana service and nginx at the same time, you need to add  in nginx.conf Header 'access control Max age' XX .

Now, OpenAPI interface uses basic authentication. Every URL request needs to carry 'authorization':'basic '+ Base64. Encode (user name +': '+ password) in the header. for example:

```
location /rest/ {
   proxy_pass  http://ip:port/rest/;  
   add_header 'Access-Control-Max-Age' 20;
}
```

### Configuration

The configuration is located in `iotdb-engines.properties`, set `enable_openApi` to `true` to enable the module while `false` to disable it.
By default, the value is `false`.

```
enable_openApi=true
```

Only take effects when `enable_openApi=true`. Set `openApi_port` as a number (1025~65535) to customize your rest service socket port.

By default, the value is `18080`.
```
openApi_port=18080
```

Number of storage groups when setting Prometheus data store

```
sg_count=5
```

OpenAPI enables SSL configuration and sets "enable_ Set "HTTPS" to "true" to enable the module and "false" to disable the module.
By default, the value is "false".

```
enable_https=false
```

Keystore path

```
key_store_path=/xxx/xxx.keystore
```
Password for keystore

```
key_store_pwd=xxxx
```
trustStore path（Not required）

```
trust_store_path=xxxx
```

Password for trustStore
```
trust_store_pwd=xxxx
```

SSL timeout in seconds

```
idle_timeout=5000
```

In the following doc, we suppose your IoTDB binds 127.0.0.1 and 18080 port.

### Health check 

Check if the iotdb service is working. 

//TODO what returned message means "working" and what means not??

Request method：get

url：http://ip:port/ping

```shell
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
## SQL interfaces

###  SQL query interface

Request method：post
content-type：application/json
url：http://ip:port/rest/read
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root limit 1 slimit 2"}' http://127.0.0.1:18080/rest/read
$ [{"values":[1],"name":"Time","type":"INT64"},{"values":[1.1],"name":"root.ln.wf02","type":""},{"values":[2.0],"name":"root.ln.wf03","type":""}]
```
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | true  |   |


Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| values | array |  values |
| name  |  string | measurements |
| type | String| data type |

###  SQL non query interface

Request method：post
content-type：application/json
url：http://ip:port/rest/nonQuery

```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/nonQuery
$ {"code":200,"message":"execute sucessfully"}
```
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | true  |   |


Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| code | integer |  Status code |
| message  |  string | message |

###  write interfaces

Request method：post
content-type：application/json
url：http://ip:port/rest/write

```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"params":["timestamp","a","b","c"],"values":[1,1,2,4],"paths":["root","ln"]}' http://127.0.0.1:18080/rest/nonQuery
$ {"code":200,"message":"execute sucessfully"}
```
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  params | array | true |  measurements  |
|  values | array | true  | values  |
|  paths | array | true  | paths  |


Return parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| values | array |  values |
| name  |  string | measurements |
| type | String| data type |


### grafana interface

#### Serve for getting time series name level by level by Grafana
Request method：post
content-type：application/json
url：http://ip:port/rest/grafana/node
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '["root","sg5"]' http://127.0.0.1:18080/rest/grafana/node
$ {"internal":["st01"],"series":[{"name":"temperature","leaf":true},{"name":"st01","leaf":false}]}
```
Request example：
```json
["root","sg5"]
```
Response examples：
```json
["wf01","wf02","wf03"]
```

##Auto Downsampling data query for Grafana
Request method：post
content-type：application/json
url：http://ip:port/rest/grafana/query/json
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"interval":"1s","stime":"1616554359000","etime":"1616554369000","paths":["root","sg6","val01"]}' http://127.0.0.1:18080/rest/grafana/query/json
$ [{"datapoints":[null,1616554359000,5.0,1616554360000,7.0,1616554361000,7.0,1616554362000,null,1616554363000,7.0,1616554364000,7.0,1616554365000,null,1616554366000,null,1616554367000,null,1616554368000],"target":"root.sg6.val01"}]
```
Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | true  |  interval |
| stime  |  number |  true |  Start time (timestamp) |
|  etime | number|  true |  End time (timestamp) |
| paths  |  array|  true |  Timeseries is root.sg Convert to paths to ["root", "sg"] |
| fills  |  array | false  |  fill |
| dataType  |  string | 否  |  Filling function |
| previous  |  string |  否 |  Fill type |
| duration  |  string |  否 |  duration |
Request example：
```json
{"paths":["root","ln"],"limitAll":{"slimit":"1","limit":""},"aggregation":"AVG","groupBy":{"samplingInterval":"1s","step":"1s"},"stime":1627286097811,"etime":1627286397811}
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
      [
        0.8999999761581421,
        1627286097811
      ],
      [
        0.75,
        1627286098811
      ],
      [
        0.75,
        1627286099811
      ],
      [
        1.0499999523162842,
        1627286100811
      ],
      [
        1.0499999523162842,
        1627286101811
      ],
      [
        0.6000000238418579,
        1627286102811
      ]
    ],
    "target":"root.ln.wf03"
  }
]
```
##Auto Downsampling data query for Grafana（DataFrame）
Request method：post
content-type：application/json
url：http://ip:port/rest/grafana/query/frame
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"interval":"1s","stime":"1616554359000","etime":"1616554369000","paths":["root","sg7"]}' http://127.0.0.1:18080/rest/grafana/query/frame
$ [{"values":[1616554359000,1616554360000,1616554361000,1616554362000,1616554363000,1616554364000,1616554365000,1616554366000,1616554367000,1616554368000],"name":"Time","type":"time"},{"values":[5.0,7.0,7.0,null,7.0,7.0,null,null,null],"name":"root.sg7.val02","type":"DOUBLE"},{"values":[5.0,null,null,null,null,null,null,null,null],"name":"root.sg7.val03","type":"DOUBLE"}]
```

Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | true  |  interval |
| stime  |  number |  true |  Start time (timestamp) |
|  etime | number|  true |  End time (timestamp) |
| paths  |  array|  true |  Timeseries is root.sg Convert path to ["root", "sg"] |
| fills  |  array | false  |  fill |
| dataType  |  string | 否  |  Filling function |
| previous  |  string |  否 |  Fill type |
| duration  |  string |  否 |  duration |
Request example：
```json
{"paths":["root","ln"],"limitAll":{"slimit":"1","limit":""},"aggregation":"AVG","groupBy":{"samplingInterval":"1s","step":"1s"},"stime":1627286097811,"etime":1627286397811}
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
    "name":"root.sg7.val02",
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
    "name":"root.sg7.val03",
    "type":"DOUBLE"
  }
]
```

## prometheus

### prometheus wirte 
Prometheus data is transmitted after protobuf (3.12.3) encoding and snappy compression

Request method：post
content-type：application/x-protobuf
url：http://ip:port/rest/prometheus/write
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
url：http://ip:port/rest/prometheus/query
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

