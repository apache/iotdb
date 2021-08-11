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

# Rest API
IoTDB的restApi设计用于支持与Grafana和Prometheus的集成同时也提供了查询、插入和non-query接口,它使用OpenAPI标准来定义接口和生成框架源代码。
为了IoTDB数据库安全我们建议使用一台非IoTDB的服务器安装反向代理服务（例如nginx等）把http请求转发到IoTDB，当然你可以不使用反向代理服务直接使用openapi的rest，如果你正在使用grafana服务同时使用nginx则需要在nginx.conf添加 add_header 'Access-Control-Max-Age' xx 来保证可以正常使用
OpenApi 接口使用了基础（basic）鉴权，每次url请求都需要在header中携带 'Authorization': 'Basic ' + base64.encode(username + ':' + password)例如:

```
location /rest/ {
   proxy_pass  http://ip:port/rest/;  
   add_header 'Access-Control-Max-Age' 20;
}
```

### Configuration
配置位于“iotdb-engines.properties”中，将“enable_openApi”设置为“true”以启用该模块，而将“false”设置为禁用该模块。
默认情况下，该值为“false”。
```
enable_openApi=true
```

仅在“enable_openApi=true”时生效。将“openApi_port”设置为数字（1025~65535），以自定义rest服务套接字端口。
默认情况下，值为“18080”。

```
openApi_port=18080
```

设置Prometheus数据存储时的存储组数量

```
sg_count=5
```

openApi 开启ssl配置，将“enable_https”设置为“true”以启用该模块，而将“false”设置为禁用该模块。
默认情况下，该值为“false”。

```
enable_https=false
```

keyStore所在路径

```
key_store_path=/xxx/xxx.keystore
```
keystore密码

```
key_store_pwd=xxxx
```
trustStore所在路径（非必填）

```
trust_store_path=xxxx
```

trustStore密码
```
trust_store_pwd=xxxx
```
ssl 超时时间单位为秒

```
idle_timeout=5000
```

## grafana接口

## 检查iotdb服务是否在运行
请求方式：get
请求url：http://ip:port/ping
```
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
$ {"code":4,"type":"ok","message":"login success!"}
```
响应示例
```json
{
"code": 4,
"type": "ok",
"message": "login success!"
}
```
用户名密码认证失败示例
```json
{
  "code": 1,
  "type": "error",
  "message": "username or passowrd is incorrect!"
}
```

##用于通过Grafana逐级获取时间序列名称
请求方式：post
请求头：application/json
请求url：http://ip:port/rest/grafana/node
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '["root","sg5"]' http://127.0.0.1:18080/rest/grafana/node
$ ["wf01","wf02","wf03"]
```
请求示例：
```json
["root","sg5"]
```

响应示例：
```json
["wf01","wf02","wf03"]
```

##为Grafana提供自动降采样数据查询
请求方式：post
请求头：application/json
请求url：http://ip:port/rest/grafana/query/json
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"paths":["root","ln","wf02"],"aggregation":"AVG","groupBy":{"samplingInterval":"1s","step":"1s"},"stime":1627286097811,"etime":1627286397811}' http://127.0.0.1:18080/rest/grafana/query/json
$ [{"datapoints":[[1.2,1627285095273],[0.75,1627285096273],[0.45,1627285097273],[0.15,1627285098273],[0.9,1627285099273],[0.0,1627285100273],[0.15,1627285101273]],"target":"root.ln.wf02"}]
```
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | 是  |  间隔 |
| stime  |  number |  是 |  开始时间(时间戳) |
| etime | number|  是 |  结束时间(时间戳) |
| paths  |  array|  是 |  timeseries 为root.sg 转换成path为["root","sg"] |
| aggregation  |  string | 否  | 函数 |
| groupBy  |  object | 否  | 分组 |
| samplingInterval  |  string | 否  | 降采样间隔 |
| step  |  string | 否  | 降采样步长 |
| limitAll  |  object | 否  | 限制 |
| slimit  |  string | 否（默认值为10）  |  列数 |
| limit  |  string | 否  |  行数 |
| fills  |  array | 否  |  填充 |
| dataType  |  string | 否  |  填充函数 |
| previous  |  string |  否 |  填充类型 |
| duration  |  string |  否 |  时间范围 |
请求示例：
```json
{"paths":["root","ln"],"limitAll":{"slimit":"1","limit":""},"aggregation":"AVG","groupBy":{"samplingInterval":"1s","step":"1s"},"stime":1627286097811,"etime":1627286397811}
```
返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------ |
|  datapoints |  array | 返回数组第一个为值，第二个为时间，依次类推  |
| target  |  string |  返回查询timeseries |

响应示例：
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
##为Grafana提供自动降采样数据查询（DataFrame）
请求方式：post
请求头：application/json
请求url：http://ip:port/rest/grafana/query/frame
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"interval":"1s","stime":"1616554359000","etime":"1616554369000","paths":["root","sg7"]}' http://127.0.0.1:18080/rest/grafana/query/frame
$ [{"values":[1616554359000,1616554360000,1616554361000,1616554362000,1616554363000,1616554364000,1616554365000,1616554366000,1616554367000,1616554368000],"name":"Time","type":"time"},{"values":[5.0,7.0,7.0,null,7.0,7.0,null,null,null],"name":"root.sg7.val02","type":"DOUBLE"},{"values":[5.0,null,null,null,null,null,null,null,null],"name":"root.sg7.val03","type":"DOUBLE"}]
```

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | 是  |  间隔 |
| stime  |  number |  是 |  开始时间(时间戳) |
| etime | number|  是 |  结束时间(时间戳) |
| paths  |  array|  是 |  timeseries 为root.sg 转换成path为["root","sg"] |
| aggregation  |  string | 否  | 函数 |
| groupBy  |  object | 否  | 分组 |
| samplingInterval  |  string | 否  | 降采样间隔 |
| step  |  string | 否  | 降采样步长 |
| limitAll  |  object | 否  | 限制 |
| slimit  |  string | 否（默认值为10）  |  列数 |
| limit  |  string | 否  |  行数 |
| fills  |  array | 否  |  填充 |
| dataType  |  string | 否  |  填充函数 |
| previous  |  string |  否 |  填充类型 |
| duration  |  string |  否 |  时间范围 |
请求示例：
```json
{"paths":["root","ln"],"limitAll":{"slimit":"1","limit":""},"aggregation":"AVG","groupBy":{"samplingInterval":"1s","step":"1s"},"stime":1627286097811,"etime":1627286397811}
```
返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------ |
|  name |  String | 字段名称  |
| type  |  string |  字段类型|
| values  |  array |  字段值|

响应示例：
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
    "type":"INT64"
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
## prometheus接口

### prometheus wirte接口
prometheus 数据经过Protobuf (3.12.3)编码和snappy 压缩后传输

请求方式：post
请求头：application/x-protobuf
请求url：http://ip:port/rest/prometheus/write
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  Timeseries | array | 是  |   |
| Labels  |  array |  是 |  tag信息 |
|  Name | String|  是 |  tagKey |
| Value  |  array|  是 | tagValue |
| Samples  |  array | 是  |   |
| Timestamp  |  number |  是 |  时间 |
| Value  |  number |  是 |  值 |

返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------ |
|  code |  number | 200 成功，500 失败，401没有权限  |
| message  |  string | 说明|


响应示例：
```json
{"code": 200,
"message": "write data success"
}
```

### prometheus read 接口
prometheus 数据经过Protobuf (3.12.3)编码和snappy 压缩后传输

请求方式：post
请求头：application/x-protobuf
请求url：http://ip:port/rest/prometheus/query
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  Queries | array | 是  |   |
| StartTimestampMs  |  number |  是 |  开始时间 |
| EndTimestampMs | number|  是 |  结束时间 |
| Matchers  |  array|  是 |  |
| Name  |  String |  是 |  名称 |
| Value  |  String |  是 |  值 |
| Hints  |  array|  是 |  |
| StepMs  |  number |  否 |  时间间隔 |
| Func  |  String |  否 |  函数 |
| StartMs  |  number |  是 |  开始时间 |
| EndMs  |  number |  否 |  结束时间 |
| Grouping  |  String |  否 |  聚合字段 |
| By  |  boolean |  否 |  是否启用group by |
| RangeMs  |  number |  否 |  滑动步长 |

返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
|  Timeseries | array |   |
| Labels  |  array | tag信息 |
|  Name | String|  tagKey |
| Value  |  array| tagValue |
| Samples  |  array |   |
| Timestamp  |  number|  时间 |
| Value  |  number | 值 |

## 其他接口

###  read 接口

请求方式：post
请求头：application/json
请求url：http://ip:port/rest/read
```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root limit 1 slimit 2"}' http://127.0.0.1:18080/rest/read
$ [{"values":[1],"name":"Time","type":"INT64"},{"values":[1.1],"name":"root.ln.wf02","type":""},{"values":[2.0],"name":"root.ln.wf03","type":""}]
```
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |


返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| values | array |  值 |
| name  |  string | 测点名称 |
| type | String| 数据类型 |

###  nonQuery 接口

请求方式：post
请求头：application/json
请求url：http://ip:port/rest/nonQuery

```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/nonQuery
$ {"code":200,"message":"execute sucessfully"}
```
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |


返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

###  写入接口

请求方式：post
请求头：application/json
请求url：http://ip:port/rest/write

```
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"params":["timestamp","a","b","c"],"values":[1,1,2,4],"paths":["root","ln"]}' http://127.0.0.1:18080/rest/nonQuery
$ {"code":200,"message":"execute sucessfully"}
```
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  params | array | 是 |  测点名称  |
|  values | array | 是  | 值  |
|  paths | array | 是  | 路径  |


返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

