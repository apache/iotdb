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
OpenApi 接口使用了基础（basic）鉴权，每次url请求都需要在header中携带 'Authorization': 'Basic ' + base64.encode(username + ':' + password)

## grafana接口

## 检查iotdb服务是否在运行
请求方式：get
请求Url：http://ip:port/ping
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
请求url：http://ip:port/v1/grafana/node
请求示例：
```json
["root","xxxx","xxxx"]
```
返回参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------ |
|  internal |  array | 返回的节点值  |
| series  |  array |  返回节点名称和类型 |
|  name |  string | 节点名称|
|  leaf | boolean  |  叶子节点为true，非叶子节点为false |
响应示例：
```json
{"internal": ["sg0","sg1"],
  "series":[
  {
  "name": "sg0",
  "leaf": false
}, {
      "name": "sg1",
      "leaf": false
    }
  ]
}
```

##为Grafana提供自动降采样数据查询
请求方式：post
请求头：application/json
请求url：http://ip:port/v1/grafana/query/json
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | 是  |  间隔 |
| stime  |  number |  是 |  开始时间(时间戳) |
|  etime | number|  是 |  结束时间(时间戳) |
| paths  |  array|  是 |  timeseries 为root.sg 转换成path为["root","sg"] |
| fills  |  object | 否  |  填充 |
| dtype  |  string |  否 |  填充类型 |
| fun  |  string |  否 |  填充函数 |
请求示例：
```json
{"interval":"1s","stime":"1000","etime":"5000","paths":["root","sg0"]}
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
    "datapoints": [
      null,
      1616554359000,
      1.0,
      1616554360000,
      5.0,
      1616554361000,
      6.0,
      1616554362000,
      null,
      1616554363000,
      9.0,
      1616554364000,
      null,
      1616554365000,
      1.0,
      1616554366000,
      3.0,
      1616554367000,
      2.0,
      1616554368000
    ],
    "target": "root.sg0.node"
  }
]
```
##为Grafana提供自动降采样数据查询（DataFrame）
请求方式：post
请求头：application/json
请求url：http://ip:port/v1/grafana/query/frame
参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  interval | string | 是  |  时间间隔 |
| stime  |  number |  是 |  开始时间(时间戳) |
|  etime | number|  是 |  结束时间(时间戳) |
| paths  |  array|  是 |  timeseries 为root.sg 转换成path为["root","sg"] |
| fills  |  object | 否  |  填充 |
| dtype  |  string |  否 |  填充类型 |
| fun  |  string |  否 |  填充函数 |
请求示例：
```json
{"interval":"1s","stime":"1000","etime":"5000","paths":["root","sg0"]}
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
    "values": [
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
    "name": "Time",
    "type": "time"
  },
  {
    "values": [
      1.0,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ],
    "name": "root.sg0.node",
    "type": "string"
  }
]
```
## prometheus接口

### prometheus wirte接口
prometheus 数据经过Protobuf (3.12.3)编码和snappy 压缩后传输

请求方式：post
请求头：application/x-protobuf
请求url：http://ip:port/v1/prometheus/write
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
请求url：http://ip:port/v1/prometheus/query
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