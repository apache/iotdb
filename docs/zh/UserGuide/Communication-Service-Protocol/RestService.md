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

## OpenAPI 协议（RESTful 服务）
IoTDB 的 RESTful 服务可用于查询、写入和管理操作，它使用 OpenAPI 标准来定义接口并生成框架。

### 鉴权
RESTful 服务使用了基础（basic）鉴权，每次 url 请求都需要在 header 中携带 'Authorization': 'Basic ' + base64.encode(username + ':' + password)

例如:


### Configuration
配置位于“iotdb-rest.properties”中，将“enable_rest_service”设置为“true”以启用该模块，而将“false”设置为禁用该模块。
默认情况下，该值为“false”。
```
enable_rest_service=true
```

仅在“enable_rest_service=true”时生效。将“rest_service_port”设置为数字（1025~65535），以自定义REST服务套接字端口。
默认情况下，值为“18080”。

```
rest_service_port=18080
```

REST Service 开启ssl配置，将“enable_https”设置为“true”以启用该模块，而将“false”设置为禁用该模块。
默认情况下，该值为“false”。

```
enable_https=false
```

keyStore所在路径

```
key_store_path=
```
keyStore密码

```
key_store_pwd=
```
trustStore所在路径（非必填）

```
trust_store_path=
```

trustStore密码
```
trust_store_pwd=
```
ssl 超时时间单位为秒

```
idle_timeout=5000
```
缓存过期时间(单位s，默认是8个小时)

```
cache_expire=28800
```
缓存中存储的最大用户数量(默认是100) 

```
cache_max_num=100
```

缓存中初始化用户数量(默认是10)

```
cache_init_num=10
```

## 检查iotdb服务是否在运行
请求方式：get
请求url：http://ip:port/ping
示例中使用的用户名为：root，密码为：root
请求示例
```shell
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
```
响应参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

响应示例
```json
{
  "code": 200,
  "message": "SUCCESS_STATUS"
}
```
用户名密码认证失败示例
```json
{
  "code": 600,
  "message": "WRONG_LOGIN_PASSWORD_ERROR"
}
```

###  SQL 查询接口

请求方式：post
请求头：application/json
请求url：http://ip:port/rest/v1/query

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |

请求示例:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root.sg25 limit 1"}' http://127.0.0.1:18080/rest/v1/query
```

响应参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| dataValues | array |  值 |
| measurements  |  string | 测点 |
其中dataValues数组中第一个为测点的值,第二个为时间

响应示例:

```json
[
  {
    "expressions": [
      "root.sg27.s3",
      "root.sg27.s4",
      "root.sg27.s3 + 1"
    ],
    "timestamps": [t1,t2],
    "values": [[11,12],[false,true],[12.0,23.0]]
  }
]
```

###  SQL 非查询接口

请求方式：post
请求头：application/json
请求url：http://ip:port/rest/v1/nonQuery

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |

请求示例:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/v1/nonQuery
```

响应参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

响应示例：
```json
{
"code": 200,
"message": "SUCCESS_STATUS"
}
```

###  写入接口
请求方式：post
请求头：application/json
请求url：http://ip:port/rest/v1/insertTablet

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  timestamps | array | 是 |  时间  |
|  measurements | array | 是  | 测点  |
|  dataType | array | 是  | 数据类型  |
|  values | array | 是  | 值  |
|  isAligned | boolean | 是  | 是否对齐  |
|  deviceId | boolean | 是  | 设备  |
|  rowSize | integer | 是  | 行数  |

请求示例:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"dataType":["INT32","BOOLEAN"],"values":[[11,22],[false,true]],"isAligned":false,"deviceId":"root.sg27","rowSize":2}' http://127.0.0.1:18080/rest/v1/insertTablet
```

响应参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

响应示例:
```json
{
"code": 200,
"message": "SUCCESS_STATUS"
}
```


