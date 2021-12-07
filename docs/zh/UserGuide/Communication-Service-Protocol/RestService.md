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

## RESTful 服务
IoTDB 的 RESTful 服务可用于查询、写入和管理操作，它使用 OpenAPI 标准来定义接口并生成框架。



### 鉴权
RESTful 服务使用了基础（basic）鉴权，每次 URL 请求都需要在 header 中携带 `'Authorization': 'Basic ' + base64.encode(username + ':' + password)`。



### 接口

#### ping

请求方式：`GET`

请求路径：http://ip:port/ping

示例中使用的用户名为：root，密码为：root

请求示例：

```shell
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
```
响应参数：

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
用户名密码认证失败示例：
```json
{
  "code": 600,
  "message": "WRONG_LOGIN_PASSWORD_ERROR"
}
```



#### query

请求方式：`POST`

请求头：`application/json`

请求路径：http://ip:port/rest/v1/query

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |

请求示例:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4, s3 + 1 from root.sg27 limit 2"}' http://127.0.0.1:18080/rest/v1/query
```

响应参数:

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| expressions | array | 结果集列名的数组 |
| timestamps | array | 时间戳列 |
|values|array|值列数组，列数与结果集列名数组的长度相同|

响应示例:

```json
{
  "expressions": [
    "root.sg27.s3",
    "root.sg27.s4",
    "root.sg27.s3 + 1"
  ],
  "timestamps": [1,2],
  "values": [[11,12],[false,true],[12.0,23.0]]
}
```



#### nonQuery

请求方式：`POST`

请求头：`application/json`

请求路径：http://ip:port/rest/v1/nonQuery

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |

请求示例:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/v1/nonQuery
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



#### insertTablet

请求方式：`POST`

请求头：`application/json`

请求路径：http://ip:port/rest/v1/insertTablet

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  timestamps | array | 是 |  时间列  |
|  measurements | array | 是  | 测点名称 |
| dataTypes | array | 是  | 数据类型  |
|  values | array | 是  | 值列，每一列中的值可以为 `null` |
|  isAligned | boolean | 是  | 是否是对齐时间序列 |
|  deviceId | boolean | 是  | 设备名称 |

请求示例：
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"dataTypes":["INT32","BOOLEAN"],"values":[[11,null],[false,true]],"isAligned":false,"deviceId":"root.sg27"}' http://127.0.0.1:18080/rest/v1/insertTablet
```

响应参数：

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



### 配置

配置位于 `iotdb-rest.properties` 中。



* 将 `enable_rest_service` 设置为 `true` 以启用该模块，而将 `false` 设置为禁用该模块。默认情况下，该值为 `false`。

```properties
enable_rest_service=true
```

* 仅在 `enable_rest_service=true` 时生效。将 `rest_service_port `设置为数字（1025~65535），以自定义REST服务套接字端口。默认情况下，值为 `18080`。

```properties
rest_service_port=18080
```

* REST Service 是否开启 SSL 配置，将 `enable_https` 设置为 `true` 以启用该模块，而将 `false` 设置为禁用该模块。默认情况下，该值为 `false`。

```properties
enable_https=false
```

* keyStore 所在路径（非必填）

```properties
key_store_path=
```


* keyStore 密码（非必填）

```properties
key_store_pwd=
```


* trustStore 所在路径（非必填）

```properties
trust_store_path=
```

* trustStore 密码（非必填）

```properties
trust_store_pwd=
```


* SSL 超时时间，单位为秒

```properties
idle_timeout=5000
```


* 缓存客户登录信息的过期时间（用于加速用户鉴权的速度，单位为秒，默认是8个小时）

```properties
cache_expire=28800
```


* 缓存中存储的最大用户数量（默认是100）

```properties
cache_max_num=100
```

* 缓存初始容量（默认是10）

```properties
cache_init_num=10
```


