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

### 开启RESTful 服务
RESTful 服务默认情况是关闭的
 * 开发者  
   
   找到sever模块中`org.apache.iotdb.db.conf.rest` 下面的`IoTDBRestServiceConfig`类，修改`enableRestService=true`即可。

 * 使用者  
   
   找到IoTDB安装目录下面的`conf/iotdb-common.properties`文件，将 `enable_rest_service` 设置为 `true` 以启用该模块。
   
   ```properties
    enable_rest_service=true
   ```

### 鉴权
除了检活接口 `/ping`，RESTful 服务使用了基础（basic）鉴权，每次 URL 请求都需要在 header 中携带 `'Authorization': 'Basic ' + base64.encode(username + ':' + password)`。

示例中使用的用户名为：`root`，密码为：`root`，对应的 Basic 鉴权 Header 格式为

```
Authorization: Basic cm9vdDpyb2901
```

- 若用户名密码认证失败，则返回如下信息：

    HTTP 状态码：`401`

    返回结构体如下
    ```json
    {
      "code": 600,
      "message": "WRONG_LOGIN_PASSWORD_ERROR"
    }
    ```

- 若未设置 `Authorization`，则返回如下信息：

  HTTP 状态码：`401`

  返回结构体如下
    ```json
    {
      "code": 603,
      "message": "UNINITIALIZED_AUTH_ERROR"
    }
    ```

### 接口

#### ping

ping 接口可以用于线上服务检活。

请求方式：`GET`

请求路径：http://ip:port/ping

请求示例：

```shell
$ curl http://127.0.0.1:18080/ping
```

返回的 HTTP 状态码：

- `200`：当前服务工作正常，可以接收外部请求。
- `503`：当前服务出现异常，不能接收外部请求。

响应参数：

|参数名称  |参数类型  |参数描述|
| ------------ | ------------ | ------------|
| code | integer |  状态码 |
| message  |  string | 信息提示 |

响应示例：

- HTTP 状态码为 `200` 时：

  ```json
  {
    "code": 200,
    "message": "SUCCESS_STATUS"
  }
  ```

- HTTP 状态码为 `503` 时：

  ```json
  {
    "code": 500,
    "message": "thrift service is unavailable"
  }
  ```

> `/ping` 接口访问不需要鉴权。

#### query

query 接口可以用于处理数据查询和元数据查询。

请求方式：`POST`

请求头：`application/json`

请求路径：http://ip:port/rest/v2/query

参数说明:

| 参数名称      |参数类型  |是否必填|参数描述|
|-----------| ------------ | ------------ |------------ |
| sql       | string | 是  |   |
| row_limit | integer | 否 | 一次查询能返回的结果集的最大行数。<br />如果不设置该参数，将使用配置文件的  `rest_query_default_row_size_limit` 作为默认值。<br />当返回结果集的行数超出限制时，将返回状态码 `411`。 |

响应参数:

| 参数名称         |参数类型  |参数描述|
|--------------| ------------ | ------------|
| expressions  | array | 用于数据查询时结果集列名的数组，用于元数据查询时为`null`|
| column_names | array | 用于元数据查询结果集列名数组，用于数据查询时为`null` |
| timestamps   | array | 时间戳列，用于元数据查询时为`null` |
| values       |array|二维数组，第一维与结果集列名数组的长度相同，第二维数组代表结果集的一列|

请求示例如下所示：

提示:为了避免OOM问题，不推荐使用select * from root.xx.** 这种查找方式。

请求示例 表达式查询:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4, s3 + 1 from root.sg27 limit 2"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": [
    "root.sg27.s3",
    "root.sg27.s4",
    "root.sg27.s3 + 1"
  ],
  "column_names": null,
  "timestamps": [
    1635232143960,
    1635232153960
  ],
  "values": [
    [
      11,
      null
    ],
    [
      false,
      true
    ],
    [
      12.0,
      null
    ]
  ]
}
```

请求示例 show child paths:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show child paths root"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "child paths"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27",
      "root.sg28"
    ]
  ]
}
```

请求示例 show child nodes:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show child nodes root"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "child nodes"
  ],
  "timestamps": null,
  "values": [
    [
      "sg27",
      "sg28"
    ]
  ]
}
```

请求示例 show all ttl:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show all ttl"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "database",
    "ttl"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27",
      "root.sg28"
    ],
    [
      null,
      null
    ]
  ]
}
```

请求示例 show ttl:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show ttl on root.sg27"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "database",
    "ttl"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27"
    ],
    [
      null
    ]
  ]
}
```

请求示例 show functions:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show functions"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "function name",
    "function type",
    "class name (UDF)"
  ],
  "timestamps": null,
  "values": [
    [
      "ABS",
      "ACOS",
      "ASIN",
      ...
    ],
    [
      "built-in UDTF",
      "built-in UDTF",
      "built-in UDTF",
      ...
    ],
    [
      "org.apache.iotdb.db.query.udf.builtin.UDTFAbs",
      "org.apache.iotdb.db.query.udf.builtin.UDTFAcos",
      "org.apache.iotdb.db.query.udf.builtin.UDTFAsin",
      ...
    ]
  ]
}
```

请求示例 show timeseries:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show timeseries"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "timeseries",
    "alias",
    "database",
    "dataType",
    "encoding",
    "compression",
    "tags",
    "attributes"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27.s3",
      "root.sg27.s4",
      "root.sg28.s3",
      "root.sg28.s4"
    ],
    [
      null,
      null,
      null,
      null
    ],
    [
      "root.sg27",
      "root.sg27",
      "root.sg28",
      "root.sg28"
    ],
    [
      "INT32",
      "BOOLEAN",
      "INT32",
      "BOOLEAN"
    ],
    [
      "RLE",
      "RLE",
      "RLE",
      "RLE"
    ],
    [
      "SNAPPY",
      "SNAPPY",
      "SNAPPY",
      "SNAPPY"
    ],
    [
      null,
      null,
      null,
      null
    ],
    [
      null,
      null,
      null,
      null
    ]
  ]
}
```

请求示例 show latest timeseries:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show latest timeseries"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "timeseries",
    "alias",
    "database",
    "dataType",
    "encoding",
    "compression",
    "tags",
    "attributes"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg28.s4",
      "root.sg27.s4",
      "root.sg28.s3",
      "root.sg27.s3"
    ],
    [
      null,
      null,
      null,
      null
    ],
    [
      "root.sg28",
      "root.sg27",
      "root.sg28",
      "root.sg27"
    ],
    [
      "BOOLEAN",
      "BOOLEAN",
      "INT32",
      "INT32"
    ],
    [
      "RLE",
      "RLE",
      "RLE",
      "RLE"
    ],
    [
      "SNAPPY",
      "SNAPPY",
      "SNAPPY",
      "SNAPPY"
    ],
    [
      null,
      null,
      null,
      null
    ],
    [
      null,
      null,
      null,
      null
    ]
  ]
}
```

请求示例 count timeseries:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"count timeseries root.**"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "count"
  ],
  "timestamps": null,
  "values": [
    [
      4
    ]
  ]
}
```

请求示例 count nodes:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"count nodes root.** level=2"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "count"
  ],
  "timestamps": null,
  "values": [
    [
      4
    ]
  ]
}
```

请求示例 show devices:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show devices"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "devices",
    "isAligned"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27",
      "root.sg28"
    ],
    [
      "false",
      "false"
    ]
  ]
}
```

请求示例 show devices with database:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show devices with database"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "devices",
    "database",
    "isAligned"
  ],
  "timestamps": null,
  "values": [
    [
      "root.sg27",
      "root.sg28"
    ],
    [
      "root.sg27",
      "root.sg28"
    ],
    [
      "false",
      "false"
    ]
  ]
}
```

请求示例 list user:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"list user"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "user"
  ],
  "timestamps": null,
  "values": [
    [
      "root"
    ]
  ]
}
```

请求示例 原始聚合查询:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.sg27"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": [
    "count(root.sg27.s3)",
    "count(root.sg27.s4)"
  ],
  "column_names": null,
  "timestamps": [
    0
  ],
  "values": [
    [
      1
    ],
    [
      2
    ]
  ]
}
```

请求示例 group by level:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.** group by level = 1"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "count(root.sg27.*)",
    "count(root.sg28.*)"
  ],
  "timestamps": null,
  "values": [
    [
      3
    ],
    [
      3
    ]
  ]
}
```

请求示例 group by:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.sg27 group by([1635232143960,1635232153960),1s)"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": [
    "count(root.sg27.s3)",
    "count(root.sg27.s4)"
  ],
  "column_names": null,
  "timestamps": [
    1635232143960,
    1635232144960,
    1635232145960,
    1635232146960,
    1635232147960,
    1635232148960,
    1635232149960,
    1635232150960,
    1635232151960,
    1635232152960
  ],
  "values": [
    [
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ],
    [
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    ]
  ]
}
```

请求示例 last:
```shell
curl -H "Content-Type:application/json"  -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select last s3 from root.sg27"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "expressions": null,
  "column_names": [
    "timeseries",
    "value",
    "dataType"
  ],
  "timestamps": [
    1635232143960
  ],
  "values": [
    [
      "root.sg27.s3"
    ],
    [
      "11"
    ],
    [
      "INT32"
    ]
  ]
}
```

请求示例 disable align:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root.sg27 disable align"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "code": 407,
  "message": "disable align clauses are not supported."
}
```

请求示例 align by device:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(s3) from root.sg27 align by device"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "code": 407,
  "message": "align by device clauses are not supported."
}
```

请求示例 select into:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4 into root.sg29.s1, root.sg29.s2 from root.sg27"}' http://127.0.0.1:18080/rest/v2/query
```

响应示例:

```json
{
  "code": 407,
  "message": "select into clauses are not supported."
}
```

#### nonQuery

请求方式：`POST`

请求头：`application/json`

请求路径：http://ip:port/rest/v2/nonQuery

参数说明:

|参数名称  |参数类型  |是否必填|参数描述|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | 是  |   |

请求示例:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"CREATE DATABASE root.ln"}' http://127.0.0.1:18080/rest/v2/nonQuery
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

请求路径：http://ip:port/rest/v2/insertTablet

参数说明:

| 参数名称         |参数类型  |是否必填|参数描述|
|--------------| ------------ | ------------ |------------ |
| timestamps   | array | 是 |  时间列  |
| measurements | array | 是  | 测点名称 |
| data_types   | array | 是  | 数据类型  |
| values       | array | 是  | 值列，每一列中的值可以为 `null` |
| is_aligned   | boolean | 是  | 是否是对齐时间序列 |
| device       | string | 是  | 设备名称 |

请求示例：
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"data_types":["INT32","BOOLEAN"],"values":[[11,null],[false,true]],"is_aligned":false,"device":"root.sg27"}' http://127.0.0.1:18080/rest/v2/insertTablet
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

配置位于 `iotdb-common.properties` 中。



* 将 `enable_rest_service` 设置为 `true` 以启用该模块，而将 `false` 设置为禁用该模块。默认情况下，该值为 `false`。

```properties
enable_rest_service=true
```

* 仅在 `enable_rest_service=true` 时生效。将 `rest_service_port `设置为数字（1025~65535），以自定义REST服务套接字端口。默认情况下，值为 `18080`。

```properties
rest_service_port=18080
```

* 将 'enable_swagger' 设置 'true' 启用swagger来展示rest接口信息, 而设置为 'false' 关闭该功能. 默认情况下，该值为 `false`。

```properties
enable_swagger=false
```

* 一次查询能返回的结果集最大行数。当返回结果集的行数超出参数限制时，您只会得到在行数范围内的结果集，且将得到状态码`411`。

```properties
rest_query_default_row_size_limit=10000
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
