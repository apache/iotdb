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

## 检查iotdb服务是否在运行
请求方式：get
请求url：http://ip:port/ping
示例中使用的用户名为：root，密码为：root
```
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
```
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




