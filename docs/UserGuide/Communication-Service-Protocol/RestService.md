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

## REST Service

IoTDB's REST services can be used to query, write, and manage operations. It uses OpenAPI standard to define the interfaces and generate framework source codes.

Now, REST Service interface uses basic authentication. Every URL request needs to carry 'authorization':'basic '+ Base64. Encode (user name +': '+ password) in the header. for example:


### Configuration

The configuration is located in `iotdb-rest.properties`, set `enable_rest_service` to `true` to enable the module while `false` to disable it.
By default, the value is `false`.

```
enable_rest_service=true
```

Only take effects when `enable_rest_service=true`. Set `rest_service_port` as a number (1025~65535) to customize your rest service socket port.

By default, the value is `18080`.
```
rest_service_port=18080
```

REST Service enables SSL configuration and sets "enable_https" to "true" to enable the module and "false" to disable the module.
By default, the value is "false".

```
enable_https=false
```

Keystore path

```
key_store_path=
```
Password for keystore

```
key_store_pwd=
```
trustStore path（Not required）

```
trust_store_path=
```

Password for trustStore
```
trust_store_pwd=
```

SSL timeout in seconds

```
idle_timeout=5000
```

cache expiration time of REST Service (in seconds)

```
cache_expire=28800
```
maximum number of users stored in cache(Default 100)

```
cache_max_num=100
```

init number of users stored in cache(Default 10)

```
cache_init_num=10
```

In the following doc, we suppose your IoTDB binds 127.0.0.1 and 18080 port.

### Health check 

Check if the iotdb service is working. 

Request method：get

url：http://ip:port/ping

The user name used in the example is: root and the password is: root

Request examples
```shell
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
```

Response parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------ |
|  code |  number | status code  |
| message  |  string | message|

Response examples
```json
{
  "code": 200,
  "message": "SUCCESS_STATUS"
}
```

Example of user name password authentication failure
```json
{
  "code": 600,
  "message": "WRONG_LOGIN_PASSWORD_ERROR"
}
```

###  SQL query interface

Request method：post
content-type：application/json
url：http://ip:port/rest/v1/query

Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | true  |   |

Request examples:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root.sg25 limit 1"}' http://127.0.0.1:18080/rest/v1/query
```

Response parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| dataValues | array |  value |
| measurements  |  string | measurement |
The first in the datavalues array is the value of the measuring point, and the second is the time

Response examples:
```json
[
  {
    "measurements": "root.sg25.s3",
    "dataValues": [
      [
        22.1,
        1635232143960
      ]
    ]
  }
]
```

###  SQL non query interface

Request method：post
content-type：application/json
url：http://ip:port/rest/v1/nonQuery

Parameter description:

|Parameter name  |Parameter type  |required|description|
| ------------ | ------------ | ------------ |------------ |
|  sql | string | true  |   |

Request examples:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/v1/nonQuery
```

Response parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| code | integer |  status code |
| message  |  string | message |

Response examples:
```json
{
"code": 200,
"message": "SUCCESS_STATUS"
}
```

###   write interfaces
Request method：post
content-type：application/json
url：http://ip:port/rest/v1/insertTablet

Parameter description
| ------------ | ------------ | ------------ |------------ |
|  timestamps | array | true |  timestamps  |
|  measurements | array | true  | measurement  |
|  dataType | array | true  | data type  |
|  values | array | true  | value  |
|  isAligned | boolean | true  | Align  |
|  deviceId | boolean | true  | device  |
|  rowSize | integer | 是  | row size  |

Request examples:
```shell
$ curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"dataType":["INT32","BOOLEAN"],"values":[[11,22],[false,true]],"isAligned":false,"deviceId":"root.sg27","rowSize":2}' http://127.0.0.1:18080/rest/v1/insertTablet
```

Response parameters:

|Parameter name  |Parameter Type  |description|
| ------------ | ------------ | ------------|
| code | integer |  status code |
| message  |  string | message |

Response examples:
```json
{
"code": 200,
"message": "SUCCESS_STATUS"
}
```

