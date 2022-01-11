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

## RESTful Service  
IoTDB's RESTful services can be used for query, write, and management operations, using the OpenAPI standard to define interfaces and generate frameworks.



### Authentication
RESTful services use basic authentication. Each URL request needs to carry `'Authorization': 'Basic ' + base64.encode(username + ':' + password)`.



### Interface

#### ping

Request method: `GET`

Request path: http://ip:port/ping

The user name used in the example is: root, password: root

Example request: 

```shell
$ curl -H "Authorization:Basic cm9vdDpyb2901" http://127.0.0.1:18080/ping
```
Response parameters:

|parameter name  |parameter type |parameter describe|
|:--- | :--- | :---|
|code | integer |  status code |
| message  |  string | message |

Sample response:
```json
{
  "code": 200,
  "message": "SUCCESS_STATUS"
}
```
Example Of user name and password authentication failure:
```json
{
  "code": 600,
  "message": "WRONG_LOGIN_PASSWORD_ERROR"
}
```



#### query

Request method: `POST`

Request header: `application/json`

Request path:http://ip:port/rest/v1/query

Parameter Description:

|parameter name  |parameter type |is required|parameter describe|
|:--- | :--- | :---| :---|
|  sql | string | yes  |  query content |

Example request:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4, s3 + 1 from root.sg27 limit 2"}' http://127.0.0.1:18080/rest/v1/query
```

Response parameters:

|  parameter name  |  parameter type |  parameter describe|
| :--- |  :--- |  --- |
|expressions | array |An array of result set column names|
|timestamps | array | Timestamp column |
|values| array |Value column, the number of columns is the same length as the result set column name array |

Sample response:

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

Request method: `POST`

Request header: `application/json`

Request path:http://ip:port/rest/v1/nonQuery

Parameter Description:

|parameter name  |parameter type |parameter describe|
|:--- | :--- | :---|
|  sql | string | query content  | 

Example request:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"set storage group to root.ln"}' http://127.0.0.1:18080/rest/v1/nonQuery
```

Response parameters:

|parameter name  |parameter type |parameter describe|
|:--- | :--- | :---|
| code | integer |  status code |
| message  |  string | message |

Sample response:
```json
{
  "code": 200,
  "message": "SUCCESS_STATUS"
}
```



#### insertTablet

Request method: `POST`

Request header: `application/json`

Request path:http://ip:port/rest/v1/insertTablet

Parameter Description:

|parameter name  |parameter type |is required|parameter describe|
|:--- | :--- | :---| :---| 
|  timestamps | array | yes |  Time column  |
|  measurements | array | yes  | The name of the measuring point |
| dataTypes | array | yes  | The data type |
|  values | array | yes  | Value columns, the values in each column can be `null` |
|  isAligned | boolean | yes  | Whether to align the timeseries |
|  deviceId | boolean | yes  | Device name |

Example request:
```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"dataTypes":["INT32","BOOLEAN"],"values":[[11,null],[false,true]],"isAligned":false,"deviceId":"root.sg27"}' http://127.0.0.1:18080/rest/v1/insertTablet
```

Sample response:

|parameter name  |parameter type |parameter describe|
|:--- | :--- | :---|
| code | integer |  status code |
| message  |  string | message |

Sample response:
```json
{
  "code": 200,
  "message": "SUCCESS_STATUS"
}
```



### Configuration

The configuration is located in 'iotdb-rest.properties'.



* Set 'enable_REST_service' to 'true' to enable the module, and 'false' to disable the module. By default, this value is' false '.

```properties
enable_rest_service=true
```

* This parameter is valid only when 'enable_REST_service =true'. Set 'rest_service_port' to a number (1025 to 65535) to customize the REST service socket port. By default, the value is 18080.

```properties
rest_service_port=18080
```

* REST Service whether to enable SSL configuration, set 'enable_https' to' true 'to enable the module, and set' false 'to disable the module. By default, this value is' false '.

```properties
enable_https=false
```

* keyStore location path (optional)

```properties
key_store_path=
```


* keyStore password (optional)

```properties
key_store_pwd=
```


* trustStore location path (optional)

```properties
trust_store_path=
```

* trustStore password (optional)

```properties
trust_store_pwd=
```


* SSL timeout period, in seconds

```properties
idle_timeout=5000
```


* Expiration time for caching customer login information (used to speed up user authentication, in seconds, 8 hours by default)

```properties
cache_expire=28800
```


* Maximum number of users stored in the cache (default: 100)

```properties
cache_max_num=100
```

* Initial cache size (default: 10)

```properties
cache_init_num=10
```


