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

## RESTful Services  
IoTDB's RESTful services can be used for query, write, and management operations, using the OpenAPI standard to define interfaces and generate frameworks.

### Enable RESTful Services

RESTful services are disabled by default.

* Developer

  Find the `IoTDBrestServiceConfig` class under `org.apache.iotdb.db.conf.rest` in the sever module, and modify `enableRestService=true`.

* User

  Find the `conf/iotdb.properties` file under the IoTDB installation directory and set `enable_rest_service` to `true` to enable the module.

  ```properties
  enable_rest_service=true
  ```

### Authentication
Except the liveness probe API `/ping`, RESTful services use the basic authentication. Each URL request needs to carry `'Authorization': 'Basic ' + base64.encode(username + ':' + password)`.

The username used in the following examples is: `root`, and password is: `root`.

And the authorization header is

```
Authorization: Basic cm9vdDpyb2901
```

- If a user authorized with incorrect username or password, the following error is returned:

  HTTP Status Code：`401`

  HTTP response body:
    ```json
    {
      "code": 600,
      "message": "WRONG_LOGIN_PASSWORD_ERROR"
    }
    ```

- If the `Authorization` header is missing，the following error is returned:

  HTTP Status Code：`401`

  HTTP response body:
    ```json
    {
      "code": 603,
      "message": "UNINITIALIZED_AUTH_ERROR"
    }
    ```

### Interface

#### ping

The `/ping` API can be used for service liveness probing.

Request method: `GET`

Request path: http://ip:port/ping

The user name used in the example is: root, password: root

Example request: 

```shell
$ curl http://127.0.0.1:18080/ping
```

Response status codes:

- `200`: The service is alive.
- `503`: The service cannot accept any requests now.

Response parameters:

|parameter name  |parameter type |parameter describe|
|:--- | :--- | :---|
|code | integer |  status code |
| message  |  string | message |

Sample response:

- With HTTP status code `200`:

  ```json
  {
    "code": 200,
    "message": "SUCCESS_STATUS"
  }
  ```

- With HTTP status code `503`:

  ```json
  {
    "code": 500,
    "message": "thrift service is unavailable"
  }
  ```

> `/ping` can be accessed without authorization.

#### query

The query interface can be used to handle data queries and metadata queries.

Request method: `POST`

Request header: `application/json`

Request path: http://ip:port/rest/v1/query

Parameter Description:

| parameter name | parameter type | required | parameter description                                        |
| -------------- | -------------- | -------- | ------------------------------------------------------------ |
| sql            | string         | yes      |                                                              |
| rowLimit       | integer        | no       | The maximum number of rows in the result set that can be returned by a query. <br />If this parameter is not set, the `rest_query_default_row_size_limit` of the configuration file will be used as the default value. <br /> When the number of rows in the returned result set exceeds the limit, the status code `411` will be returned. |

Response parameters:

| parameter name | parameter type | parameter description                                        |
| -------------- | -------------- | ------------------------------------------------------------ |
| expressions    | array          | Array of result set column names for data query, `null` for metadata query |
| columnNames    | array          | Array of column names for metadata query result set, `null` for data query |
| timestamps     | array          | Timestamp column, `null` for metadata query                  |
| values         | array          | A two-dimensional array, the first dimension has the same length as the result set column name array, and the second dimension array represents a column of the result set |

**Examples:**

Tip: Statements like `select * from root.xx.**` are not recommended because those statements may cause OOM.

**Expression query**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4, s3 + 1 from root.sg27 limit 2"}' http://127.0.0.1:18080/rest/v1/query
````

```json
{
  "expressions": [
    "root.sg27.s3",
    "root.sg27.s4",
    "root.sg27.s3 + 1"
  ],
  "columnNames": null,
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

**Show child paths**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show child paths root"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Show child nodes**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show child nodes root"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Show all ttl**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show all ttl"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
    "storage group",
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

**Show ttl**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show ttl on root.sg27"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
    "storage group",
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

**Show functions**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show functions"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Show timeseries**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show timeseries"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
    "timeseries",
    "alias",
    "storage group",
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

**Show latest timeseries**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show latest timeseries"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
    "timeseries",
    "alias",
    "storage group",
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

**Count timeseries**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"count timeseries root.**"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Count nodes**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"count nodes root.** level=2"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Show devices**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show devices"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Show devices with storage group**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show devices with storage group"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
    "devices",
    "storage group",
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

**List user**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"list user"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Aggregation**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.sg27"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": [
    "count(root.sg27.s3)",
    "count(root.sg27.s4)"
  ],
  "columnNames": null,
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

**Group by level**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.** group by level = 1"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Group by**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(*) from root.sg27 group by([1635232143960,1635232153960),1s)"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": [
    "count(root.sg27.s3)",
    "count(root.sg27.s4)"
  ],
  "columnNames": null,
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

**Last**

```shell
curl -H "Content-Type:application/json"  -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select last s3 from root.sg27"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "expressions": null,
  "columnNames": [
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

**Disable align**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select * from root.sg27 disable align"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "code": 407,
  "message": "disable align clauses are not supported."
}
```

**Align by device**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select count(s3) from root.sg27 align by device"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "code": 407,
  "message": "align by device clauses are not supported."
}
```

**Select into**

```shell
curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"select s3, s4 into root.sg29.s1, root.sg29.s2 from root.sg27"}' http://127.0.0.1:18080/rest/v1/query
```

```json
{
  "code": 407,
  "message": "select into clauses are not supported."
}
```

#### nonQuery

Request method: `POST`

Request header: `application/json`

Request path: http://ip:port/rest/v1/nonQuery

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

Request path: http://ip:port/rest/v1/insertTablet

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

* The maximum number of rows in the result set that can be returned by a query. When the number of rows in the returned result set exceeds the limit, the status code `411` is returned.

````properties
rest_query_default_row_size_limit=10000
````

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
