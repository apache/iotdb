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

# Identifier

## Usage scenarios

Certain objects within IoTDB, including `TRIGGER`, `FUNCTION`(UDF), `CONTINUOUS QUERY`, `SCHEMA TEMPLATE`, `USER`, `ROLE`,`Pipe`,`PipeSink`,`alias` and other object names are known as identifiers.

## Constraints

Below are basic constraints of identifiers, specific identifiers may have other constraints, for example, `user` should consists of more than 4 characters. 

- Permitted characters in unquoted identifiers:
  - [0-9 a-z A-Z _ ] (letters, digits and underscore)
  - ['\u2E80'..'\u9FFF'] (UNICODE Chinese characters)
- Identifiers may begin with a digit, unquoted identifiers can not be a real number.
- Identifiers are case sensitive.
- Key words can be used as an identifier.

**You need to quote the identifier with back quote(`) in the following cases:**

- Identifier contains special characters.
- Identifier that is a real number.

## How to use quotations marks in quoted identifiers

`'` and `"` can be used directly in quoted identifiers.

` may be written as `` in quoted  identifiers. See the example below:

```sql
# create template t1't"t
create schema template `t1't"t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)

# create template t1`t
create schema template `t1``t` 
(temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

## Examples

Examples of case in which quoted identifier is used ：

- Trigger name should be quoted in cases described above ：

  ```sql
  # create trigger named alert.`listener-sg1d1s1
  CREATE TRIGGER `alert.``listener-sg1d1s1`
  AFTER INSERT
  ON root.sg1.d1.s1
  AS 'org.apache.iotdb.db.engine.trigger.example.AlertListener'
  WITH (
    'lo' = '0', 
    'hi' = '100.0'
  )
  ```

- UDF name should be quoted in cases described above ：

  ```sql
  # create a funciton named 111, 111 is a real number.
  CREATE FUNCTION `111` AS 'org.apache.iotdb.udf.UDTFExample'
  ```

- Template name should be quoted in cases described above ：

  ```sql
  # create a template named 111, 111 is a real number.
  create schema template `111` 
  (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
  ```

- User and Role name should be quoted in cases described above, blank space is not allow in User and Role name whether quoted or not ：

  ```sql
  # create user special`user.
  CREATE USER `special``user.` 'write_pwd'
  
  # create role 111
  CREATE ROLE `111`
  ```

- Continuous query name should be quoted in cases described above ：

  ```sql
  # create continuous query test.cq
  CREATE CONTINUOUS QUERY `test.cq` 
  BEGIN 
    SELECT max_value(temperature) 
    INTO temperature_max 
    FROM root.ln.*.* 
    GROUP BY time(10s) 
  END
  ```

- Pipe、PipeSink should be quoted in cases described above ：

  ```sql
  # create PipeSink test.*1
  CREATE PIPESINK `test.*1` AS IoTDB ('ip' = '输入你的IP')
  
  # create Pipe test.*2
  CREATE PIPE `test.*2` TO `test.*1` FROM 
  (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS) WITH 'SyncDelOp' = 'true'
  ```

- `AS` function provided by IoTDB can assign an alias to time series selected in query. Alias can be constant(including string) or identifier.

  ```sql
  select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
  
  # Header of result dataset
  +-----------------------------+-----------|-----+
  |                         Time|temperature|speed|
  +-----------------------------+-----------|-----+
  ```

- The key/value of an attribute can be String Literal and identifier, more details can be found at **key-value pair** part. 



