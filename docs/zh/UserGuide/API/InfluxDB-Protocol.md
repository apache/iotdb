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

# 切换方案

假如您原先接入InfluxDB的业务代码如下：

  ```java
  influxDB = InfluxDBFactory.connect(openurl, username, password);
  influxDB.createDatabase(database);
  influxDB.insert(ponit);
  influxDB.query(query);
  ```

您只需要将InfluxDBFactory替换为**IotDBInfluxDB**即可实现向IoTDB的切换
 
  ```java
  influxDB = IotDBInfluxDBFactory.connect(openurl, username, password);
  influxDB.createDatabase(database);
  influxDB.insert(ponit);
  influxDB.query(query);
  ```

