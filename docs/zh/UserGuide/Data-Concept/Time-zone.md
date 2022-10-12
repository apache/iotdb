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

# 时区

客户端连接 IoTDB 服务器时，可以指定该连接所要使用的时区。如果未指定，则**默认以客户端所在的时区作为连接的时区。**

在 JDBC 和 Session 原生接口连接中均可以设置时区，使用方法如下：

```java
JDBC: (IoTDBConnection) connection.setTimeZone("+08:00");

Session: session.setTimeZone("+08:00");
```

在 CLI 命令行工具中，通过命令手动设置时区的方式为：

```sql
SET time_zone=+08:00
```

查看当前连接使用的时区的方法如下：

```java
JDBC: (IoTDBConnection) connection.getTimeZone();

Session: session.getTimeZone();
```

CLI 中的方法为：

```sql
SHOW time_zone
```

## 时区使用场景

IoTDB 服务器只针对时间戳进行存储和处理，时区只用来与客户端进行交互，具体场景如下：

1. 将客户端传来的日期格式的字符串转化为相应的时间戳。

   例如，执行写入 `insert into root.sg.d1(timestamp, s1) values(2021-07-01T08:00:00.000, 3.14)`

   则 `2021-07-01T08:00:00.000`将会根据客户端所在的时区转换为相应的时间戳，如果在东八区，则会转化为`1625097600000` ，等价于 0 时区 `2021-07-01T00:00:00.000` 的时间戳值。

   >  Note：同一时刻，不同时区的日期不同，但时间戳相同。

   

2. 将服务器返回给客户端结果中包含的时间戳转化为日期格式的字符串。

   以上述情况为例，执行查询 `select * from root.sg.d1`，则服务器会返回 (1625097600000, 3.14) 的时间戳值对，如果使用 CLI 命令行客户端，则 `1625097600000` 又会被根据时区转化为日期格式的字符串，如下图所示：

   ```
   +-----------------------------+-------------+
   |                         Time|root.sg.d1.s1|
   +-----------------------------+-------------+
   |2021-07-01T08:00:00.000+08:00|         3.14|
   +-----------------------------+-------------+
   ```

   而如果在 0 时区的客户端执行查询，则显示结果将是：

   ```
   +-----------------------------+-------------+
   |                         Time|root.sg.d1.s1|
   +-----------------------------+-------------+
   |2021-07-01T00:00:00.000+00:00|         3.14|
   +-----------------------------+-------------+
   ```

   注意，此时返回的时间戳是相同的，只是不同时区的日期不同。
