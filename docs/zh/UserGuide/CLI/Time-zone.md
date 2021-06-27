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

客户端连接 IoTDB 服务器时，可以指定该连接所要使用的时区。如果未指定，则**默认以服务器所在的时区作为默认的时区值。**

时区被用来：1. 将客户端传来的时间格式的字符串转化为相应的时间戳；2. 将服务器返回给客户端结果中包含的时间戳转化为时间格式字符串。

在 JDBC 和 Session 原生接口连接中均可以设置时区，使用方法如下：

```java
(IoTDBConnection) connection.setTimeZone("+08:00");

session.setTimeZone("+08:00");
```

查看当前连接使用的时区的方法如下：

```java
(IoTDBConnection) connection.getTimeZone();

session.getTimeZone();
```



