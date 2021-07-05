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

# Time zone

When a client connects to the IoTDB server, it can specify the time zone to be used for this connection. If not specified, the default time zone value is the time zone of the client.

Time zone is used to: 1. Convert the time format string sent from client to corresponding time stamp; 2. Convert the timestamp in the result returned by the server into a time format string.

The time zone can be set in both JDBC and session native interface connections. The usage is as follows:

```java
(IoTDBConnection) connection.setTimeZone("+08:00");

session.setTimeZone("+08:00");
```

The way to view the time zone used by the current connection is as follows:

```java
(IoTDBConnection) connection.getTimeZone();

session.getTimeZone();
```

## Time zone usage scenarios

1. Convert the time format string sent from the client to the corresponding time stamp.

   For example，execute `insert into root.sg.d1(timestamp, s1) values(2021-07-01T08:00:00.000, 3.14)`

   Then `2021-07-01T08:00:00.000` will be converted to the corresponding timestamp value according to the time zone of the client. If it's in GMT+08:00,  the result will be `1625097600000` ，which is equal to the timestamp value of  `2021-07-01T00:00:00.000` in GMT+00:00。

2. Convert the timestamp in the result returned to the client into a time format string.

   Take the above situation as an example，execute `select * from root.sg.d1`，the server will return the time value pair:  `(1625097600000, 3.14)`. If CLI tool is used，then `1625097600000` will be converted into time format string according to time zone, as shown in the figure below：

   ```
   +-----------------------------+-------------+
   |                         Time|root.sg.d1.s1|
   +-----------------------------+-------------+
   |2021-07-01T08:00:00.000+08:00|         3.14|
   +-----------------------------+-------------+
   ```

   If the query is executed on the client in GMT:+00:00, the result will be as follows:

   ```
   +-----------------------------+-------------+
   |                         Time|root.sg.d1.s1|
   +-----------------------------+-------------+
   |2021-07-01T00:00:00.000+00:00|         3.14|
   +-----------------------------+-------------+
   ```

