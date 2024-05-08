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

When a client connects to the IoTDB server, it can specify the time zone to be used for this connection. If not specified, the default time zone is the one of the client.

The time zone can be set in both JDBC and session native interface connections. The usage is as follows:

```java
JDBC: (IoTDBConnection) connection.setTimeZone("+08:00");

Session: session.setTimeZone("+08:00");
```

In the CLI command line tool, the way to manually set the time zone through command is as follows:

```java
SET time_zone=+08:00
```

The way to view the time zone used by the current connection is as follows:

```java
JDBC: (IoTDBConnection) connection.getTimeZone();

Session: session.getTimeZone();
```

In CLI:

```sql
SHOW time_zone
```

## Time zone usage scenarios

The IoTDB server only stores and processes time stamps, and the time zone is only used to interact with clients. The specific scenarios are as follows:

1. Convert the time format string sent from the client to the corresponding time stamp.

   For example，execute `insert into root.sg.d1(timestamp, s1) values(2021-07-01T08:00:00.000, 3.14)`

   Then `2021-07-01T08:00:00.000` will be converted to the corresponding timestamp value according to the time zone of the client. If it's in GMT+08:00,  the result will be `1625097600000` ，which is equal to the timestamp value of  `2021-07-01T00:00:00.000` in GMT+00:00。

   > Note: At the same time, the dates of different time zones are different, but the timestamps are the same.

   

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

   Note that the timestamps returned are the same, but the dates shown in different time zones are different.
