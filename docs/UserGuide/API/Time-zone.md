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

When a client connects to the iotdb server, it can specify the time zone to be used for this connection. If not specified, the default time zone value is the time zone of the client.

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



