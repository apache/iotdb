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

# 系统信息查询

系统信息查询是用来查询ip地址、系统时间、cpu负载、总物理内存、剩余物理内存的一种查询。

在IoTDB中可以通过  `SHOW NOW()`来对系统信息进行查询。

**示例：**查询当前系统信息

```
IoTDB> show now()
+------------+-----------------------------+-------+---------------+--------------+
|   IpAddress|                   SystemTime|CpuLoad|TotalMemorySize|FreeMemorySize|
+------------+-----------------------------+-------+---------------+--------------+
|192.168.85.1|2022-02-21T10:40:55.766+08:00|  2.59%|         15.88G|         7.09G|
+------------+-----------------------------+-------+---------------+--------------+
Total line number = 1
It costs 0.151s
IoTDB>
```

