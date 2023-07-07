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

## 环境要求

要使用IoTDB，你需要具备以下条件：

* Java >= 1.8
> 1.8, 11到17都是经过验证的。请确保环境路径已被相应设置。

* Maven >= 3.6
> 如果你想从源代码编译和安装IoTDB）。
* 设置最大打开文件数为65535，以避免出现 "太多的打开文件 "的错误。
* (可选)将somaxconn设置为65535，以避免系统在高负载时出现 "连接重置 "错误。


> **# Linux** <br>`sudo sysctl -w net.core.somaxconn=65535` <br>**# FreeBSD 或 Darwin** <br>`sudo sysctl -w kern.ipc.somaxconn=65535`

