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

# 查询历史可视化工具

IoTDB查询历史可视化工具使用一个监控网页来提供查看查询历史和SQL语句执行时间的服务，同时也可以查看当前host的内存和CPU使用情况。

IoTDB查询历史可视化工具使用`8181`端口。在浏览器中输入`ip:8181`，你就可以看到如下网页：

<img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/65688727-3038e380-e09e-11e9-8266-24ff0a1efa96.png">

> 注意：目前，我们仅支持查看Windows和Linux系统的CPU使用率。如果你在使用其他操作系统，你将看到如下提示信息："Can't get the cpu ratio, because this OS is not support".