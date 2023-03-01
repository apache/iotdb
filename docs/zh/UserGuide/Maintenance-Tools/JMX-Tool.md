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

## JMX 工具

Java VisualVM 提供了一个可视化的界面，用于查看 Java 应用程序在 Java 虚拟机（JVM）上运行的详细信息，并对这些应用程序进行故障排除和分析。

### 使用

第一步：获得 IoTDB-server。

第二步：编辑配置文件

* IoTDB 在本地
查看`$IOTDB_HOME/conf/jmx.password`，使用默认用户或者在此添加新用户
若新增用户，编辑`$IOTDB_HOME/conf/jmx.access`，添加新增用户权限

* IoTDB 不在本地
编辑`$IOTDB_HOME/conf/iotdb-env.sh`
修改以下参数：
```
JMX_LOCAL="false"
JMX_IP="the_real_iotdb_server_ip"  # 填写实际 IoTDB 的 IP 地址
```
查看`$IOTDB_HOME/conf/jmx.password`，使用默认用户或者在此添加新用户
若新增用户，编辑`$IOTDB_HOME/conf/jmx.access`，添加新增用户权限

第三步：启动 IoTDB-server。

第四步：使用 jvisualvm
1. 确保安装 jdk 8。jdk 8 以上需要 [下载 visualvm](https://visualvm.github.io/download.html)
2. 打开 jvisualvm
3. 在左侧导航栏空白处右键 -> 添加 JMX 连接
<img style="width:100%; max-width:300px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/81462914-5738c580-91e8-11ea-94d1-4ff6607e7e2c.png">

4. 填写信息进行登录，按下图分别填写，注意需要勾选”不要求 SSL 连接”。
例如：
连接：192.168.130.15:31999
用户名：iotdb
口令：passw!d
<img style="width:100%; max-width:300px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/81462909-53a53e80-91e8-11ea-98df-0012380da0b2.png">
