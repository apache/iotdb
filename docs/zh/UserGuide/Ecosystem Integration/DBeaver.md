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

## DBeaver-IoTDB

DBeaver 是一个 SQL 客户端和数据库管理工具。DBeaver 可以使用 IoTDB 的 JDBC 驱动与 IoTDB 进行交互。

### DBeaver 安装

* DBeaver 下载地址：https://dbeaver.io/download/

### IoTDB 安装

* 下载 IoTDB 二进制版本
  * IoTDB 下载地址：https://iotdb.apache.org/Download/
  * 版本 >= 0.13.0
* 或者从源代码中编译
  * 参考 https://github.com/apache/iotdb

### 连接 IoTDB 与 DBeaver

1. 启动 IoTDB 服务

   ```shell
   ./sbin/start-server.sh
   ``` 
2. 启动 DBeaver

3. 打开 Driver Manager

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2010.56.22%20AM.png?raw=true)
4. 为 IoTDB 新建一个驱动类型

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2010.56.51%20AM.png?raw=true)

5. 下载[源代码](https://iotdb.apache.org/zh/Download/)，解压并运行下面的命令编译 jdbc 驱动

   ```shell
   mvn clean package -pl jdbc -am -DskipTests -P get-jar-with-dependencies
   ```
7. 在`jdbc/target/`下找到并添加名为`apache-iotdb-jdbc-{version}-jar-with-dependencies.jar`的库，点击 `Find Class`。

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202022-04-26%20at%205.57.32%20PM.png?raw=true)

8. 编辑驱动设置

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2011.03.03%20AM.png?raw=true)
  
9. 新建 DataBase Connection， 选择 iotdb

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2011.05.44%20AM.png?raw=true) 

10. 编辑 JDBC 连接设置

   ```
   JDBC URL: jdbc:iotdb://127.0.0.1:6667/
   Username: root
   Password: root
   ```
   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2011.07.09%20AM.png?raw=true)

11. 测试连接

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2011.07.31%20AM.png?raw=true)

12. 可以开始通过 DBeaver 使用 IoTDB

   ![](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/DBeaver/Screen%20Shot%202021-05-17%20at%2011.08.33%20AM.png?raw=true)
