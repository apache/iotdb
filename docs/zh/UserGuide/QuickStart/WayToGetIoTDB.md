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

## 下载与安装

IoTDB 为您提供了两种安装方式，您可以参考下面的建议，任选其中一种：

第一种，从官网下载安装包。这是我们推荐使用的安装方式，通过该方式，您将得到一个可以立即使用的、打包好的二进制可执行文件。

第二种，使用源码编译。若您需要自行修改代码，可以使用该安装方式。

### 安装环境要求

安装前请保证您的电脑上配有 JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

如果您需要从源码进行编译，还需要安装：

1. Maven >= 3.6 的运行环境，具体安装方法可以参考以下链接：[https://maven.apache.org/install.html](https://maven.apache.org/install.html)。

> 注： 也可以选择不安装，使用我们提供的'mvnw.sh' 或 'mvnw.cmd' 工具。使用时请用'mvnw.sh' 或 'mvnw.cmd'命令代替下文的'mvn'命令。

### 从官网下载二进制可执行文件

您可以从 [http://iotdb.apache.org/Download/](http://iotdb.apache.org/Download/) 上下载已经编译好的可执行程序 iotdb-xxx.zip，该压缩包包含了 IoTDB 系统运行所需的所有必要组件。

下载后，您可使用以下操作对 IoTDB 的压缩包进行解压：

```
Shell > uzip iotdb-<version>.zip
```

### 使用源码编译

您可以获取已发布的源码 [https://iotdb.apache.org/Download/](https://iotdb.apache.org/Download/) ，或者从 [https://github.com/apache/iotdb/tree/master](https://github.com/apache/iotdb/tree/master) git 仓库获取

源码克隆后，进入到源码文件夹目录下。如果您想编译已经发布过的版本，可以先用`git checkout -b my_{project.version} v{project.version}`命令新建并切换分支。比如您要编译0.12.4这个版本，您可以用如下命令去切换分支：

```shell
> git checkout -b my_0.12.4 v0.12.4
```

切换分支之后就可以使用以下命令进行编译：

```
> mvn clean package -pl server -am -Dmaven.test.skip=true
```

编译后，IoTDB 服务器会在 "server/target/iotdb-server-{project.version}" 文件夹下，包含以下内容：

```
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- tools/      <-- system tools
```

如果您想要编译项目中的某个模块，您可以在源码文件夹中使用`mvn clean package -pl {module.name} -am -DskipTests`命令进行编译。如果您需要的是带依赖的 jar 包，您可以在编译命令后面加上`-P get-jar-with-dependencies`参数。比如您想编译带依赖的 jdbc jar 包，您就可以使用以下命令进行编译：

```shell
> mvn clean package -pl jdbc -am -DskipTests -P get-jar-with-dependencies
```

编译完成后就可以在`{module.name}/target`目录中找到需要的包了。

### 通过 Docker 安装 (Dockerfile)

Apache IoTDB 的 Docker 镜像已经上传至 [https://hub.docker.com/r/apache/iotdb](https://hub.docker.com/r/apache/iotdb)，

1. **获取 IoTDB docker 镜像**

   - **推荐**：执行 `docker pull apache/iotdb:latest` 即可获取最新的 docker 镜像。

   - 用户也可以根据代码提供的 Dockerfile 文件来自己生成镜像。Dockerfile 存放在的 docker 工程下的 src/main/Dockerfile 中。

     - 方法 1：```$ docker build -t iotdb:base git://github.com/apache/iotdb#master:docker```

     - 方法 2：
       ```shell
       $ git clone https://github.com/apache/iotdb
       $ cd iotdb
       $ mvn package -DskipTests
       $ cd docker
       $ docker build -t iotdb:base .
       ```

   当 docker image 在本地构建完成的时候 （示例中的 tag 为 iotdb:base)，已经距完成只有一步之遥了！

2. **创建数据文件和日志的 docker 挂载目录 (docker volume):**
```
$ docker volume create mydata
$ docker volume create mylogs
```
3. **运行 docker 容器：**

```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
您可以使用`docker ps`来检查是否运行成功，当成功时控制台会输出下面的日志：
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
您可以使用下面的命令来获取 container 的 ID:

```shell
$ docker container ls
```
假设这个 ID 为 <C_ID>.

然后使用下面的命令获取这个 ID 对应的 IP 地址，假设获取的 IP 为 <C_IP>:

```shell
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
现在 IoTDB 服务器已经启动成功了。

4. 如果您想尝试使用 iotdb-cli 命令行，您可以使用如下命令：

```shell
$ docker exec -it <C_ID> /bin/bash
$ (now you have enter the container): /iotdb/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```

还可以使用本地的 iotdb-cli，执行如下命令：

```shell
$ /%IOTDB_HOME%/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```
5. 如果您想写一些代码来插入或者查询数据，您可以在 pom.xml 文件中加入下面的依赖：

```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.13.0-SNAPSHOT</version>
        </dependency>
```
这里是一些使用 IoTDB-JDBC 连接 IoTDB 的示例：https://github.com/apache/iotdb/tree/master/example/jdbc/src/main/java/org/apache/iotdb
