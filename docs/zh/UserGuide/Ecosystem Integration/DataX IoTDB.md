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
# DataX IoTDB

[DataX](https://github.com/alibaba/DataX) iotdbwriter 插件，用于通过 DataX 同步其他数据源的数据到 IoTDB 中。

这个插件是利用IotDB session功能进行数据导入的。需要配合 DataX 服务一起使用。

## 关于 DataX

DataX 是阿里云 DataWorks数据集成 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能。

更多信息请参阅: `https://github.com/alibaba/DataX/`

## 代码说明

DataX iotdbwriter 插件代码 [这里](./iotdbwriter)。

这个目录包含插件代码以及 DataX 项目的开发环境。

iotdbwriter 插件依赖的 DataX 代码中的一些模块。而这些模块并没有在 Maven 官方仓库中。所以我们在开发 iotdbwriter 插件时，需要下载完整的 DataX 代码库，才能进行插件的编译和开发。

### 目录结构

1. `iotdbwriter/`

   这个目录是 iotdbwriter 插件的代码目录。这个目录中的所有代码，都托管在 Apache IotDB 的代码库中。

   iotdbwriter 插件帮助文档在这里：`iotdbwriter/doc`

2. `init-env.sh`

   这个脚本主要用于构建 DataX 环境，他主要进行了以下操作：

   1. 将 DataX 的二进制部署压缩包下载到本地。
   2. 将 DataX 的压缩包解压。

   这个脚本执行后，开发者就可以进入 `iotdbwriter/` 目录开始开发或编译了。

### 编译部署

1. 运行 `init-env.sh`

2. 按需修改 `iotdbwriter/src` 中的代码。

3. 单独编译iotdbwriter：

   进入`iotdbwriter`目录内编译：
   ```shell script
    mvn -U clean package assembly:single -Dmaven.test.skip=true 
   ```
   
   如果编译时出现错误`Could not find artifact com.alibaba.datax:datax-common:pom:0.0.1-SNAPSHOT ...`，可尝试以下方式解决：
   
   - 解压 alibaba-datax-maven-m2-20210928.tar.gz
   - 解压后，将得到的 alibaba/datax/ 目录，拷贝到所使用的 maven 对应的 .m2/repository/com/alibaba/ 下。
   - 再次尝试编译。
   
4. 将编译后的`iotdbwriter`插件，复制到Datax的安装目录里：

   进入`datax-iotdb-connector`目录，执行命令。
   ```shell script
   cp -R iotdbwriter/target/datax/plugin/writer/iotdbwriter datax/plugin/writer/ 
   ```



### 使用datax传输数据

进入Datax的安装目录`datax/`后，执行`python bin/datax.py ${USER_DEFINE}.json` ，即可启动一个datax传输作业。

其中 ${USER_DEFINE}.json 为datax的作业配置文件。



## 插件详细说明

###  IOTDBWriter 插件文档

#### 1 快速介绍

IOTDBWriter支持将大批量数据写入IOTDB中。

####  2 实现原理

IOTDBWriter 通过IOTDB原生支持Session方式导入数据， IOTDBWriter会将`reader`读取的数据进行缓存在内存中，然后批量导入至IOTDB。

####  3 功能说明

#####  3.1 配置样例

这里是一份从Stream读取数据后导入至IOTDB的配置文件。

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "guangzhou",
                "type": "string"
              },
              {
                "random": "2014-07-07 00:00:00, 2016-07-07 00:00:00",
                "type": "date"
              },
              {
                "random": "0, 19890604",
                "type": "long"
              },
              {
                "random": "2, 10",
                "type": "bool"
              },
              {
                "random": "10, 23410",
                "type": "DOUBLE"
              }
            ],
            "sliceRecordCount": 100000
          }
        },
        "writer": {
          "name": "iotdbwriter",
          "parameter": {
            "column": [
              {
                "name": "loc",
                "type": "text"
              },
              {
                "name": "timeseries",
                "type": "date"
              },
              {
                "name": "s2",
                "type": "int64"
              },
              {
                "name": "s3",
                "type": "boolean"
              },
              {
                "name": "s4",
                "type": "double"
              }
            ],
            "username": "root",
            "password": "root",
            "host": "localhost",
            "port": 6667,
            "deviceId": "root.sg4.d1",
            "batchSize": 1000
          }
        }
      }
    ]
  }
}

```

##### 3.2 参数说明

* **username**

  - 描述：访问IotDB数据库的用户名
  - 必选：是
  - 默认值：无

* **password**

  - 描述：访问IotDB数据库的密码
  - 必选：是
  - 默认值：无

* **deviceId**

  - 描述：写入IotDB的设备id。
  - 必选：是
  - 默认值：无


* **host**

  - 描述：访问IotDB数据库的ip地址。
  - 必选：是
  - 默认值：无

* **port**

  - 描述：访问IotDB数据库的端口。
  - 必选：是
  - 默认值：无

* **column**

  - 描述：**需要写入数据**的字段，字段之间用英文逗号分隔。字段中必须包含名为**timeseries**的字段，且类型为date。字段类型为iotDB支持的写入数据类型，包含：BOOLEAN，INT32，INT64，FLOAT，DOUBLE，TEXT。例如: "column": [{ "name": "s2", "type": "int64"}]。
  - 必选：是
  - 默认值：否

* **batchSize**

  - 描述：一次写入数据的批量数目
  - 必选：否
  - 默认值：无

