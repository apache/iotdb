# Overview

IoTDB(Internet of Things Database) is an integrated data management engine designed for timeseries data, which can provide users specific services for data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its intense integration with Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high-speed data input and complex data analysis in the IoT industrial field.

# Main Features

IoTDB's features are as following:

1. Flexible deployment. IoTDB provides users one-click installation tool on the cloud, once-decompressed-used terminal tool and the bridge tool between cloud platform and terminal tool (Data Synchronization Tool).
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage (For one billion data storage, hard drive cost less than $0.23)
3. Efficient directory structure. IoTDB supports efficient oganization for complex timeseries data structure from intelligent networking devices, oganization for timeseries data from devices of the same type, fuzzy searching strategy for massive and complex directory of timeseries data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for timeseries data accross devices and sensors, computation in timeseries field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get start. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Intense integration with Open Source Ecosystem. IoTDB supports Hadoop, Spark, etc. analysis ecosystems and Grafana visualization tool.

For the latest information about IoTDB, please visit our [IoTDB official website](http://tsfile.org/index).

# Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
3. TsFile >= 0.5.0 (TsFile Github page: [https://github.com/thulab/tsfile](https://github.com/thulab/tsfile))
4. IoTDB-JDBC >= 0.5.0 (IoTDB-JDBC Github page: [https://github.com/thulab/iotdb-jdbc](https://github.com/thulab/iotdb-jdbc))

TODO: TsFile and IoTDB-JDBC dependencies will be removed after Jialin Qiao re-structured the Project.

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website’s [Document Part](http://tsfile.org/document).

## Build

If you are not the first time that building IoTDB, remember deleting the following files:
```
rm -rf iotdb/data/
rm -rf iotdb/lib/
```

Then you can build IoTDB using Maven:

```
mvn clean package -Dmaven.test.skip=true
```

If successful, you will see the the following text in the terminal:

```
[INFO] BUILD SUCCESS
```
Otherwise, you may need to check the error statements and fix the problems.

After build, the IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:


```
iotdb/     <-- root path
|
+- bin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- LICENSE    <-- LICENSE
```

> NOTE: We also provide already built JARs and project at [http://tsfile.org/download](http://tsfile.org/download) instead of build the jar package yourself.

## Configure

Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: environment config module (iotdb-env.bat, iotdb-env.sh), system config module (tsfile-format.properties, iotdb-engine.properties) and log config module (logback.xml). All of these kinds of configuration files are put in iotdb/config folder.

For more, you are advised to check our website [document page](http://tsfile.org/document). The forth chapter in User Guide Document will give you the details.

## Start

### Start Server

After that we start the server. Running the startup script: 

```
# Unix/OS X
> ./bin/start-server.sh

# Windows
> bin\start-server.bat
```

### Stop Server

The server can be stopped with ctrl-C or the following script:

```
# Unix/ OS X
> ./bin/stop-server.sh

# Windows
> bin\stop-server.bat
```

### Start Client

Now let's trying to read and write some data from IoTDB using our Client. To start the client, you need to explicit the server's IP and PORT as well as the USER_NAME and PASSWORD. 

```
# Unix/OS X
> ./bin/start-client.sh -h <IP> -p <PORT> -u <USER_NAME>

# Windows
> bin\start-client.bat -h <IP> -p <PORT> -u <USER_NAME>
```

> NOTE: In the system, we set a default user in IoTDB named 'root'. The default password for 'root' is 'root'. You can use this default user if you are making the first try or you didn't create users by yourself.

The command line client is interactive so if everything is ready you should see the welcome logo and statements:

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version 0.5.0


IoTDB> login successfully
IoTDB>
```
### Have a try
Now, you can use IoTDB SQL to operate IoTDB, and when you’ve had enough fun, you can input 'quit' or 'exit' command to leave the client. 

But lets try something slightly more interesting:

``` 
IoTDB> SET STORAGE GROUP TO root.vehicle
execute successfully.
IoTDB> CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE
execute successfully.
```
Till now, we have already create a table called root.vehicle and add a colomn called d0.s0 in the table. Let's take a look at what we have done by 'SHOW TIMESERIES' command.

``` 
IoTDB> SHOW TIMESERIES
===  Timeseries Tree  ===

root:{
    vehicle:{
        d0:{
            s0:{
                 DataType: INT32,
                 Encoding: RLE,
                 args: {},
                 StorageGroup: root.vehicle
            }
        }
    }
}
```
Insert timeseries data is the basic operation of IoTDB, you can use 'INSERT' command to finish this:

```
IoTDB> insert into root.vehicle.d0(timestamp,s0) values(1,101);
execute successfully.
```
The data we've just inserted displays like this:

```
IoTDB> SELECT d0.s0 FROM root.vehicle
+-----------------------+------------------+
|                   Time|root.vehicle.d0.s0|
+-----------------------+------------------+
|1970-01-01T08:00:00.001|               101|
+-----------------------+------------------+
record number = 1
execute successfully.
```

If your session looks similar to what’s above, congrats, your IoTDB is operational!

For more on what commands are supported by IoTDB SQL, see our website [document page](http://tsfile.org/document). The eighth chapter in User Guide Document will give you help.


# TsFile 导入脚本使用说明
## 使用方法

###创建MetaData(自定义创建，样例为测试数据metadata)
```
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
SET STORAGE GROUP TO root.fit.d1;
SET STORAGE GROUP TO root.fit.d2;
SET STORAGE GROUP TO root.fit.p;
```
### 启动import脚本

```
# Unix/OS X
> ./bin/import-csv.sh -h xxx.xxx.xxx.xxx -p xxx -u xxx -pw xxx -f <载入文件路径>

# Windows
> bin\import-csv.bat -h xxx.xxx.xxx.xxx -p xxx -u xxx -pw xxx -f <载入文件路径>
```

### 错误文件
位于当前目录下的csvInsertError.error文件

# TsFile 导出脚本使用说明
## 使用方法

### 启动export脚本
```
# Unix/OS X
> ./bin/export-csv.sh -h xxx.xxx.xxx.xxx -p xxx -u xxx -td <导出文件路径> [-tf <时间格式>]

# Windows
> bin\export-csv.bat -h xxx.xxx.xxx.xxx -p xxx -u xxx -td <导出文件路径> [-tf <时间格式>]
```
