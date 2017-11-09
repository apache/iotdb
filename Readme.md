# Overview
IoTDB(Internet of Things Database) is an integrated data management engine designed for timeseries data, which can provide users specific services for data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its intense integration with Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high-speed data input and complex data analysis in the IoT industrial field.

# Key features

IoTDB's features are as following:

1. Flexible deployment. IoTDB provides users one-click installation tool on the cloud, once-decompressed-used terminal tool and the bridge tool between cloud platform and terminal tool (Data Synchronization Tool).
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage (For one billion data storage, hard drive cost less than $0.23)
3. Efficient directory structure. IoTDB supports efficient oganization for complex timeseries data structure from intelligent networking devices, oganization for timeseries data from devices of the same type, fuzzy searching strategy for massive and complex directory of timeseries data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for timeseries data accross devices and sensors, computation in timeseries field (frequency domain transform) and rich aggregation function support in time dimension.
6. Easy to get start. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Intense integration with Open Source Ecosystem. IoTDB supports Hadoop, Spark, etc. analysis ecosystems and Grafana visualization tool.


# 使用方法

## 环境依赖

* JDK >= 1.8
* Maven > 3.0

## 项目打包

> mvn clean package -Dmaven.test.skip=true

## 打包后的项目结构

> cd iotdb

```
iotdb/     <-- 根目录
|
+- bin/       <-- 运行脚本
|
+- conf/      <-- 配置文件目录
|
+- lib/       <-- 项目依赖目录
|
+- LICENSE    <-- 代码LICENSE
```


## 启动服务器

```
# Unix/OS X
> ./bin/start-server.sh

# Windows
> bin\start-server.bat
```

## 关闭服务器

```
# Unix/ OS X
> ./bin/stop-server.sh

# Windows
> bin\stop-server.bat
```

## 启动客户端

```
# Unix/OS X
> ./bin/start-client.sh -h xxx.xxx.xxx.xxx -p xxx -u xxx

# Windows
> bin\start-client.bat -h xxx.xxx.xxx.xxx -p xxx -u xxx
```

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
> ./bin/export-csv.sh -h xxx.xxx.xxx.xxx -p xxx -u xxx -tf <导出文件路径> [-t <时间格式>]

# Windows
> bin\export-csv.bat -h xxx.xxx.xxx.xxx -p xxx -u xxx -tf <导出文件路径> [-t <时间格式>]
```