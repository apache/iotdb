# 使用方法

## 环境依赖

* JDK >= 1.8
* Maven > 3.0

## 项目打包

> mvn clean package -Dmaven.test.skip=true

## 打包后的项目结构

> cd tsfiledb

```
tsfiledb/     <-- 根目录
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