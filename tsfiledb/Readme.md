# 使用方法

## 环境依赖

* JDK >1.8
* Maven > 3.0

## 项目打包

> mvn clean package -Dmaven.test.skip=true

## 打包后的项目结构

> cd tsfiledb

```
tsfiledb/		<-- 根目录
|
+- bin/			<-- 运行脚本
|
+- conf/			<-- 配置文件目录
|
+- lib/			<-- 项目依赖目录
|
+- LICENSE		<-- 代码LICENSE
```


## 启动服务器

> ./bin/start-server.sh

## 关闭服务器

> ./bin/stop-server.sh

# 启动客户端

> ./bin/start-client.sh -h xxx.xxx.xxx.xxx -p xxx -u xxx

#TsFile Load脚本使用说明
## 使用方法

###创建MetaData(自定义创建，样例为测试数据metadata)
```
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
SET STORAGE GROUP TO root.fit.d1;
```
### 启动load脚本

```
> ./bin/start-csvToTsfile.sh.sh -h 127.0.0.1 -p 6667 -u root -pw root -f <载入文件路径> -t <时间格式>
``` 

### 错误文件
位于${TSFILE_HOME}下
