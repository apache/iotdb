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
