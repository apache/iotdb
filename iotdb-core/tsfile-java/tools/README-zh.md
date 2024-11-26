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

[English](./README.md) | [中文](./README-zh.md)
# TsFile Tools 手册
## 简介

## 开发

### 前置条件

构建 Java 版的 TsFile Tools，必须要安装以下依赖:

1. Java >= 1.8 (1.8, 11 到 17 都经过验证. 请确保设置了环境变量).
2. Maven >= 3.6 (如果要从源代码编译TsFile).


### 使用 maven 构建

```
mvn clean package -P with-java -DskipTests
```

### 安装到本地机器

```
mvn install -P with-java -DskipTests
```

## schema 定义

| 参数         | 说明                       | 是否必填 | 默认值  |
|------------|--------------------------|------|------|
| table_name | 表名                       | 是    |      |
| time_precision | 时间精度（可选值有：ms/us/ns）      | 否    | ms   |
| has_header | 是否包含表头 (可选值有：true/false) | 否    | true |
| separator | 行内分隔符（可选值有：, /tab/ ;）    | 否    | ,    |
| null_format | 空值                       | 否    |    |
| id_columns | 主键列，支持cvs中不存在的列做为层级      | 否    |      |
| time_column | 时间列                      | 是    |      |
| csv_columns | 按照顺序与csv列一一对应            | 是    |      |

说明：

id_columns 按照顺序进行设置值，支持csv 文件中不存在的列作为层级
例如csv 只有a,b,c,d,time五列则
id_columns
a1 default aa
a
其中a1 不在csv列，为虚拟列，默认值为aa

csv_columns 之后的内容为值列的定义，每一行的第一个字段为在tsfile中的测点名，第二个字段为类型
当csv中某一列不需要写入 tsfile时，可以设置为 SKIP
例：
csv_columns
地区 TEXT，
厂号 TEXT,
设备号 TEXT,
SKIP,
SKIP,
时间 INT64,
温度 FLOAT,
排量 DOUBLE,

### 数据示例
csv 文件内容
```
地区,  厂号, 设备号,  型号, 维修周期, 时间,   温度,   排量
河北, 1001,     1,    10,       1,    1,   80.0, 1000.0
河北, 1001,     1,    10,       1,    4,   80.0, 1000.0
河北, 1002,     7,     5,       2,    1,   90.0, 1200.0
```
schema 定义

```
table_name=root.db1
time_precision=ms
has_header=true
separator=,
null_format=\N


id_columns
集团 DEFAULT 大唐
地区
厂号
设备号

time_column=时间

csv_columns
地区 TEXT，
厂号 TEXT,
设备号 TEXT,
SKIP,
SKIP,
时间 INT64,
温度 FLOAT,
排量 DOUBLE,
```
## 命令

```
csv2tsfile.sh --source ./xxx/xxx --target /xxx/xxx --fail_dir /xxx/xxx 
csv2tsfile.bat --source ./xxx/xxx --target /xxx/xxx --fail_dir /xxx/xxx 
```


