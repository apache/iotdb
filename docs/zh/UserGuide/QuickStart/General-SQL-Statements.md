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

## 数据库管理

数据库（Database）类似关系数据库中的 Database，是一组结构化的时序数据的集合。

* 创建数据库

 创建一个名为 root.ln 的数据库,语法如下：
```
CREATE DATABASE root.ln
```
* 查看数据库
  
查看所有数据库：
```
SHOW DATABASES
```
* 删除数据库
  
删除名为 root.ln 的数据库：
```
DELETE DATABASE root.ln
```
* 统计数据库数量
  
统计数据库的总数
```
COUNT DATABASES
```
## 时间序列管理

时间序列（Timeseries）是以时间为索引的数据点的集合，在IoTDB中时间序列指的是一个测点的完整序列，本节主要介绍时间序列的管理方式。

* 创建时间序列

需指定编码方式与数据类型。例如创建一条名为root.ln.wf01.wt01.temperature的时间序列：
```
CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH datatype=FLOAT,ENCODING=RLE
```
* 查看时间序列
  
查看所有时间序列：
```
SHOW TIMESERIES
```

使用通配符匹配数据库root.ln下的时间序列：

```
SHOW TIMESERIES root.ln.**
```
* 删除时间序列
  
创建名为 root.ln.wf01.wt01.temperature 的时间序列
```
DELETE TIMESERIES root.ln.wf01.wt01.temperature
```
* 统计时间序列
  
统计时间序列的总数
```
COUNT TIMESERIES root.**
```
统计某通配符路径下的时间序列数量：
```
COUNT TIMESERIES root.ln.**
```
## 时间序列路径管理

除时间序列概念外，IoTDB中还有子路径、设备的概念。

**子路径：**是一条完整时间序列名称中的一部分路径，如时间序列名称为root.ln.wf01.wt01.temperature，则root.ln、root.ln.wf01、root.ln.wf01.wt01都是其子路径。

**设备：**是一组时间序列的组合，在 IoTDB 中设备是由root至倒数第二级节点的子路径，如时间序列名称为root.ln.wf01.wt01.temperature，则root.ln.wf01.wt01是其设备

* 查看设备
```
SHOW DEVICES
```

* 查看子路径
  
查看 root.ln 的下一层：
```
SHOW CHILD PATHS root.ln
```
* 查看子节点
  
查看 root.ln 的下一层：
```
SHOW CHILD NODES root.ln
```
* 统计设备数量
  
统计所有设备
```
COUNT DEVICES
```
* 统计节点数
  
统计路径中指定层级的节点个数
```
COUNT NODES root.ln.** LEVEL=2
```
## 查询数据

以下为IoTDB中常用查询语句。

* 查询指定时间序列的数据

查询root.ln.wf01.wt01设备下的所有时间序列的数据

```
SELECT * FROM root.ln.wf01.wt01
```

* 查询某时间范围内的时间序列数据

查询root.ln.wf01.wt01.temperature时间序列中时间戳大于 2022-01-01T00:05:00.000 的数据

```
SELECT temperature FROM root.ln.wf01.wt01 WHERE time > 2022-01-01T00:05:00.000
```

* 查询数值在指定范围内的时间序列数据

查询root.ln.wf01.wt01.temperature时间序列中数值大于 36.5 的数据：

```
SELECT temperature FROM root.ln.wf01.wt01 WHERE temperature > 36.5
```

* 使用 last 查询最新点数据
```
SELECT last * FROM root.ln.wf01.wt01
```



