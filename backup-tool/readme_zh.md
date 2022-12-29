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

# Backup-Tool
IOTDB 的数据导入导出工具,导出的数据主要包含时间序列的结构和其对应的数据。

## 项目结构
本项目分为两个子项目：backup-core和backup-command项目；  
backup-core项目包含主要的业务逻辑和实现方法，该项目可以以jar包的形式对外提供服务。  
backup-command项目是一个工具类包，功能实现依赖于核心项目，提供命令行工具。 

## 编译
前提：
1. java >= 1.8, 需要配置JAVA_HOME环境变量
2. Maven >= 3.6

你可以使用以下命令来编译或者打包这个项目:  
````
mvn clean package;  
mvn install;  
mvn test;   
````
ps: 使用 '-DskipTests' 来跳过测试用例    
由于core-jar还没有放到远程仓库，为了顺利编译工具，首先需要执行mvn install -DskipTests。  
集成测试需要一个真正的iotdbserver，你可以在“test/resource/sesseinConfig.properties”中配置服务器.  

## 特性    
- 基于iotdb-session   
- 基于pipeline的导入导出工具，容易扩展  
    1. ````
       pipeline设计文档: https://apache-iotdb.feishu.cn/wiki/wikcn8zaxOegcQ4vBASwtmbmE4f
       ```` 
- 基于project reactor框架，多轨道执行，支持背压机制  
- 数据导出以设备为最小单元
  1. 数据导出以设备为最小单元，即一个设备对应一个文件，这个文件中包含这个设备的所有measurement数据，因为导出最小单元是设备，所以对应的路径最小应该是设备级别的路径。
- 可以导出一个或多个存储组的数据
- 保证时间序列结构一致性
  1. 可以保证导入导出过程中时间序列的结构不会改变，导出数据的时候可以选择是否导出时间序列的结构信息，这些信息会单独保存在一个文件中；里面详细的记录了时间序列的属性，各个字段的类型、压缩格式、是否是对齐时间序列等等，导入的时候根据这个文件创建对应的时间序列，保证时间序列结构不会变化。
- 支持增量/全量导出
  1. where参数支持选定时间范围，目前不支持非time的参数
- 支持超过1000个以上时间序列的导出
  1. 由于本工具是依赖于iotdb-session，而session查询是有限制的，一次性查询出的时间序列结果是不允许超过1000个的，本工具解决了这个问题，是查询结果中的时间序列大于1000也能正常导出。
- 核心功能提供jar包，方便第三方集成
- 丰富的导出格式： 
  1. 可以查看的导出文件，提供sql、csv两种文件格式可以直接查看  
  2. 不可以查看的导出文件，导出压缩格式的文件，占用磁盘空间比较小，适合做一些备份工作；现在支持的压缩格式TSFILE、SNAPPY、GZIP等

## 导出
导出命令工具：backup-export.bat/backup-export.sh
````
 -h  // iotdb host address
 -p  // port
 -u  // username
 -pw // password
 -f  // fileFolder
 -i  // 要导出的路径
 -sy // 文件生成策略，1.以设备的路径为为文件名生成文件 2、额外提供一个文件来记录文件名与设备路径的对应关系,除了tsfile，其他格式适用
 -se // 是否需要导出时间序列结构,如果是true的话，工具会额外导出一个文件来记录它对应的时间序列结构
 -c  // 压缩格式 : SQL、CSV、SNAPPY、GZIP、LZ4、TSFILE
 -w  // where  条件
 -vn // virtualNum 虚拟存储组个数,导出tsfile格式适用
 -pi // partitionInterval 时间分区间隔,导出tsfile格式适用
````

## 导入
导入命令工具: backup-import.bat/backup-import.sh
````
 -h  // iotdb host address
 -p  // port
 -u  // username
 -pw // password
 -f  // fileFolder
 -se // 是否需要从记录时间序列结构的文件创建对应的时间序列,如果是true的话，创建时间序列
 -c  // 压缩格式 : SQL、CSV、SNAPPY、GZIP、LZ4
````

## 示例
场景描述：  
- 存储组 ： 
  1. root.ln.company1  
  2. root.ln.company2  
- 时间序列：  
  1.root.ln.company1.diggingMachine.d1.position  
  2.root.ln.company1.diggingMachine.d1.weight  
  3.root.ln.company1.diggingMachine.d2.position  
  4.root.ln.company1.diggingMachine.d2.weight  
  5.root.ln.company2.taxiFleet.team1.mileage  
  6.root.ln.company2.taxiFleet.team1.oilConsumption  
  
 > 存储组root.ln.company1 下有两台挖掘机d1、d2  
 > 存储组root.ln.company2 下有1个出租车队team1  
 --- 
 > 导出d1设备，导出文件到d:/company1/machine，文件策略选择第二种(额外文件)，需要生成时间序列结构，文件格式gzip
 ````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\company1\\machine -i root.ln.company1.diggingMachine.d1 -sy true -se true -c gzip
 ````
> 导出结果：  
> TIMESERIES_STRUCTURE.STRUCTURE 记录时间序列结构  
> CATALOG_COMPRESS.CATALOG  记录文件名与设备路径的对应关系  
> 1.gz.bin    
--- 
> 导出company1下所有设备，导出文件到d:/company1/machine，文件策略选择第一种，不需要生成时间序列结构，文件格式csv
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\company1\\machine -i root.ln.company1.** -sy false -se false -c csv
````
> 导出结果：  
> d2.csv   
> d1.csv  
--- 
> 导出root.ln下所有设备，导出文件到d:/all/devices，文件策略选择第一种，需要生成时间序列结构，文件格式csv
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\all\\devices -i root.ln.** -sy false -se true -c csv -w "time > 1651036025230"
````
> 导出结果：  
> TIMESERIES_STRUCTURE.STRUCTURE 记录时间序列结构   
> d2.csv   
> d1.csv  
> team1.csv
--- 
> 导入数据，指定导出数据的文件夹d:/all/devices，需要导入时间序列，文件格式csv
````
backup-import.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\all\\devices -se true -c csv
````
--- 
ps: 导出 tsfile
````
backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\tsfileexport\\A106 -i  root.yyy.** -se false -c tsfile -vn 3 -pi 604800
````
如果你想导入tsfile文件,请使用 [Load External TsFile Tool工具](https://iotdb.apache.org/zh/UserGuide/V0.13.x/Write-And-Delete-Data/Load-External-Tsfile.html)。