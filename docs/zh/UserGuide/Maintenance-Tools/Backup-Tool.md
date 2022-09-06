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
你可以使用以下命令来编译或者打包这个项目:  
````
mvn clean package;  
mvn install;  
mvn test;   
mvn verify;   
````
ps: 使用 '-DskipTests' 来跳过测试用例    
由于core-jar还没有放到远程仓库，为了顺利编译工具，首先需要执行mvn install -DskipTests。  
集成测试需要一个真正的iotdbserver，你可以在“test/resource/sesseinConfig.properties”中配置服务器.  

## 特性    
- 基于iotdb-session   
- 基于pipeline的导入导出工具  
- 基于react框架  
- 支持很多情形的导入导出,比如导入导出一个存储组,导入导出对应的时间序列结构,包含设备是否对齐的信息等等  

## 使用
````
-  backup-export.bat -h 127.0.0.1 -p 6667 -u root -pw root -f d:\\validate_test\\83 -i root.** -sy true -se true -c gzip  
-  backup-import.1.bat -h 127.0.0.1 -p 6667 -u root -pw root -f D:\validate_test\83 -se true -c gzip
- -h  // iotdb host address
- -p  // port
- -u  // username
- -pw // password
- -f  // fileFolder
- -sy // 是否需要时间序列结构,如果是true的话，工具会额外导出一个文件来记录它对应的时间序列结构
- -se // 文件生成策略，1.以entity的路径为为文件名生成文件 2、额外提供一个文件来记录文件名与entity路径的对应关系
- -c  // 压缩格式 : SQL、CSV、SNAPPY、GZIP、LZ4
- -w  // where  条件
- -i  // 要导出的路径
````