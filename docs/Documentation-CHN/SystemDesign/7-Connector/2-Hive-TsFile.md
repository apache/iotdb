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

## TsFile的Hive连接器

TsFile的Hive连接器实现了对Hive读取外部Tsfile类型的文件格式的支持，
使用户能够通过Hive操作Tsfile。

有了这个连接器，用户可以
* 将单个Tsfile文件加载进Hive，不论文件是存储在本地文件系统或者是HDFS中
* 将某个特定目录下的所有文件加载进Hive，不论文件是存储在本地文件系统或者是HDFS中
* 使用HQL查询tsfile
* 到现在为止, 写操作在hive-connector中还没有被支持. 所以, HQL中的insert操作是不被允许的

### 设计原理
