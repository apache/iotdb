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

# 写前日志

## 工作流程

* WAL 总体记录原理
  * 对于每一个 Memtable，都会记录一个 WAL 文件，当 Memtable 被 flush 完成时，WAL 会被删掉。
* WAL 记录细节
  * 在 org.apache.iotdb.db.writelog.manager 中，会不断在 nodeMap 中积累 WAL
  * WAL 刷磁盘有两种方式（同时启用）
    * 在 org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode 中会根据配置中的 wal_buffer_size 分配 WAL 的 buffer 大小，如在新增 WAL 过程中超过了该 buffer 大小则刷到磁盘中
    * 在 org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode 中每次写入记录会判断当前 node 积累的 WAL 大小是否超过配置中的 flush_wal_threshold，如超过则刷到磁盘中
    * 在 org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager 启动时会生成一个定时线程，根据 force_wal_period_in_ms 定时调用线程将内存中的 nodeMap 刷到磁盘中，调用示例如下
      * 持久化(forceTask)-sleep({force_wal_period_in_ms})-持久化(forceTask)-sleep({force_wal_period_in_ms})

## 测试结果

* 整个 forceTask 主要耗时都集中在 org.apache.iotdb.db.writelog.io.LogWriter.force()，且因磁盘属性不同差别巨大
* 分别对 SSD 和 HDD 进行 forceTask 的测试
  * 测试负载为1sg,1device,100sensor,每个sensor写100W个点，force_wal_period_in_ms=10
  * 在 SSD 中，每秒可以刷大约 75MB 的数据到磁盘中
  * 在 HDD 中，每秒可以刷大约 5MB 的数据到磁盘中
  * 所以在 HDD 环境中，用户必须注意调节 force_wal_period_in_ms 不会太小，否则会严重影响写入性能
    * 经过测试，在 HDD 中较优的参数配置为 100ms-200ms，测试结果图如下
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93157479-e3319f80-f73c-11ea-836f-459d03cb2fab.png">

## 相关代码

* org.apache.iotdb.db.writelog.*
