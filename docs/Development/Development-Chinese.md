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

# 一、工作流程

## 主要链接

IoTDB 官网：https://iotdb.apache.org/

代码库：https://github.com/apache/incubator-iotdb/tree/master

快速上手：https://iotdb.apache.org/#/Documents/Quick%20Start

## 订阅邮件列表

邮件列表是 Apache 项目进行技术讨论和用户沟通的地方，关注邮件列表就可以收到邮件了。

邮件列表地址：dev@iotdb.apache.org

关注方法：用想接收邮件的邮箱向 dev-subscribe@iotdb.apache.org 发一封邮件，主题内容不限，收到回复后，再次向确认地址发一封确认邮件（确认地址比较长，推荐qq邮箱）。

## 新功能、Bug 反馈、改进等

所有希望 IoTDB 做的功能或修的 bug，都可以在 Jira 上提 issue：https://issues.apache.org/jira/projects/IOTDB/issues/IOTDB-9?filter=allopenissues

可以选择 issue 类型：bug、improvement、new feature等。新建的 issue 会自动向邮件列表中同步邮件，之后的讨论可在 jira 上留言，也可以在邮件列表进行。当问题解决后请关闭 issue。

## 邮件讨论内容（英文）

* 第一次参与邮件列表可以简单介绍一下自己。（Hi, I'm xxx ...)

* 开发功能前可以发邮件声明一下自己想做的任务。（Hi，I'm working on issue IOTDB-XXX，My plan is ...）

## 贡献文档

IoTDB 所有官网上的内容都在项目根目录的 docs 中：

* docs/Documentation/SystemDesign: 系统设计文档-英文版
* docs/Documentation-CHN/SystemDesign: 系统设计文档-中文版
* docs/Documentation/UserGuide: 用户手册-英文版
* docs/Documentation-CHN/UserGuide: 用户手册-中文版
* docs/Community: 社区
* docs/Development: 开发指南

官网上的版本和分支的对应关系：

* In progress -> master
* major_version.x -> rel/major_version （如 0.9.x -> rel/0.9）

注意事项：

* Markdown 中的图片可上传至 https://github.com/thulab/iotdb/issues/543 获得 url
* 新增加的系统设计文档和用户手册的 md 文件，需要在英文版对应的根目录下的 0-Content.md 中增加索引

## 贡献代码

可以到 jira 上领取现有 issue 或者自己创建 issue 再领取，评论说我要做这个 issue 就可以。

* 克隆仓库到自己的本地的仓库，clone到本地，关联apache仓库为上游 upstream 仓库。
* 从 master 切出新的分支，分支名根据这个分支的功能决定，一般叫 f_new_feature(如f_storage_engine) 或者 fix_bug(如fix_query_cache_bug)
* 在 idea 中添加code style为 根目录的 java-google-style.xml
* 修改代码，增加测试用例（单元测试、集成测试）
	* 集成测试参考: server/src/test/java/org/apache/iotdb/db/integration/IoTDBTimeZoneIT
* 提交 PR, 以 [IOTDB-jira号] 开头
* 发邮件到 dev 邮件列表：(I've submitted a PR for issue IOTDB-xxx [link])
* 根据其他人的审阅意见进行修改，继续更新，直到合并
* 关闭 jira issue

## 二、IoTDB 调试方式

推荐使用 Intellij idea。```mvn clean package -DskipTests``` 之后把 ```server/target/generated-sources/antlr4``` 和 ```service-rpc/target/generated-sources/thrift``` 标记为 ```Source Root```。 

* 服务器主函数：```server/src/main/java/org/apache/iotdb/db/service/IoTDB```，可以debug模式启动
* 客户端：```client/src/main/java/org/apache/iotdb/client/```，linux 用 Clinet，windows 用 WinClint，可以直接启动，需要参数"-h 127.0.0.1 -p 6667 -u root -pw root"
* 服务器的 rpc 实现（主要用来客户端和服务器通信，一般在这里开始打断点）：```server/src/main/java/org/apache/iotdb/db/service/TSServiceImpl```
	* jdbc所有语句：executeStatement(TSExecuteStatementReq req)
	* jdbc查询语句：executeQueryStatement(TSExecuteStatementReq req)	* native写入接口：insert(TSInsertReq req)

* 存储引擎 org.apache.iotdb.db.engine.StorageEngine
* 查询引擎 org.apache.iotdb.db.qp.QueryProcessor

