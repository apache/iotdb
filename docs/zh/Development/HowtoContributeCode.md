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

# 技术贡献指南
## 社区贡献流程概览
Apache IoTDB 社区通过 JIRA 上的 issue 进行任务管理。
Issue 的完整生命周期：创建 issue -> 认领 issue -> 提交 pr -> 审阅 pr -> 合并 pr -> 关闭 issue。

## 技术贡献规范

 ### 创建 issue
 在 [ JIRA ](https://issues.apache.org/JIRA/projects/IOTDB/issues)上创建 issue 需要注意几个事项: 
 1. 命名：争取采用清晰易懂的名字，如支持一种新的聚合查询功能（avg）、优化原始数据查询性能等。Issue 的名字之后会作为发版的 release note。

 2. 描述：新功能、优化需要描述具体希望做什么。 Bug 反馈需要描述环境、负载、现象描述（异常日志）、影响版本等，最好有复现方法。

 ### 认领 issue
 在 JIRA 上认领 issue：分配给自己。建议添加一句评论：I'm doing this。避免与其他贡献者重复开发。
 <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E8%AE%A4%E9%A2%86issue.png?raw=true">注：如果发现自己无法认领issue，则是因为自己的账户权限不够。
遇到此情况，请向 dev@iotdb.apache.org 邮件列表发送邮件，标题为： [application] apply for permission to assign issues to XXX， 其中XXX是自己的JIRA用户名。
### 提交 PR
#### 需提交的内容
Issue 类型：New Feature

1.提交中英文版本的用户手册和代码修改的 pr。

用户手册主要描述功能定义和使用方式，以便用户使用。用户手册建议包括：场景描述，配置方法，接口功能描述，使用示例。官网的用户手册内容放置在 apache/iotdb 仓库根目录下，英文版放在 docs/UserGuide ，中文版放在 docs/zh/UserGuide 。
如果需要更新用户手册，包括新增或删除文档和修改文档名，需要在 master 分支的site/src/main/.vuepress/config.js 中做相应修改。

2.提交单元测试UT或集成测试IT

需要增加单元测试UT 或集成测试IT，尽量覆盖多的用例。可以参考 xxTest（路径：iotdb/server/src/test/java/org/apache/iotdb/db/query/aggregation/）， xxIT（路径：iotdb/integration/src/test/java/org/apache/iotdb/db/integration/）。

Issue 类型：Improvement

提交代码和 UT，一般不需要修改用户手册。
最好提交相关实验结果，其中包含量化的改进效果和带来的副作用。

Issue 类型：Bug

需要编写能够复现此 bug 的 单元测试 UT 或集成测试 IT。

#### 代码管理
分支管理：

IoTDB 版本命名方式为：0.大版本.小版本。例如 0.12.4，12 就是大版本，4 是小版本。

master 分支作为当前主开发分支，对应下一个未发布的大版本，每个大版本发布时会切出一个单独的分支归档，如 0.12.x 系列版本的代码处于 rel/0.12 分支下。

后续如果发现并修复了某发布版本的 bug。对这些 bug 的修复都需要往大于等于该版本对应的归档分支提 pr。如一个关于 0.11.x 版本 bug 修复的 pr 需要同时向 rel/0.11、rel/0.12 和 master 分支提交。

代码格式化:
提交 PR 前需要使用 mvn spotless:apply 将代码格式化，再 commit，不然会导致 ci 代码格式化检查失败。

注意事项:

iotdb-datanode.properties 和 IoTDBConfig 默认值需要保持一致。

如果需要对配置参数进行改动。以下文件需要同时修改：
  1. 配置文件：server/src/assembly/resources/conf/iotdb-datanode.properties
  2. 代码：IoTDBDescriptor、IoTDBConfig
  3. 文档：docs/UserGuide/Reference/DataNode-Config-Manual.md、docs/zh/UserGuide/Reference/DataNode-Config-Manual.md

如果你想要在 IT 和 UT 文件中对配置参数进行修改，你需要在 @Before 修饰的方法里修改，并且在 @After 修饰的方法里重置，来避免对其他测试的影响。合并模块的参数统一放在CompactionConfigRestorer 文件里。

#### PR 命名
命名方式：分支标签-Jira 标签-PR 名

示例： [To rel/0.12] [IoTDB-1907] implement customized sync process: sender

分支标签

如果是向非 master 分支提 pr，如 rel/0.13 分支，需要在 pr 名写上 [To rel/0.13]。如果是指向master分支，则不需要写分支标签。

Jira 标签

以 JIRA 号开头，如：[IOTDB-1907] implement customized sync process: sender。这样创建 PR 后，机器人会将 PR 链接自动链到对应 issue 上。

如果创建 PR 时忘记添加 JIRA 号，或 JIRA 号不规范，则 PR 不会被自动链接到 Jira 上，需要先改正 PR 命名，并手动将 PR 链接贴到 issue 页面（通过留言或链接框）。

#### PR 描述
通常 PR 名无法涵盖所有改动，需要添加具体描述，改动了哪些内容。对于较难理解的地方给予一定的解释。

修 bug 的 pr 需要描述 bug 出现的原因，以及解决方法，另外还需要描述UT/IT测试用例添加的情况和负面效果的描述。

#### 提交 PR 后

向邮件列表 dev@iotdb.apache.org 发送一封邮件，主要介绍 PR 的工作。重视每个审阅者的意见，一一回复，并对达成一致的建议进行修改。

### 审阅PR
主要关注以下事项：
1. PR命名是否规范，新功能和bug修复类型的pr是否带了JIRA 号。
2. PR 描述是否清晰。
3. 功能测试用例或性能测试报告是否附上。
4. 新功能是否有用户手册。
5. 尽量不夹带其他问题的代码修改，将不相关的修改拆分到其他PR。

代码审阅流程：

1. 点击 PR 的 Files changed 
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B1.png?raw=true">
2. 对于有问题的行，移动到左侧，会出现加号，点击加号，然后评论，点击 Start a review，此时，所有的 Review 意见都会暂存起来，别人看不到。<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B2.png?raw=true">
3. 所有评论加完后，需要点击 Review changes，选择你的意见，已经可以合并的选择 Approve，有 Bug 需要改的选择 Request changes 或者 Comment，不确定的选择 Comment。最后 Submit review 提交审阅意见，提 PR 的人才能看见此意见。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B3.png?raw=tru">
### 合并PR
确认所有审阅意见均已回复。

有1个以上 committer 的Approval。

选择 squash merge （当且仅当作者仅有一个提交记录，且记录的commitlog清晰，可选择rebase）。

到 JIRA 上关闭对应的 issue，标记修复或完成的版本【注意，解决或关闭 issue 都需要对 issue 添加 pr 或描述，通过 issue 要能够追踪这个任务的变动】。

## 如何写用户手册和设计文档
官网的用户手册和其他文档都在 apache/iotdb 仓库中进行维护。

官网各个页面的索引项是在 master 分支的 site/src/main/.vuepress/config.js 文件维护的，用户手册的具体内容是在各个版本的分支维护的，如 0.12 版本的用户手册文档在 rel/0.12 中。

用户手册

主要描述功能定义和使用方式，以便用户使用。
用户手册建议包括：场景描述，配置参数，接口功能描述，使用示例。用户手册建议包括：场景描述，配置参数，接口功能描述，使用示例。

设计文档

主要描述如何实现某个功能，包括代码模块组织及算法，以便开发者参与。
设计文档建议包括如下内容：背景描述，设计目标，总体思路，主要模块，接口设计

### 如何修改用户手册
贡献用户手册和贡献代码的流程是一样的，只是修改的文件不同。
用户手册的英文版放在 docs/UserGuide ,  中文版放在 docs/zh/UserGuide 下。
如果需要更新 {master} 或 {rel/*} 分支的用户手册目录，包括新增或删除md文档、修改md文档名，需要在 master 分支的site/src/main/.vuepress/config.js 中做相应修改。

### 如何修改官网顶部导航栏

在 site/src/main/.vuepress/config.js 中搜索 nav （应有中英文两个地方），仿照现有的代码做相应修改。之后提交PR等待合并。需要新增的文档可以放进 docs 和 docs/zh 文件夹下。