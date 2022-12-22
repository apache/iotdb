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

# 工作流程

## 主要链接

IoTDB 官网：https://iotdb.apache.org/

代码库：https://github.com/apache/iotdb/tree/master

Go语言的代码库：https://github.com/apache/iotdb-client-go

资源库（包含项目文件等）：https://github.com/apache/iotdb-bin-resources

快速上手：http://iotdb.apache.org/UserGuide/master/Get%20Started/QuickStart.html

Jira 任务管理：https://issues.apache.org/jira/projects/IOTDB/issues

Wiki 文档管理：https://cwiki.apache.org/confluence/display/IOTDB/Home

邮件列表: https://lists.apache.org/list.html?dev@iotdb.apache.org

每日构建: https://ci-builds.apache.org/job/IoTDB/job/IoTDB-Pipe/job/master/

Slack: https://apacheiotdb.slack.com/join/shared_invite/zt-qvso1nj8-7715TpySZtZqmyG5qXQwpg#/shared-invite/email

## 订阅邮件列表

邮件列表是 Apache 项目进行技术讨论和用户沟通的地方，订阅邮件列表就可以收到邮件了。

邮件列表地址：dev@iotdb.apache.org

订阅方法：用想接收邮件的邮箱向 dev-subscribe@iotdb.apache.org 发一封邮件，主题内容不限，收到回复后，再次向确认地址发一封确认邮件（确认地址比较长，推荐 qq 邮箱）。

其他邮件列表：
* notifications@iotdb.apache.org （用于接收 JIRA 通知。)
  * 如果你只想接收个别感兴趣的 JIRA 通知，你不需要订阅这个列表。你只需要在 JIRA issue 页面上点击"开始关注这个 issue" 或者在这个 issue 上做评论就行了。
* commits@iotdb.apache.org （任何代码改动都会通知到此处。该邮件列表邮件数量十分多，请注意。)
* reviews@iotdb.apache.org （任何代码审阅意见都会通知到此处。该邮件列表邮件数量十分多，请注意。)

## 新功能、Bug 反馈、改进等

所有希望 IoTDB 做的功能或修的 bug，都可以在 Jira 上提 issue

可以选择 issue 类型：bug、improvement、new feature 等。新建的 issue 会自动向邮件列表中同步邮件，之后的讨论可在 jira 上留言，也可以在邮件列表进行。当问题解决后请关闭 issue。

## 邮件讨论内容（英文）

* 第一次参与邮件列表可以简单介绍一下自己。（Hi, I'm xxx ...)

* 开发功能前可以发邮件声明一下自己想做的任务。（Hi，I'm working on issue IOTDB-XXX，My plan is ...）

## 贡献文档

IoTDB 所有官网上的内容都在项目根目录的 docs 中：

* docs/SystemDesign: 系统设计文档-英文版
* docs/zh/SystemDesign: 系统设计文档-中文版
* docs/UserGuide: 用户手册-英文版
* docs/zh/UserGuide: 用户手册-中文版
* docs/Community: 社区-英文版
* docs/zh/Community: 社区-中文版
* docs/Development: 开发指南-英文版
* docs/zh/Development: 开发指南-中文版

官网上的版本和分支的对应关系：

* In progress -> master
* major_version.x -> rel/major_version （如 0.9.x -> rel/0.9）

注意事项：

* Markdown 中的图片可上传至 https://github.com/apache/iotdb-bin-resources 获得 url

## 代码格式化

我们使用 [Spotless
plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) 和 [google-java-format](https://github.com/google/google-java-format) 格式化 Java 代码。你可以通过以下步骤将 IDE 配置为在保存时自动应用格式以 IDEA 为例）：

1. 下载 [google-java-format-plugin v1.7.0.5](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/83169), 安装到 IDEA(Preferences -> plugins -> search google-java-format),[更详细的操作手册](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides)
2. 从磁盘安装 (Plugins -> little gear icon -> "Install plugin from disk" -> Navigate to downloaded zip file)
3. 开启插件，并保持默认的 GOOGLE 格式 (2-space indents)
4. 在 Spotless 没有升级到 18+之前，不要升级 google-java-format 插件
5. 安装 [Save Actions 插件](https://plugins.jetbrains.com/plugin/7642-save-actions) , 并开启插件，打开 "Optimize imports" and "Reformat file" 选项。
6. 在“Save Actions”设置页面中，将 "File Path Inclusion" 设置为 .*\.java”, 避免在编辑的其他文件时候发生意外的重新格式化

## 编码风格
我们使用 [maven-checkstyle-plugin](https://checkstyle.sourceforge.io/config_filefilters.html) 来保证所有的 Java 代码风格都遵循在项目根目录下的 [checkstyle.xml](https://github.com/apache/iotdb/blob/master/checkstyle.xml) 文件中定义的规则集.

您可以从该文件中查阅到所有的代码风格要求。当开发完成后，您可以使用 `mvn validate` 命令来检查您的代码是否符合代码风格的要求。

另外, 当您在集成开发环境开发时，可能会因为环境的默认代码风格配置导致和本项目的风格规则冲突。

在 IDEA 中，您可以通过如下步骤解决风格规则不一致的问题。

### 禁用通配符引用

1. 跳转至 Java 代码风格配置页面 (Preferences... -> 编辑器 -> 代码风格 -> Java)。
2. 切换到“导入”标签。
3. 在“常规”部分，启用“使用单个类导入”选项。
4. 将“将 import 与‘*’搭配使用的类计数”改成 999 或者一个比较大的值。
5. 将“将静态 import 与‘*’搭配使用的名称计数”改成 999 或者一个比较大的值。

## 贡献代码

可以到 jira 上领取现有 issue 或者自己创建 issue 再领取，评论说我要做这个 issue 就可以。

* 克隆仓库到自己的本地的仓库，clone 到本地，关联 apache 仓库为上游 upstream 仓库。
* 从 master 切出新的分支，分支名根据这个分支的功能决定，一般叫 f_new_feature（如 f_storage_engine) 或者 fix_bug（如 fix_query_cache_bug)
* 在 idea 中添加 code style 为 根目录的 java-google-style.xml
* 修改代码，增加测试用例（单元测试、集成测试）
	* 集成测试参考：server/src/test/java/org/apache/iotdb/db/integration/IoTDBTimeZoneIT
* 用 `mvn spotless:check` 检查代码样式，并用`mvn spotless:apply`修复样式
* 提交 PR, 以 [IOTDB-jira 号] 开头
* 发邮件到 dev 邮件列表：(I've submitted a PR for issue IOTDB-xxx [link])
* 根据其他人的审阅意见进行修改，继续更新，直到合并
* 关闭 jira issue

# IoTDB 调试方式

## 导入代码

### Intellij idea

推荐使用 Intellij idea。```mvn clean package -DskipTests``` 

之后把 ```antlr/target/generated-sources/antlr4``` 和 ```thrift/target/generated-sources/thrift``` 标记为 ```Source Root```。 

### Eclipse

如果是 eclipse 2019 之前的版本，需要现在 IoTDB 根目录执行 `mvn eclipse:eclipse -DskipTests`。

import -> General -> Existing Projects into Workspace -> 选择 IoTDB 根目录

如果 eclipse 2019 之后的版本

import -> Maven -> Existing Maven Projects


# 常见编译错误

## 无法下载 thrift-* 等文件
例如 `Could not get content
org.apache.maven.wagon.TransferFailedException: Transfer failed for https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-ubuntu`
这一般是网络问题，这时候需要手动下载上述文件：

  * 根据以下网址手动下载上述文件；
      * https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-MacOS
      * https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-ubuntu
  
 * 将该文件拷贝到 thrift/target/tools/目录下 
 * 重新执行 maven 的编译命令


## 无法下载errorprone ：
```Failed to read artifact descriptor for com.google.errorprone:javac
-shaded:jar:9+181-r4173-1: Could not transfer artifact com.google.errorprone:javac-shaded:pom:9+181-r4173-1
```
1. 手动下载jar包：   https://repo1.maven.org/maven2/com/google/errorprone/javac-shaded/9+181-r4173-1/javac-shaded-9+181-r4173-1.jar
2. 将jar包安装到本地私仓库 ：   mvn install:install-file -DgroupId=com.google.errorprone -DartifactId=javac-shaded -Dversion=9+181-r4173-1 -Dpackaging=jar -Dfile=D:\workspace\iotdb-master\docs\javac-shaded-9+181-r4173-1.jar
