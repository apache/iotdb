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


## Workbench简介

Workbench是一个可创建多个连接的图形化数据库管理工具，用于管理IoTDB，提供元数据的可视化与管理、数据的增删改查以及权限控制等功能。Workbench不仅满足专业开发人员的所有需求，同时简洁友好的界面设计对初学者来说也易于上手。

## 安装

如果你在安装过程中遇到任何问题，你可以参考文档或向软件供应商或技术支持团队寻求帮助。

环境要求：JDK1.8.0_162及以上。

1. 下载并解压软件。第一步是从官方网站或一个可信赖的来源下载软件，下载地址为https://www.timecho.com/product 。
2. 启动后端服务。输入指令：
```
java -jar workbench.jar
```
或：
```
nohup java -jar workbench.jar  >/dev/null 2>&1 &
```
默认端口为 9090；

1. 访问web界面。默认地址为`IP：9090`。

## 登录

默认用户名为root,密码为123456。用户名必须由字母、数字、下划线组成，不能以数字和下划线开始，须大于等于4个字符，密码必须大于等于6位。点击"**文A**"可切换语言，有中文、英文可选。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image6.jpeg)

## 用户界面

**主界面**

主界面由操作栏、导航栏、工具栏和几个窗格组成。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image7.png)

1、连接操作栏

可以新建数据库的连接或者数据库的查询。

2、对象窗格

对象窗格显示已连接的数据库实例，采用树状结构设计，点击出现子节点可以方便地处理数据库和它们管理的对象，展示的最低层级是设备。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image8.jpeg)

3、导航栏

导航栏可选择"**数据库管理**"、"**监控管理**"、"**操作日志**"、"**数据看板**"。

4、状态栏

状态栏显示当前选项下的状态信息，当选中"**数据库管理**"时，状态栏显示数据库的在线情况、IP、端口、服务器状态及其存储组、设备、物理量的数量信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image9.jpeg)

当选\"**监控管理**\"时，状态栏显示数据库的在线情况、IP、端口、服务器状态、数据库版本、激活信息及到期时间。z注："**数据库版本**"处的图标表示企业版或开源版，Workbench部分功能在开源版上无法使用。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image10.jpeg)

5、操作列

操作列可以选择任意选项进行操作。

6、信息窗格

信息窗格显示对象的详细信息。

## 连接

首先用连接窗口创建一个或多个连接。点击"**数据连接**"创建新建连接。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image11.jpeg)

在弹出的连接窗口输入需要填写的信息，其中数据连接名称必须大于等于3个字符。然后点击"**连接测试**"，显示"**连接测试通过**"则表示正确连接，点击确定即可新建连接。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image12.png)

若要修改连接情况，则可在状态栏右侧点"**编辑**"选项即可修改数据库连接信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image13.jpeg)

## 服务器安全性

**白名单**

企业版IoTDB可以通过添加白名单列表来设置允许访问IoTDB的IP。使用开源版IoTDB连接Workbench无法使用该功能。

从导航栏选择"**数据库管理**"-\>"**白名单**"可查看已添加的白名单IP列表。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image14.jpeg)

点击"**白名单**"可新增白名单IP，点击"**编辑**"/"**删除**"可修改白名单列表的IP信息。

**权限管理**

Workbench提供强大的工具以管理服务器用户帐号和数据库对象的权限。在操作列中点击
"**数据库用户管理**"或 "**数据库角色管理**"来打开用户或角色的对象列表。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image15.jpeg)

**新增用户**。选择"**数据库用户管理**"-\>"**用户账号＋**"可新增用户，按要求填写用户名和密码即可，可以为用户添加角色信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image16.jpeg)

**添加权限**。权限可分为数据管理权限（如对数据进行增删改查）以及权限管理权限（用户、角色的创建与删除，权限的赋予与撤销等）。选择"**数据库用户管理**"-\>"**数据管理权限**"-\>"**添加权限**"可为用户添加数据管理权限。在已添加的权限处可以选择"**编辑**"或"**删除**"以修改权限信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image17.jpeg)

弹出的界面可以选择权限作用的粒度以及具体权限内容。注意只有勾选【查询数据】权限和【查看用户】权限，其他权限才能在Workbench中生效查看。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image18.jpeg)

选择"**数据库用户管理**"-\>"**权限管理权限**"勾选信息窗格中具体的权限信息，点击"保存"可为用户添加权限管理权限。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image19.jpeg)

**权限预览**。选择"**数据库用户管理**"-\>"**数据权限预览**"可预览该用户名下所有的数据权限。注：该功能仅支持企业版。

## 数据迁移

导入导出工具可将CSV格式的文件批量导入或导出IoTDB。

**批量导入**

批量导入功能仅在企业版中支持。在对象窗格中选择要操作的数据库，选择到设备节点，则右侧信息窗格将出现"**设备结构"**信息，点击"**导入物理量**"，下载模板填写物理量信息，再上传该CSV文件即可批量导入物理量。注：当前版本不支持导入对齐物理量。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image20.png)

选择"**数据预览**"-\>"**批量导入**"则可将符合模板要求的CSV文件数据导入，当前支持导入对齐时间序列。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image21.jpeg)

**批量导出**

批量导出功能仅在企业版中支持。在对象窗格中选择要操作的数据库，进入设备节点，选择"**设备结构**"-\>"**导出物理量**"即可批量导出该实体下的物理量元数据。搜索框内可输入名称/别名、标签名称、标签值进行过滤。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image22.jpeg)

选择"**数据预览**"-\>"**导出数据**"则可批量导出该实体下的数据。搜索框内可按时间范围、时间间隔和物理量进行过滤。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image23.jpeg)

## 数据预览

Workbench提供多层次的数据预览工具。

**数据模型**

在对象窗格中选择要预览的数据连接，点击操作列的"**数据模型**"即可预览数据模型，root被定义为LEVEL=0，Workbench中默认显示到LEVEL=1，点击"**查看更多**"可查看更多层级的数据模型信息。"**查看更多**"功能仅在企业版中支持。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image24.png)

**存储组信息**

点击操作列的"**存储组信息**"即可预览该数据连接的所有存储组信息，点击"**详情**"可查看该存储组下的实体详情，继续点击实体详情可查看物理量详情。点击"**编辑**"可编辑该存储组的TTL信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image25.jpeg)

## 查询

Workbench提供强大的查询工具，可直接编辑查询文本，保存查询，用于简化查询行任务。

**新建查询**

点击"**连接操作栏**"的"**查询**"，选择要进行操作的数据连接,即可进入查询编辑器。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image26.jpeg)

在查询编辑器界面可输入SQL语句，提示框会提示符合条件的关键字。右侧可按要求选择函数或数据进行计算。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image27.png){width="5.90625in" height="3.125in"}

输入SQL语句后，点击右上角可选择操作，分别是"保存"、"运行"、"暂停"和"删除"。运行结果显示10行每页，默认限制返回结果100行，也可选择取消限制全部展示。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image28.jpeg)

**查看查询**

已保存的连接可在对象窗格中该数据连接下的"**查询**"下查看。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image29.jpeg)

## 集群管理

**拓扑管理**

Workbench提供对集群拓扑图的查看。选择"**数据库管理**"-\>"**节点管理**"\>"**拓扑管理**"可以查看拓扑图。"拓扑管理"功能仅在企业版中支持。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image30.png)

拓扑图显示节点IP、节点类型及端口。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image31.jpeg)

**节点管理**

Workbench提供对集群节点的管理。选择"**数据库管理**"-\>"**节点管理**"可以查看节点状态。可按节点ID或节点类型进行查询。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image32.jpeg)

**分区管理**

Workbench提供对集群分区的管理。选择"**数据库管理**"-\>"**分区管理**"可以查看分区状态。可按分区ID或分区类型进行查询。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image33.png)

## 存储组管理

**新增存储组**

在对象窗格中选择要操作的数据库对象，点击"**新建存储组**"即可创建存储组，存储组名称为必填，存储组名称为必填，正常情况下只能输入字母、数字、下划线以及UNICODE
中文字符如果包含特殊字符，请使用反引号。存活时间选填。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image34.png)

**删除存储组**

在对象窗格中选择要操作的存储组，在操作列中选择"**编辑**"可修改存储组存活时间，选择"**删除**"可删除存储组。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image35.png)

## 设备及物理量管理

**新建物理量**

在对象窗格中选择要操作的存储组，点击"**新建设备**"，按要求填写信息则可创建该存储组下的物理量。物理量名称名称为必填，正常情况下只能输入字母、数字、下划线以及UNICODE
中文字符如果包含特殊字符，请使用反引号。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image36.png)

**数据趋势预览**

"数据趋势"功能仅在企业版中支持。在对象窗格中选定存储组，"**数据趋势**"显示该存储组下的物理量趋势图表，单击图表显示详细信息，可选定时间范围查询该区间内的数据，并显示最小值等数据

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image37.png)

**修改、删除物理量**

在对象窗格中选择要操作的设备
，点击右侧的"**编辑**"可修改该设备下的物理量信息（别名、标签和属性）。点击"**删除**"则可删除该设备。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image38.png)

## 操作日志

"操作日志"提供所有在Workbench上进行的操作记录，可按IP、用户、数据连接、关键词和时间范围进行筛选。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image39.jpeg)

## 监控管理

Workbench提供"**监控管理**"提供来查看已选择的服务器监控属性。从导航栏选择"**监控管理**"并选择你想要的监控类型。可选"**监控指标**"，"**连接信息**"，"**审计日志**"。

**监控指标**

监控指标可供查看CPU指标、内存指标和存储指标的最新信息。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image40.png)

**连接信息**

连接信息可供查看连接到Workbench的用户和服务器信息。"连接信息"功能仅在企业版中支持。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image41.png)

**审计日志**

审计日志显示所有在IoTDB执行的操作，Workbench提供查询接口，可按时间段或用户名进行查询。"审计日志"功能仅在企业版中支持。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image42.png)

## 数据看板

数据看板可供创建数据库数据的直观表示，下图为挂载Grafana模板的仪表盘。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image43.png)

**配置**

**文件配置**

在文件application-prod.properties中配置看板地址，找到配置项url.dashboard=https://grafana.com/，填写grafana
URL。

application-prod.properties源文件内容：
  ```plain Text
# Designate the log configuration file
logging.config=classpath:log4j2.xml

# Set port and context path
server.port=9090
server.servlet.context-path=/api

# The following data source configuration method will cause data loss after the project is repackaged.
# To facilitate testing during development, refer to the application-prod.properties file for configuration during actual project deployment
# sqlite
spring.datasource.url=jdbc:sqlite:./iotdb.db
spring.datasource.driver-class-name=org.sqlite.JDBC
# mysql
#spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
#spring.datasource.url=jdbc:mysql://
#spring.datasource.username=
#spring.datasource.password=

# Enable the multipart uploading function
spring.servlet.multipart.enabled=true
spring.servlet.multipart.file-size-threshold=2KB
spring.servlet.multipart.max-file-size=200MB
spring.servlet.multipart.max-request-size=215MB

# All files generated during CSV import and export are stored in this folder
file.temp-dir=./tempFile

spring.messages.basename=messages

# enable open audit in iotdb
enableIotdbAudit = false
# enable open audit in workbench:
enableWorkbenchAudit = true
# timechodb  config server rpc port
configServerPort=8867
# dashboard url
url.dashboard=https://grafana.com/
  ```

**URL获取**

登录Grafan面板，点击分享按钮，在弹出的窗口选择"**Link**"，复制"**Link
URL**"即可。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image44.png)

## Q&A

1.  如果遇到以下情况，请尝试退出"无痕模式"或者更换浏览器。

![](https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Workbench/image45.png)

2.  如果看不到监控信息，需要开启IoTDB的Metric。

3.  双活配置发生变化时需建议重新建立连接。
