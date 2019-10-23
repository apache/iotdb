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

# 第5章 系统管理

## 系统日志

IoTDB支持用户通过修改日志配置文件的方式对IoTDB系统日志（如日志输出级别等）进行配置，系统日志配置文件默认位置在$IOTDB_HOME/conf文件夹下，默认的日志配置文件名为logback.xml。用户可以通过增加或更改其中的xml树型节点参数对系统运行日志的相关配置进行修改。需要说明的是，使用日志配置文件对系统日志进行配置并非修改完文件立刻生效，而是重启IoTDB系统后生效。详细配置说明参看本文日志文件配置说明。

同时，为了方便在系统运行过程中运维人员对系统的调试，我们为系统运维人员提供了动态修改日志配置的JMX接口，能够在系统不重启的前提下实时对系统的Log模块进行配置。详细使用方法参看动态系统日志配置说明)。

### 动态系统日志配置说明

#### 连接JMX

本节以Jconsole为例介绍连接JMX并进入动态系统日志配置模块的方法。启动Jconsole控制页面，在新建连接处建立与IoTDB Server的JMX连接（可以选择本地进程或给定IoTDB的IP及PORT进行远程连接，IoTDB的JMX服务默认运行端口为31999），如下图使用远程进程连接Localhost下运行在31999端口的IoTDB JMX服务。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577195-f94d7500-1ef3-11e9-999a-b4f67055d80e.png">

连接到JMX后，您可以通过MBean选项卡找到名为`ch.qos.logback.classic`的`MBean`，如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577204-fe122900-1ef3-11e9-9e89-2eb1d46e24b8.png">

在`ch.qos.logback.classic`的MBean操作（Operations）选项中，可以看到当前动态系统日志配置支持的5种接口，您可以通过使用相应的方法，来执行相应的操作，操作页面如图。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577216-09fdeb00-1ef4-11e9-9005-542ad7d9e9e0.png">

#### 动态系统日志接口说明

* reloadDefaultConfiguration接口

该方法为重新加载默认的logback配置文件，用户可以先对默认的配置文件进行修改，然后调用该方法将修改后的配置文件重新加载到系统中，使其生效。

* reloadByFileName接口

该方法为加载一个指定路径的logback配置文件，并使其生效。该方法接受一个名为p1的String类型的参数，该参数为需要指定加载的配置文件路径。

* getLoggerEffectiveLevel接口

该方法为获取指定Logger当前生效的日志级别。该方法接受一个名为p1的String类型的参数，该参数为指定Logger的名称。该方法返回指定Logger当前生效的日志级别。

* getLoggerLevel接口

该方法为获取指定Logger的日志级别。该方法接受一个名为p1的String类型的参数，该参数为指定Logger的名称。该方法返回指定Logger的日志级别。

需要注意的是，该方法与`getLoggerEffectiveLevel`方法的区别在于，该方法返回的是指定Logger在配置文件中被设定的日志级别，如果用户没有对该Logger进行日志级别的设定，则返回空。按照Logback的日志级别继承机制，如果一个Logger没有被显示地设定日志级别，其将会从其最近的祖先继承日志级别的设定。这时，调用`getLoggerEffectiveLevel`方法将返回该Logger生效的日志级别；而调用本节所述方法，将返回空。
