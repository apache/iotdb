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

## 应用场景

 * 场景 1

某公司采用表面贴装技术（SMT）生产芯片：需要首先在芯片上的焊接点处印刷（即涂抹）锡膏，然后将元器件放置在锡膏上，进而通过加热熔化锡膏并冷却，使得元器件被焊接在芯片上。上述流程采用自动化生产线。为了确保产品质量合格，在印刷锡膏后，需要通过光学设备对锡膏印刷的质量进行评估：采用三维锡膏印刷检测（SPI）设备对每个焊接点上的锡膏的体积（v）、高度（h）、面积（a）、水平偏移（px）、竖直偏移（py）进行度量。

为了提升印刷质量，该公司有必要将各个芯片上焊接点的度量值进行存储，以便后续基于这些数据进行分析。

此时可以采用 IoTDB 套件中的 TsFile 组件、TsFileSync 工具和 Hadoop/Spark 集成组件对数据进行存储：每新印刷一个芯片，就在 SPI 设备上使用 SDK 写一条数据，这些数据最终形成一个 TsFile 文件。通过 TsFileSync 工具，生成的 TsFile 文件将按一定规则（如每天）被同步到 Hadoop 数据中心，并由数据分析人员对其进行分析。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51579014-695ef980-1efa-11e9-8cbc-e9e7ee4fa0d8.png">

在场景 1 中，仅需要 TsFile、TsFileSync 部署在一台 PC 上，此外还需要部署 Hadoop/Spark 连接器用于数据中心端 Hadoop/Spark 集群的数据存储和分析。其示意图如上图所示。下图展示了此时的应用架构。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/81768490-bf034f00-950d-11ea-9b56-fef3edca0958.png">

 * 场景 2

某公司拥有多座风力发电机，公司在每个发电机上安装了上百种传感器，分别采集该发电机的工作状态、工作环境中的风速等信息。

为了保证发电机的正常运转并对发电机及时监控和分析，公司需要收集这些传感器信息，在发电机工作环境中进行部分计算和分析，还需要将收集的原始信息上传到数据中心。

此时可以采用 IoTDB 套件中的 IoTDB、TsFileSync 工具和 Hadoop/Spark 集成组件等。需要部署一个场控 PC 机，其上安装 IoTDB 和 TsFileSync 工具，用于支持读写数据、本地计算和分析以及上传数据到数据中心。此外还需要部署 Hadoop/Spark 连接器用于数据中心端 Hadoop/Spark 集群的数据存储和分析。如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51579033-7ed42380-1efa-11e9-889f-fb4180291a9e.png">

下图给出了此时的应用架构。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51579064-8f849980-1efa-11e9-8cd6-a7339cd0540f.jpg">

 * 场景 3

某工厂在厂区范围内拥有多种机械手设备，这些机械手设备的硬件配置有限，很难搭载复杂的应用程序。在每个机械手设备上工厂安装了很多种传感器，用以对机械手的工作状态、温度等信息进行监控。由于工厂的网络环境原因，在工厂内部的机械手均处于工厂内部局域网内，无法连接外部网络。同时，工厂中会有少量服务器能够直接连接外部公网。

为了保证机械手的监控数据能够及时监控和分析，公司需要收集这些机械手传感器信息，将其发送至可以连接外部网络的服务器上，而后将原始数据信息上传到数据中心进行复杂的计算和分析。

此时，可以采用 IoTDB 套件中的 IoTDB、IoTDB-Client 工具、TsFileSync 工具和 Hadoop/Spark 集成组件等。将 IoTDB 服务器安装在工厂连接外网的服务器上，用户接收机械手传输的数据并将数据上传到数据中心。将 IoTDB-Client 工具安装在每一个连接工厂内网的机械手上，用于将传感器产生的实时数据上传到工厂内部服务器。再使用 TsFileSync 工具将原始数据上传到数据中心。此外还需要部署 Hadoop/Spark 连接器用于数据中心端 Hadoop/Spark 集群的数据存储和分析。如下图中间场景所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51579080-96aba780-1efa-11e9-87ac-940c45b19dd7.jpg">

下图给出了此时的应用架构。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/81768477-b874d780-950d-11ea-80ca-8807b9bd0970.png">

 * 场景 4

某汽车公司在其下属的汽车上均安装了传感器采集车辆的行驶状态等监控信息。这些汽车设备的硬件配置有限，很难搭载复杂的应用程序。安装传感器的汽车可以通过窄带物联网相互连接，也可以通过窄带物联网将数据发送至外部网络。

为了能够实时接收汽车传感器所采集的物联网数据，公司需要在车辆行驶的过程中将传感器数据通过窄带物联网实时发送至数据中心，而后在数据中心的服务器上进行复杂的计算和分析。

此时，可以采用 IoTDB 套件中的 IoTDB、IoTDB-Client 和 Hadoop/Spark 集成组件等。将 IoTDB-Client 工具安装在每一辆车联网内的车辆上，使用 IoTDB-JDBC 工具将数据直接传回数据中心的服务器。 

此外还需要部署 Hadoop/Spark 集群用于数据中心端的数据存储和分析。如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51579095-a4f9c380-1efa-11e9-9f95-17165ec55568.jpg">
