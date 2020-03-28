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

<!-- TOC -->

## Outline

- Material: Sample Data
    - Scenario Description
    - Sample Data

<!-- /TOC -->
# Material: Sample Data

### Scenario Description

A power department needs to monitor the operation of various power plants under its jurisdiction. By collecting real-time monitoring data sent by various types of sensors deployed by various power plants, the power department can monitor the real-time operation of the power plants and understand the trend of data changes, etc. IoTDB has the characteristics of high write throughput and rich query functions, which can provide effective support for the needs of the power department.

The real-time data needed to be monitored involves multiple attribute layers:

* **Power Generation Group**: The data belongs to nearly ten power generation groups, and the name codes are ln, sgcc, etc.

* **Power Plant**: The power generation group has more than 10 kinds of electric fields, such as wind farm, hydropower plant and photovoltaic power plant, numbered as wf01, wf02, wf03 and so on.

* **Device**: Each power plant has about 5,000 kinds of power generation devices such as wind turbines and photovoltaic panels, numbered as wt01, wt02 and so on.

* **Sensor**: For different devices, there are 10 to 1000 sensors monitoring different states of the devices , such as power supply status sensor (named status), temperature sensor (named temperature), hardware version sensor (named hardware), etc. 

It is worth noting that prior to the use of IoTDB by the power sector, some historical monitoring data of various power plants needs to be imported into the IoTDB system (we will introduce the import method in [Import Historical Data](需要连接到具体的网页链接Chapter3ImportHistoricalData)). Simutaneouly, the real-time monitoring data is continuously flowing into the IoTDB system (we will introduce the import method in Section 3.3.2 of this chapter). 

### Sample Data
Based on the description of the above sample scenarios, we provide you with a simplified sample data. The data download address is http://tsfile.org/download.

The basic information of the data is shown in Table below.
 
<center>**Table: The basic information of the data**

|Name  |Data Type|  Coding | Meaning |
|:---|:---|:---|:---|
|root.ln.wf01.wt01.status|   Boolean|PLAIN| the power supply status of  ln group wf01 plant wt01 device |
|root.ln.wf01.wt01.temperature  |Float|RLE| the temperature of ln group wf01 plant wt01 device|
|root.ln.wf02.wt02.hardware  |Text|PLAIN| the hardware version of ln group wf02 plant wt02 device|
|root.ln.wf02.wt02.status  |Boolean|PLAIN| the power supply status of  ln group wf02 plant wt02 device|
|root.sgcc.wf03.wt01.status|Boolean|PLAIN| the power supply status of  sgcc group wf03 plant wt01 device|
|root.sgcc.wf03.wt01.temperature   |Float|RLE| the temperature of sgcc group wf03 plant wt01 device|

</center>

The time span of this data is from 10:00 on November 1, 2017 to 12:00 on November 2, 2017. The frequency at which data is generated is two minutes each.

In [Data Model Selection and Creation](Chapter3DataModelSelectionandCreation), we will show how to apply IoTDB's data model rules to construct the data model shown above. In [Import Historical Data](Chapter3ImportHistoricalData), we will introduce you to the method of importing historical data, and in [Import Real-time Data](Chapter3ImportReal-timeData), we will introduce you to the method of accessing real-time data. In [Data Query](Chapter3DataQuery), we will introduce you to three typical data query patterns using IoTDB. In [Data Maintenance](Chapter3DataMaintenance), we will show you how to update and delete data using IoTDB.