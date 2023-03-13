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

## IoTDB数据文件夹概览工具

IoTDB数据文件夹概览工具用于打印出数据文件夹的结构概览信息，工具位置为 tools/tsfile/print-iotdb-data-dir。

### 用法

-   Windows:

```bash
.\print-iotdb-data-dir.bat <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>) 
```

-   Linux or MacOs:

```shell
./print-iotdb-data-dir.sh <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>)
```

注意：如果没有设置输出结果的存储路径, 将使用相对路径"IoTDB_data_dir_overview.txt"作为默认值。

### 示例

以Windows系统为例：

`````````````````````````bash
.\print-iotdb-data-dir.bat D:\github\master\iotdb\data\datanode\data
````````````````````````
Starting Printing the IoTDB Data Directory Overview
````````````````````````
output save path:IoTDB_data_dir_overview.txt
data dir num:1
143  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
|==============================================================
|D:\github\master\iotdb\data\datanode\data
|--sequence
|  |--root.redirect0
|  |  |--1
|  |  |  |--0
|  |--root.redirect1
|  |  |--2
|  |  |  |--0
|  |--root.redirect2
|  |  |--3
|  |  |  |--0
|  |--root.redirect3
|  |  |--4
|  |  |  |--0
|  |--root.redirect4
|  |  |--5
|  |  |  |--0
|  |--root.redirect5
|  |  |--6
|  |  |  |--0
|  |--root.sg1
|  |  |--0
|  |  |  |--0
|  |  |  |--2760
|--unsequence
|==============================================================
`````````````````````````

