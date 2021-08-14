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

## 物理量模板

### 问题背景

某汽车设备提供商向汽车装配商提供自己的车载传感器组件，这种组件包括30个左右的独立传感器，这些组件都是同一型号的，即每个组件包含的独立传感器都相同。现需要支持的组件总数约为100万个，该设备提供商需要存储、查询和分析这些传感器收集的数据以实现其商业价值。

结合IoTDB实际情况进行元数据存储开销分析，可知按照普通的方案进行存储需要维护30*100万即3000万个工况节点和相关元数据，由实践经验可知，每个工况节点内存占用约为300字节，总体内存占用将达到9GB。 在读写性能较好的情况下，IoTDB元数据占用内存往往仅占总堆内内存的10%，在此场景中即要求物理内存的总大小达到90G。

深入分析思考该实际生产背景，其关键点为“这些组件都是同一型号的”，这也意味着每一个组件中的传感器种类是完全一样的，为每一个组件的每一个传感器都创建时间序列并保存，这实际上是一种内存冗余。

一种合理的方式是将这些相同的元数据仅存储一份来节省宝贵的内存，***物理量模板*** 就是这样的功能。

### 概念定义

物理量模板（Measurement template，v0.13 起支持）

实际应用中有许多实体所采集的物理量相同，即具有相同的工况名称和类型，可以声明一个**物理量模板**来定义可采集的物理量集合。

将物理量模版挂在树形数据模式的任意节点上，表示该节点下的所有实体具有相同的物理量集合。

目前每一条路径节点仅允许挂载一个物理量模板，即当一个节点被挂载物理量模板后，它的祖先节点和后代节点都不能再挂载物理量模板。实体将使用其自身或祖先的物理量模板作为有效模板。

### 使用

当前仅支持通过Session编程接口使用物理量模板，包括模板的创建和挂载操作。


* 创建一个物理量模板

```
* name: 物理量模板名称
* measurements: 工况名称列表，如果该工况是非对齐的，直接将其名称放入一个 list 中再放入 measurements 中，
*               如果该工况是对齐的，将所有对齐工况名称放入一个 list 再放入 measurements 中
* dataTypes: 数据类型名称列表，如果该工况是非对齐的，直接将其数据类型放入一个 list 中再放入 dataTypes 中，
             如果该工况是对齐的，将所有对齐工况的数据类型放入一个 list 再放入 dataTypes 中
* encodings: 编码类型名称列表，如果该工况是非对齐的，直接将其数据类型放入一个 list 中再放入 encodings 中，
             如果该工况是对齐的，将所有对齐工况的编码类型放入一个 list 再放入 encodings 中
* compressors: 压缩方式列表                          
void createSchemaTemplate(
      String templateName,
      List<String> schemaName,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
```

* 将名为'templateName'的物理量模板挂载到'prefixPath'路径下，在执行这一步之前，你需要创建名为'templateName'的物理量模板

``` 
void setSchemaTemplate(String templateName, String prefixPath)
```

挂载好物理量模板后，即可进行数据的写入。例如存储组为root.sg，模板t1(s1,s2)被挂载到了节点root.sg.car，那么可直接向时间序列（如root.sg.car.d1.s1和root.sg.car.d1.s2）写入时间序列数据，该时间序列已可被当作正常创建的序列使用。

使用物理量模板的示例可以参见 `example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java`。
