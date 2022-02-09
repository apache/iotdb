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

当面临一批大量的同类型的实体，这些实体的物理量都相同时，为每个序列注册时间序列一方面时间序列的元数据将占用较多的内存资源，另一方面大量序列的维护工作也会十分复杂。

为了实现同类型不同实体的物理量元数据共享，减少元数据内存占用，同时简化同类型实体的管理，IoTDB引入物理量模板功能。

下图展示了一个燃油车场景的数据模型，各地区的多台燃油车的速度、油量、加速度、角速度四个物理量将会被采集，显然这些燃油车实体具备相同的物理量。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_without_template.png?raw=true" alt="example without template">

### 概念定义

物理量模板（Schema template，v0.13 起支持）

实际应用中有许多实体所采集的物理量相同，即具有相同的工况名称和类型，可以声明一个**物理量模板**来定义可采集的物理量集合。

将物理量模版挂载在树形数据模式的任意节点上，表示该节点下的所有实体具有相同的物理量集合。

目前每一条路径节点仅允许挂载一个物理量模板，即当一个节点被挂载物理量模板后，它的祖先节点和后代节点都不能再挂载物理量模板。实体将使用其自身或祖先的物理量模板作为有效模板。

使用物理量模板后，问题背景中示例的燃油车数据模型将会转变至下图所示的形式。所有的物理量元数据仅在模板中保存一份，所有的实体共享模板中的元数据。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_with_template.png?raw=true" alt="example with template">

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

* 将名为 `templateName` 的物理量模板挂载到 `prefixPath` 路径下，在执行这一步之前，你需要创建名为 `templateName` 的物理量模板

``` 
void setSchemaTemplate(String templateName, String prefixPath)
```

挂载好物理量模板后，即可进行数据的写入。例如存储组为root.sg，模板t1(s1,s2)被挂载到了节点root.sg.car，那么可直接向时间序列（如root.sg.car.d1.s1和root.sg.car.d1.s2）写入时间序列数据，该时间序列已可被当作正常创建的序列使用。

使用物理量模板的示例可以参见 `example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java`。
