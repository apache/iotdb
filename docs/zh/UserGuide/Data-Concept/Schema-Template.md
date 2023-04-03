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

# 元数据模板

## 问题背景

对于大量的同类型的实体，每一个实体下的物理量都相同，为每个序列注册时间序列一方面时间序列的元数据将占用较多的内存资源，另一方面，大量序列的维护工作也会十分复杂。

为了实现同类型不同实体的物理量元数据共享，减少元数据内存占用，同时简化同类型实体的管理，IoTDB引入元数据模板功能。

下图展示了一个燃油车场景的数据模型，各地区的多台燃油车的速度、油量、加速度、角速度四个物理量将会被采集，显然这些燃油车实体具备相同的物理量。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Measurement-Template/example_without_template.png?raw=true" alt="example without template">

## 概念定义

元数据模板（Schema template）

实际应用中有许多实体所采集的物理量相同，即具有相同的工况名称和类型，可以声明一个**元数据模板**来定义可采集的物理量集合。

将元数据模版挂载在树形数据模式的任意节点上，表示该节点下的所有实体具有相同的物理量集合。

目前每一条路径节点仅允许挂载一个元数据模板，即当一个节点被挂载元数据模板后，它的祖先节点和后代节点都不能再挂载元数据模板。实体将使用其自身或祖先的元数据模板作为有效模板。

特别需要说明的是，挂载模板与使用模板的概念不同。一个节点挂载模板后，其所有后代节点都可以使用这个模板，因此可以通过向同类实体的祖先节点挂载模板来简化操作。当系统向挂载模板的节点（或其后代节点）插入模板中定义的物理量时，这个节点就被设置为“正在使用模板”。

使用元数据模板后，问题背景中示例的燃油车数据模型将会转变至下图所示的形式。所有的物理量元数据仅在模板中保存一份，所有的实体共享模板中的元数据。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Measurement-Template/example_with_template.png?raw=true" alt="example with template">

### 生命周期

了解元数据的生命周期及相关名词，有助于更顺畅地使用元数据模板。在描述这一概念时，有六个关键词分别指向特定的过程，分别是“创建”、“挂载”、“激活”、“解除”、“卸载”、和“删除”。下图展示了一个模板从创建、挂到删除的全部过程。当用户操作执行其中任一过程时，系统都会执行对应条件检查，如条件检查通过，则操作成功，否则，操作会被拒绝：

1. 创建模板时，检查确认正在创建的模板名称与所有已存在的模板不重复；
2. 在某节点挂载模板，需检查该节点的所有祖先节点与子孙节点，确认均未挂载任何模板；
3. 在某节点激活模板，需检查确认该节点或其祖先已挂载对应模板，且该节点下不存在与模板中同名的物理量；
4. 在某节点解除模板时，需确认该节点已经激活了模板，请注意，解除模板会删除该节点通过模板实例化的物理量及其数据点；
5. 在某节点卸载模板时，需检查确认该节点曾挂载该模板，且其所有子孙节点均不处于模板激活状态；
6. 删除模板时，需检查确认模板没有挂载在任何一个节点上。

最后需要补充的是，**对挂载模板与激活模板进行区分，是为了服务一种常见的场景**：在 Apache IoTDB 元数据模型 MTree 中，经常需要在数量众多的节点上“应用”元数据模板，而这些节点一般拥有共同的祖先节点。因此，可以在其共同祖先节点**挂载**模板，而不必对其大量的孩子节点进行挂载操作。对于需要“应用”模板的节点，则应该使用**激活模板**的操作。

<img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Data-Concept/Measurement-Template/example_template_lifetime_zh.png?raw=true" alt="example with template">

## 使用

目前，用户可以通过 Session 编程接口或 IoTDB-SQL 来使用元数据模板，包括模板的创建、修改、挂载与卸载等。Session 编程接口的详细文档可参见[此处](../API/Programming-Java-Native-API.md)，IoTDB-SQL 的详细文档可参加[此处](../Operate-Metadata/Template.md)。下文将以 Session 中使用方法为例，进行简要介绍。


* 创建元数据模板

在 Session 中创建元数据模板，可以通过先后创建 Template、MeasurementNode 的对象，构造模板内部物理量结构，并通过以下接口创建模板

```java
public void createSchemaTemplate(Template template);

Class Template {
    private String name;
    private boolean directShareTime;
    Map<String, Node> children;
    public Template(String name, boolean isShareTime);
    
    public void addToTemplate(Node node);
    public void deleteFromTemplate(String name);
    public void setShareTime(boolean shareTime);
}

Abstract Class Node {
    private String name;
    public void addChild(Node node);
    public void deleteChild(Node node);
}

Class MeasurementNode extends Node {
    TSDataType dataType;
    TSEncoding encoding;
    CompressionType compressor;
    public MeasurementNode(String name, 
                           TSDataType dataType, 
                           TSEncoding encoding,
                          CompressionType compressor);
}
```

* 构造元数据模板

构造上图中的元数据模板，并挂载到对应节点，可参考如下代码。**请注意，我们强烈建议您将模板设置在 database 或 database 下层的节点中，以更好地适配未来地更新及各模块的协作。**

``` java
MeasurementNode nodeV = new MeasurementNode("velocity", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeF = new MeasurementNode("fuel_amount", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
MeasurementNode nodeA = new MeasurementNode("acceleration", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
MeasurementNode nodeAng = new MeasurementNode("angular_velocity", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);

Template template = new Template("template");
template.addToTemplate(nodeV);
template.addToTemplate(nodeF);
template.addToTemplate(nodeA);
template.addToTemplate(nodeAng);

createSchemaTemplate(template);
setSchemaTemplate("template", "root.Beijing");
```

挂载元数据模板后，即可进行数据的写入。如按上述代码创建并挂载模板，并在 root.Beijing 路径上设置了 database 后，即可写入例如 root.Beijing.petro_vehicle.velocity 等时间序列数据，系统将自动创建 petro_vehicle 节点，并设置其“正在使用模板”，对写入数据应用模板中为 velocity 定义的元数据信息。
