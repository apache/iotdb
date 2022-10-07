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
# 虚拟存储组
## 背景

存储组由用户显示指定，使用语句"SET STORAGE GROUP TO"来指定存储组，每一个存储组有一个对应的 StorageGroupProcessor

为了确保最终一致性，每一个存储组有一个数据插入锁（排它锁）来同步每一次插入操作。
所以服务端数据写入的并行度为存储组的数量。

## 问题

从背景中可知，IoTDB数据写入的并行度为 max(客户端数量，服务端数据写入的并行度)，也就是max(客户端数量，存储组数量)

在生产实践中，存储组的概念往往与特定真实世界实体相关（例如工厂，地点，国家等）。
因此存储组的数量可能会比较小，这会导致IoTDB写入并行度不足。即使我们开再多的客户端写入线程，也无法走出这种困境。

## 解决方案

我们的方案是将一个存储组下的设备分为若干个设备组（称为虚拟存储组），将同步粒度从存储组级别改为虚拟存储组粒度。

更具体的，我们使用哈希将设备分到不同的虚拟存储组下，例如：
对于一个名为"root.sg.d"的设备（假设其存储组为"root.sg"），它属于的虚拟存储组为"root.sg.[hash("root.sg.d") mod num_of_virtual_storage_group]"

## 使用方法

通过改变如下配置来设置每一个存储组下虚拟存储组的数量：

```
virtual_storage_group_num
```

推荐值为[virtual storage group number] = [CPU core number] / [user-defined storage group number]

参考[配置手册](../Reference/Config-Manual.md)以获取更多信息。