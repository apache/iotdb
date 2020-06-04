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

# 元数据管理

IoTDB 的元数据统一由 MManger 管理，包括以下几个部分：

* 元数据树
* 元数据日志管理
* 标签/属性管理


## MManager

* 维护 tag 倒排索引：`Map<String, Map<String, Set<LeafMNode>>> tagIndex`

	> tag key -> tag value -> timeseries LeafMNode

该类初始化时，会replay mlog中的内容，将元数据信息还原出来，元数据操作日志主要有六种类型，每个操作前都会先获得整个元数据的写锁（存储在MManager中），操作完后释放：

* 创建时间序列
	* 检查存储组是否存在，如果不存在且开启自动创建，则创建存储组
	* 在 MTree 中创建时间序列，可带扩展名
	* 如果开启动态参数，检查内存是否满足
	* 如果非重启（需要记录日志）
		* 拿到标签文件当前 offset
		* 将时间序列信息记录到 mlog 中
		* 将 tags/attributes 持久化到tlog中
	* 如果是重启（需要恢复内存结构）
		* 根据 mlog 中的 offset 读取标签文件，重建 tagIndex 索引

* 删除时间序列
	* 获得所有满足前缀的时间序列的fullPath
	* 遍历上一步获得的所有时间序列，并逐个在 MTree 中删除
		* 删除的时候，需要首先获得该时间序列所在device的InternalMNode的写锁
		* 若获取成功
			* 删除MTree中对应的LeafMNode
			* 根据LeafMNode中的offset，读取标签文件，更新 tagIndex 索引
			* 若发现某一个存储组为空，则记录下该空的存储组名
		* 若获取失败
			* 返回此未删除成功的时间序列
	* 如果非重启（需要记录日志）
	      *  删除记录下的空的存储组
		* 将所删除的时间序列信息记录到 mlog 中
		* 目前并不会删除 tlog 中关于此时间序列的标签/属性信息。
	

* 设置存储组
	* 在 MTree 中创建存储组
	* 如果开启动态参数，检查内存是否满足
	* 如果非重启，将log持久化至mlog中

* 删除存储组
	* 在 MTree 中删除对应的存储组，并返回该存储组下的所有时间序列的LeafMNode
		* 删除的时候，需要首先获得该存储组的InternalMNode的写锁
		* 若获取成功
			* 删除MTree中对应的InternalMNode
		* 若获取失败
			* 返回此未删除成功的存储组名
	* 遍历上一步返回的LeafMNode，根据LeafMNode中的offset，读取标签文件，更新tagIndex 索引
	* 如果非重启，将log持久化至mlog中

* 设置TTL
	* 获得对应存储组的StorageGroupMNode，修改其TTL属性
	* 如果非重启，将log持久化至mlog中

* 改变时间序列标签信息offset
	* 修改时间序列对应的LeafMNode中的offset


除了这六种需要记录日志的操作外，还有六种对时间序列标签/属性信息进行更新的操作，同样的，每个操作前都会先获得整个元数据的写锁（存储在MManager中），操作完后释放：

* 重命名标签或属性
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 若新键已经存在，则抛异常
	* 若新键不存在
		* 若旧键不存在，则抛异常
		* 若旧键存在，用新键替换旧键，并持久化至tlog中
		* 如果旧键是标签，则还需更新 tagIndex

* 重新设置标签或属性的值
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 若需要重新设置的标签或属性的键值不存在，则抛异常
	* 若需要重新设置的是标签，则还需更新 tagIndex
	* 将更新后的标签和属性信息持久化至tlog中

* 删除已经存在的标签或属性
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 遍历需要删除的标签或属性，若不存在，则跳过
	* 若需要删除的是标签，则还需更新 tagIndex
	* 将更新后的标签和属性信息持久化至tlog中

* 添加新的标签
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签信息
	* 遍历需要添加的标签，若已存在，则抛异常
	* 将更新后的标签信息持久化至tlog中
	* 根据添加的标签信息，更新 tagIndex

* 添加新的属性
   * 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取属性信息
	* 遍历需要添加的属性，若已存在，则抛异常
	* 将更新后的属性信息持久化至tlog中

* 更新插入标签和属性
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 遍历需要更新插入的标签和属性，若已存在，则用新值更新；若不存在，则添加
	* 将更新后的属性信息持久化至tlog中
	* 如果包含更新插入中包含标签信息，还需更新 tagIndex



## 元数据树

* org.apache.iotdb.db.metadata.MTree

树中包括三种节点：StorageGroupMNode、InternalMNode（非叶子节点）、LeafMNode（叶子节点），他们都是MNode的子类。

每个InternalMNode中都有一个读写锁，查询元数据信息时，需要获得路径上每一个InternalMNode的读锁，修改元数据信息时，如果修改的是LeafMNode，需要获得其父节点的写锁，若修改的是InternalMNode，则只需获得本身的写锁。若该InternalMNode位于Device层，则还包含了一个`Map<String, MNode> aliasChildren`，用于存储别名信息。

StorageGroupMNode 继承 InternalMNode，包含存储组的元数据信息，如 TTL。

LeafMNode 中包含了对应时间序列的Schema信息，其别名(若没有别名，则为null)以及该时间序列的标签/属性信息在tlog文件中的offset(若没有标签/属性，则为-1)

示例：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625246-fc3e8200-467e-11ea-8815-67b9c4ab716e.png">

IoTDB 的元数据管理采用目录树的形式，倒数第二层为设备层，最后一层为传感器层。

默认存在根节点 root，创建存储组、删除存储组、创建时间序列、删除时间序列均为对树的节点的操作。

* 创建存储组（root.a.b.sg）
	* 为当前存储组创建中间节点（a.b）
	* 确保此路径前缀不存在其他存储组（不允许存储组嵌套）
	* 检查确保存储组不存在
	* 创建存储组节点

* 创建时间序列（root.a.b.sg.d.s1）
	* 遍历路径，确保存储组已经创建
	* 找到倒数第二层节点，检查叶子节点是否存在
	* 创建叶子节点，叶子节点内存储别名
	* 如果有别名，则在设备节点多创建一个链接指向叶子节点

* 删除存储组和删除时间序列的操作相似，即将存储组或时间序列节点在其父节点中删除，时间序列节点还需要将其别名在父节点中删除；若在删除过程中，发现某一节点没有任何子节点了，还需要递归删除此节点。
	
## 元数据日志管理

* org.apache.iotdb.db.metadata.MLogWriter

所有元数据的操作均会记录到元数据日志文件中，此文件默认为 data/system/schema/mlog.txt。

系统重启时会重做 mlog 中的日志，重做之前需要标记不需要记录日志。当重启结束后，标记需要记录日志。

元数据日志的类型由 MetadataOperationType 类记录。mlog 直接存储字符串编码。

示例 sql 及对应的 mlog 记录：

* set storage group to root.turbine

	> mlog: 2,root.turbine
	
	> 格式: 2,path

* delete storage group root.turbine	
	
	> mlog: 1,root.turbine
	
	> 格式: 1,path

* create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)

	> mlog: 0,root.turbine.d1.s1,3,2,1,,温度,offset
	
	> 格式: 0,path,TSDataType,TSEncoding,CompressionType,[properties],[alias],[tag-attribute offset]

* delete timeseries root.turbine.d1.s1

	> mlog: 1,root.turbine.d1.s1
	
	> 格式: 1,path	

* set ttl to root.turbine 10
	
	> mlog: 10,root.turbine,10
		
	> 格式: 10,path,ttl

* alter timeseries root.turbine.d1.s1 add tags(tag1=v1)
   > 只有当root.turbine.d1.s1原来不存在任何标签/属性信息时，该sql才会产生日志
   
   > mlog: 12,root.turbine.d1.s1,10
   
	> 格式: 10,path,[change offset]

## 标签文件
* org.apache.iotdb.db.metadata.TagLogFile


所有时间序列的标签/属性信息都会保存在标签文件中，此文件默认为 data/system/schema/mlog.txt。

* 每条时间序列的 tags 和 attributes 持久化总字节数为 L，在 iotdb-engine.properties 中配置。

* 持久化内容：Map<String,String> tags, Map<String,String> attributes，如果内容不足 L，则需补空。

> create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes (attr1=v1, attr2=v2)

> 在tlog.txt中的内容：

> tagsSize (tag1=v1, tag2=v2) attributesSize (attr1=v1, attr2=v2)

## 系统重启
当系统重启的时候，会逐条重做一遍mlog中记录的日志。
