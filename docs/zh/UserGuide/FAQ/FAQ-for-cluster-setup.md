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

# 分布式部署 FAQ

## 一、集群启停

### 1. ConfigNode初次启动失败，如何排查原因？

- ConfigNode初次启动时确保已清空data/confignode目录
- 确保该ConfigNode使用到的<IP+端口>没有被占用，没有与已启动的ConfigNode使用到的<IP+端口>冲突 
- 确保该ConfigNode的cn_target_confignode_list（指向存活的ConfigNode；如果该ConfigNode是启动的第一个ConfigNode，该值指向自身）配置正确 
- 确保该ConfigNode的配置项（共识协议、副本数等）等与cn_target_confignode_list对应的ConfigNode集群一致

### 2. ConfigNode初次启动成功，show cluster的结果里为何没有该节点？

- 检查cn_target_confignode_list是否正确指向了正确的地址； 如果cn_target_confignode_list指向了自身，则会启动一个新的ConfigNode集群

### 3. DataNode初次启动失败，如何排查原因？

- DataNode初次启动时确保已清空data/datanode目录。 如果启动结果为“Reject DataNode restart.”则表示启动时可能没有清空data/datanode目录
- 确保该DataNode使用到的<IP+端口>没有被占用，没有与已启动的DataNode使用到的<IP+端口>冲突
- 确保该DataNode的dn_target_confignode_list指向存活的ConfigNode

### 4. 移除DataNode执行失败，如何排查？

- 检查remove-datanode脚本的参数是否正确，是否传入了正确的ip:port或正确的dataNodeId
- 只有集群可用节点数量 > max(元数据副本数量, 数据副本数量)时，移除操作才允许被执行
- 执行移除DataNode的过程会将该DataNode上的数据迁移到其他存活的DataNode，数据迁移以Region为粒度，如果某个Region迁移失败，则被移除的DataNode会一直处于Removing状态
- 补充：处于Removing状态的节点，其节点上的Region也是Removing或Unknown状态，即不可用状态。 该Remvoing状态的节点也不会接受客户端的请求。 
如果要使Removing状态的节点变为可用，用户可以使用set system status to running 命令将该节点设置为Running状态； 
如果要使迁移失败的Region处于可用状态，可以使用migrate region from datanodeId1 to datanodeId2 命令将该不可用的Region迁移到其他存活的节点。 
另外IoTDB后续也会提供remove-datanode.sh -f命令，来强制移除节点（迁移失败的Region会直接丢弃）

### 5. 挂掉的DataNode是否支持移除？

- 当前集群副本数量大于1时可以移除。 如果集群副本数量等于1，则不支持移除。 在下个版本会推出强制移除的命令

### 6. 从0.13升级到1.0需要注意什么？

- 0.13版本与1.0版本的文件目录结构是不同的，不能将0.13的data目录直接拷贝到1.0集群使用。如果需要将0.13的数据导入至1.0，可以使用LOAD功能
- 0.13版本的默认RPC地址是0.0.0.0，1.0版本的默认RPC地址是127.0.0.1


## 二、集群重启

### 1. 如何重启集群中的某个ConfigNode？
- 第一步：通过stop-confignode.sh或kill进程方式关闭ConfigNode进程
- 第二步：通过执行start-confignode.sh启动ConfigNode进程实现重启
- 下个版本IoTDB会提供一键重启的操作

### 2. 如何重启集群中的某个DataNode？
- 第一步：通过stop-datanode.sh或kill进程方式关闭DataNode进程
- 第二步：通过执行start-datanode.sh启动DataNode进程实现重启
- 下个版本IoTDB会提供一键重启的操作

### 3. 将某个ConfigNode移除后（remove-confignode），能否再利用该ConfigNode的data目录重启？
- 不能。会报错：Reject ConfigNode restart. Because there are no corresponding ConfigNode(whose nodeId=xx) in the cluster.

### 4. 将某个DataNode移除后（remove-datanode），能否再利用该DataNode的data目录重启？
- 不能正常重启，启动结果为“Reject DataNode restart. Because there are no corresponding DataNode(whose nodeId=xx) in the cluster. Possible solutions are as follows:...”

### 5. 用户看到某个ConfigNode/DataNode变成了Unknown状态，在没有kill对应进程的情况下，直接删除掉ConfigNode/DataNode对应的data目录，然后执行start-confignode.sh/start-datanode.sh，这种情况下能成功吗?
- 无法启动成功，会报错端口已被占用

## 三、集群运维

### 1. Show cluster执行失败，显示“please check server status”，如何排查?
- 确保ConfigNode集群一半以上的节点处于存活状态
- 确保客户端连接的DataNode处于存活状态

### 2. 某一DataNode节点的磁盘文件损坏，如何修复这个节点?
- 当前只能通过remove-datanode的方式进行实现。remove-datanode执行的过程中会将该DataNode上的数据迁移至其他存活的DataNode节点（前提是集群设置的副本数大于1）
- 下个版本IoTDB会提供一键修复节点的功能

### 3. 如何降低ConfigNode、DataNode使用的内存？
- 在conf/confignode-env.sh、conf/datanode-env.sh文件可通过调整MAX_HEAP_SIZE、MAX_DIRECT_MEMORY_SIZE等选项可以调整ConfigNode、DataNode使用的最大堆内、堆外内存


