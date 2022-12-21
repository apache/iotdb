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

# FAQ for Cluster Setup

## 1. Cluster StartUp and Stop

### 1). Failed to start ConfigNode for the first time, how to find the reason?

- Make sure that the data/confignode directory is cleared when start ConfigNode for the first time.
- Make sure that the <IP+Port> used by ConfigNode is not occupied, and the <IP+Port> is also not conflicted with other ConfigNodes.
- Make sure that the `cn_target_confignode_list` is configured correctly, which points to the alive ConfigNode. And if the ConfigNode is started for the first time, make sure that `cn_target_confignode_list` points to itself.
- Make sure that the configuration(consensus protocol and replica number) of the started ConfigNode is accord with the `cn_target_confignode_list` ConfigNode.

### 2). ConfigNode is started successfully, but why the node doesn't appear in the results of `show cluster`?

- Examine whether the `cn_target_confignode_list` points to the correct address. If `cn_target_confignode_list` points to itself, a new ConfigNode cluster is started.

### 3). Failed to start DataNode for the first time, how to find the reason?

- Make sure that the data/datanode directory is cleared when start DataNode for the first time. If the start result is “Reject DataNode restart.”, maybe the data/datanode directory is not cleared.
- Make sure that the <IP+Port> used by DataNode is not occupied, and the <IP+Port> is also not conflicted with other DataNodes. 
- Make sure that the `dn_target_confignode_list` points to the alive ConfigNode.

### 4). Failed to remove DataNode, how to find the reason?

- Examine whether the parameter of remove-datanode.sh is correct, only rpcIp:rpcPort and dataNodeId are correct parameter.
- Only when the number of available DataNodes in the cluster is greater than max(schema_replication_factor, data_replication_factor), removing operation can be executed.
- Removing DataNode will migrate the data from the removing DataNode to other alive DataNodes. Data migration is based on Region, if some regions are migrated failed, the removing DataNode will always in the status of `Removing`.
- If the DataNode is in the status of `Removing`, the regions in the removing DataNode will also in the status of `Removing` or `Unknown`, which are unavailable status. Besides, the removing DataNode will not receive new write requests from client. 
And users can use the command `set system status to running` to make the status of DataNode from Removing to Running;
If users want to make the Regions from Removing to available status, command `migrate region from datanodeId1 to datanodeId2` can take effect, this command can migrate the regions to other alive DataNodes.
Besides, IoTDB will publish `remove-datanode.sh -f` command in the next version, which can remove DataNodes forced (The failed migrated regions will be discarded).

### 5). Whether the down DataNode can be removed?

- The down DataNode can be removed only when the replica factor of schema and data is greater than 1.  
Besides, IoTDB will publish `remove-datanode.sh -f` function in the next version.

### 6).What should be paid attention to when upgrading from 0.13 to 1.0?

- The file structure between 0.13 and 1.0 is different, we can't copy the data directory from 0.13 to 1.0 to use directly. 
If you want to load the data from 0.13 to 1.0, you can use the LOAD function.
- The default RPC address of 0.13 is `0.0.0.0`, but the default RPC address of 1.0 is `127.0.0.1`.


## 2. Cluster Restart

### 1). How to restart any ConfigNode in the cluster?
- First step: stop the process by stop-confignode.sh or kill PID of ConfigNode. 
- Second step: execute start-confignode.sh to restart ConfigNode.

### 2). How to restart any DataNode in the cluster?
- First step: stop the process by stop-datanode.sh or kill PID of DataNode.
- Second step: execute start-datanode.sh to restart DataNode.

### 3). If it's possible to restart ConfigNode using the old data directory when it's removed?
- Can't. The running result will be "Reject ConfigNode restart. Because there are no corresponding ConfigNode(whose nodeId=xx) in the cluster".

### 4). If it's possible to restart DataNode using the old data directory when it's removed?
- Can't. The running result will be "Reject DataNode restart. Because there are no corresponding DataNode(whose nodeId=xx) in the cluster. Possible solutions are as follows:...".

### 5). Can we execute start-confignode.sh/start-datanode.sh successfully when delete the data directory of given ConfigNode/DataNode without killing the PID?
- Can't. The running result will be "The port is already occupied".

## 3. Cluster Maintenance

### 1). How to find the reason when Show cluster failed, and error logs like "please check server status" are shown?
- Make sure that more than one half ConfigNodes are alive.
- Make sure that the DataNode connected by the client is alive.

### 2). How to fix one DataNode when the disk file is broken?
- We can use remove-datanode.sh to fix it. Remove-datanode will migrate the data in the removing DataNode to other alive DataNodes.
- IoTDB will publish Node-Fix tools in the next version.

### 3). How to decrease the memory usage of ConfigNode/DataNode?
- Adjust the MAX_HEAP_SIZE、MAX_DIRECT_MEMORY_SIZE options in conf/confignode-env.sh and conf/datanode-env.sh.


