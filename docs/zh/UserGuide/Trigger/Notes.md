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



## 重要注意事项

- 触发器从注册时开始生效，不对已有的历史数据进行处理。**即只有成功注册触发器之后发生的写入请求才会被触发器侦听到。**
- 触发器目前采用**同步触发**，所以编写时需要保证触发器效率，否则可能会大幅影响写入性能。**您需要自己保证触发器内部的并发安全性**。
- 集群中**不能注册过多触发器**。因为触发器信息全量保存在 ConfigNode 中，并且在所有 DataNode 都有一份该信息的副本。
- **建议注册触发器时停止写入**。注册触发器并不是一个原子操作，注册触发器时，会出现集群内部分节点已经注册了该触发器，部分节点尚未注册成功的中间状态。为了避免部分节点上的写入请求被触发器侦听到，部分节点上没有被侦听到的情况，我们建议注册触发器时不要执行写入。
- 触发器将作为进程内程序执行，如果您的触发器编写不慎，内存占用过多，由于 IoTDB 并没有办法监控触发器所使用的内存，所以有 OOM 的风险。
- 持有有状态触发器实例的节点宕机时，我们会尝试在另外的节点上恢复相应实例，在恢复过程中我们会调用一次触发器类的 restore 接口，您可以在该接口中实现恢复触发器所维护的状态的逻辑。
- 触发器 JAR 包有大小限制，必须小于 min(`config_node_ratis_log_appender_buffer_size_max`, 2G)，其中 `config_node_ratis_log_appender_buffer_size_max` 是一个配置项，具体含义可以参考 IOTDB 配置项说明。
- **不同的 JAR 包中最好不要有全类名相同但功能实现不一样的类**。例如：触发器 trigger1、trigger2 分别对应资源 trigger1.jar、trigger2.jar。如果两个 JAR 包里都包含一个 `org.apache.iotdb.trigger.example.AlertListener` 类，当 `CREATE TRIGGER` 使用到这个类时，系统会随机加载其中一个 JAR 包中的类，最终导致触发器执行行为不一致以及其他的问题。