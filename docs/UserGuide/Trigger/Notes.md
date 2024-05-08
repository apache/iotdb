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

# Notes

- The trigger takes effect from the time of registration, and does not process the existing historical data. **That is, only insertion requests that occur after the trigger is successfully registered will be listened to by the trigger. **
- The fire process of trigger is synchronous currently, so you need to ensure the efficiency of the trigger, otherwise the writing performance may be greatly affected. **You need to guarantee concurrency safety of triggers yourself**.
- Please do no register too many triggers in the cluster. Because the trigger information is fully stored in the ConfigNode, and there is a copy of the information in all DataNodes
- **It is recommended to stop writing when registering triggers**. Registering a trigger is not an atomic operation. When registering a trigger, there will be an intermediate state in which some nodes in the cluster have registered the trigger, and some nodes have not yet registered successfully. To avoid write requests on some nodes being listened to by triggers and not being listened to on some nodes, we recommend not to perform writes when registering triggers.
- When the node holding the stateful trigger instance goes down, we will try to restore the corresponding instance on another node. During the recovery process, we will call the restore interface of the trigger class once.
- The trigger JAR package has a size limit, which must be less than min(`config_node_ratis_log_appender_buffer_size_max`, 2G), where `config_node_ratis_log_appender_buffer_size_max` is a configuration item. For the specific meaning, please refer to the IOTDB configuration item description.
- **It is better not to have classes with the same full class name but different function implementations in different JAR packages.** For example, trigger1 and trigger2 correspond to resources trigger1.jar and trigger2.jar respectively. If two JAR packages contain a `org.apache.iotdb.trigger.example.AlertListener` class, when `CREATE TRIGGER` uses this class, the system will randomly load the class in one of the JAR packages, which will eventually leads the inconsistent behavior of trigger and other issues.