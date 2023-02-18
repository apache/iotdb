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



# Instructions

The trigger provides a mechanism for listening to changes in time series data. With user-defined logic, tasks such as alerting and data forwarding can be conducted.

The trigger is implemented based on the reflection mechanism. Users can monitor data changes by implementing the Java interfaces. IoTDB allows users to dynamically register and drop triggers without restarting the server.

The document will help you learn to define and manage triggers.

## Pattern for listening

A single trigger can be used to listen for data changes in a time series that match a specific pattern. For example, a trigger can listen for the data changes of time series `root.sg.a`, or time series that match the pattern `root.sg.*`. When you register a trigger, you can specify the path pattern that the trigger listens on through an SQL statement.

## Trigger Type

There are currently two types of triggers, and you can specify the type through an SQL statement when registering a trigger:

- Stateful triggers: The execution logic of this type of trigger may depend on data from multiple insertion statement . The framework will aggregate the data written by different nodes into the same trigger instance for calculation to retain context information. This type of trigger is usually used for sampling or statistical data aggregation for a period of time. information. Only one node in the cluster holds an instance of a stateful trigger.
- Stateless triggers: The execution logic of the trigger is only related to the current input data. The framework does not need to aggregate the data of different nodes into the same trigger instance. This type of trigger is usually used for calculation of single row data and abnormal detection. Each node in the cluster holds an instance of a stateless trigger.

## Trigger Event

There are currently two trigger events for the trigger, and other trigger events will be expanded in the future. When you register a trigger, you can specify the trigger event through an SQL statement:

- BEFORE INSERT: Fires before the data is persisted. **Please note that currently the trigger does not support data cleaning and will not change the data to be persisted itself.**
- AFTER INSERT: Fires after the data is persisted.



