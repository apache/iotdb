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

# Schema Template

## Problem scenario

When faced with a large number of entities of the same type and the measurements of these entities are the same, registering time series for each measurent will result in the following problems. On the one hand, the metadata of time series will occupy a lot of memory resources; on the other hand, the maintenance of a large number of time series will be very complex.

In order to enable different entities of the same type to share metadata, reduce the memory usage of metadata, and simplify the management of numerous entities and measurements, IoTDB introduces the schema template function.

The following picture illustrates the data model of petrol vehicle scenario. The velocity, fuel amount, acceleration, and angular velocity of each petrol vehicle spread over cities will be collected. Obviously, the measurements of single petrol vehicle are the same as those of another.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_without_template.png?raw=true" alt="example without template">

## Concept

Supported from v0.13

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A schema template can be declared to define the collectable measurements set. Schema template is hung on any node of the tree data pattern, which means that all entities under the node have the same measurements set.

Currently you can only set one schema template on a specific path. If there's one schema template on one node, it will be forbidden to set any schema template on the ancestors or descendants of this node. An entity will use it's own schema template or ancestor's schema template.

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to schema template will be introduced one by one.

After applying schema template, the following picture illustrates the new data model of petrol vehicle scenario. All petrol vehicles share the schemas defined in template. There are no redundancy storage of measurement schemas.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_with_template.png?raw=true" alt="example with template">

## Usage

Java Native API, C++ Native API, and IoTDB-SQL have supported Schema Template usage.