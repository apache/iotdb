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

## Schema Template

### Problem scenario

When faced with a large number of entities of the same type and the measurements of these entities are the same, registering time series for each measurent will result in the following problems. On the one hand, the metadata of time series will occupy a lot of memory resources; on the other hand, the maintenance of a large number of time series will be very complex.

In order to enable different entities of the same type to share metadata, reduce the memory usage of metadata, and simplify the management of numerous entities and measurements, IoTDB introduces the schema template function.

The following picture illustrates the data model of petrol vehicle scenario. The velocity, fuel amount, acceleration, and angular velocity of each petrol vehicle spread over cities will be collected. Obviously, the measurements of single petrol vehicle are the same as those of another.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_without_template.png?raw=true" alt="example without template">

### Concept

Supported from v0.13

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A schema template can be declared to define the collectable measurements set. Schema template is hung on any node of the tree data pattern, which means that all entities under the node have the same measurements set.

Currently you can only set one schema template on a specific path. If there's one schema template on one node, it will be forbidden to set any schema template on the ancestors or descendants of this node. An entity will use it's own schema template or ancestor's schema template.

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to schema template will be introduced one by one.

After applying schema template, the following picture illustrates the new data model of petrol vehicle scenario. All petrol vehicles share the schemas defined in template. There are no redundancy storage of measurement schemas.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Data%20Concept/Measurement%20Template/example_with_template.png?raw=true" alt="example with template">

### Usage

Currently, only Session API supports Schema Template usage.

* Create a schema template
```

* name: template name
* measurements: List of measurements, if it is a single measurement, just put it's name
*     into a list and add to measurements if it is a vector measurement, put all measurements of
*     the vector into a list and add to measurements
* dataTypes: List of datatypes, if it is a single measurement, just put it's type into a
*     list and add to dataTypes if it is a vector measurement, put all types of the vector
*     into a list and add to dataTypes
* encodings: List of encodings, if it is a single measurement, just put it's encoding into
*     a list and add to encodings if it is a vector measurement, put all encodings of the
*     vector into a list and add to encodings
* compressors: List of compressors                            
void createSchemaTemplate(
      String templateName,
      List<String> schemaName,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
```
* Set the device template named `templateName` at path `prefixPath`. You should firstly create the template using

``` 

void setSchemaTemplate(String templateName, String prefixPath)

```

After setting schema template，data could be inserted directly to the according timeseries. For example, suppose there's storage group root.sg and template t1(s1,s2) has been set to root.sg.car，then timeseries like root.sg.car.d1.s1 and root.sg.car.d1.s2 are available and data can be inserted。

For examples of schema template, you can refer to example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java