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

## Measurement Template

### Problem scenario

An automotive equipment provider provides an automotive assembler with its own on-board sensor kit, which consists of around 30 individual sensors of the same model, that is, each component contains the same individual sensor.The total number of supported components is about 1 million, and the equipment provider needs to store, query, and analyze the data collected by these sensors to achieve commercial value.

If we create timeseries for each measurement in this scenario, according to practical experience, the memory usage of each measurement node is about 300 bytes, and the total memory usage will reach 9GB.In the case of good I/O performance, IoTDB metadata occupies only 10% of the total heap memory. In this scenario, the total physical memory size is required to reach 90GB.

Thinking deeply about the actual production context, the key point is that "these components are all of the same model", which means that the type of sensor in each component is exactly the same. Creating and saving time series for each sensor in each component is actually a kind of memory redundancy.

A reasonable way is to save precious memory by storing only one copy of the same metadata. This is what measurement templates do.

### Concept

Supported from v0.13

In the actual scenario, many entities collect the same measurements, that is, they have the same measurements name and type. A measurement template can be declared to define the collectable measurements set. Measurement template is hung on any node of the tree data pattern, which means that all entities under the node have the same measurements set.

Currently you can only set one measurement template on a specific path. If there's one measurement template on one node, it will be forbidden to set any measurement template on the ancestors or descendants of this node. An entity will use it's own measurement template or ancestor's measurement template.

In the following chapters of data definition language, data operation language and Java Native Interface, various operations related to measurement template will be introduced one by one.

### Usage

Currently, only Session API supports Measurement Template usage.

* Create a measurement template
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
* Set the device template named 'templateName' at path 'prefixPath'. You should firstly create the template using

``` 

void setSchemaTemplate(String templateName, String prefixPath)

```



For examples of measurement template, you can refer to example/session/src/main/java/org/apache/iotdb/AlignedTimeseriesSessionExample.java