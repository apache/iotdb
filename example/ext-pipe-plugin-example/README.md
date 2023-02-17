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

# How to develop 1 ext-pipe plugin?

## 1. Create 1 new java project, add below maven dependency.

```xml
<dependencies>
    <dependency>
        <artifactId>external-pipe-api</artifactId>
        <groupId>org.apache.iotdb</groupId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

## 2. Develop 2 class to implement below 2 java Interface.

```java
IExternalPipeSinkWriterFactory
IExternalPipeSinkWriter
```

**Note:** Please refer to example codes in **example/ext-pipe-plugin-example** .


## 3. build project and get plugin's  xxx.jar file

```shell
xxx-jar-with-dependencies.jar
```


## 4. install plugin's xxx.jar file to IoTDB

```shell
mkdir -p ext/extPipe
cp xxx-jar-with-dependencies.jar  ext/extPipe
nohup ./A/sbin/start-server.sh >/dev/null 2>&1
```

