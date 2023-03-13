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

## 原生接口写入
原生接口 （Session） 是目前IoTDB使用最广泛的系列接口，包含多种写入接口，适配不同的数据采集场景，性能高效且支持多语言。

### 多语言接口写入
* ### Java
    使用Java接口写入之前，你需要先建立连接，参考 [Java原生接口](../API/Programming-Java-Native-API.md)。
    之后通过 [ JAVA 数据操作接口（DML）](../API/Programming-Java-Native-API.md#数据写入)写入。

* ### Python
    参考 [ Python 数据操作接口（DML）](../API/Programming-Python-Native-API.md#数据写入)

* ### C++ 
    参考 [ C++ 数据操作接口（DML）](../API/Programming-Cpp-Native-API.md)

* ### Go
    参考 [Go 原生接口](../API/Programming-Go-Native-API.md)