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

# 压缩方式

当时间序列写入并按照指定的类型编码为二进制数据后，IoTDB会使用压缩技术对该数据进行压缩，进一步提升空间存储效率。虽然编码和压缩都旨在提升存储效率，但编码技术通常只适合特定的数据类型（如二阶差分编码只适合与INT32或者INT64编码，存储浮点数需要先将他们乘以10m以转换为整数），然后将它们转换为二进制流。压缩方式（SNAPPY）针对二进制流进行压缩，因此压缩方式的使用不再受数据类型的限制。

IoTDB允许在创建一个时间序列的时候指定该列的压缩方式。现阶段IoTDB现在支持的压缩方式有两种：

* UNCOMPRESSED（不压缩）
* SNAPPY压缩

压缩方式的指定语法详见本文[5.4节](../Operation%20Manual/SQL%20Reference.md)。
