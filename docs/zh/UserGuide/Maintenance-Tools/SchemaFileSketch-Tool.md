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

## PBTreeFile 解析工具

自 1.1 版本起，IoTDB 将每个 database 下序列的元数据存储为 pbtree.pst 文件。

如果需要将该文件转为便于阅读的的格式，可以使用本工具来解析指定 pbtree.pst 。

### 使用方式

Linux/MacOS
> ./print-pbtree-file.sh -f your/path/to/pbtree.pst -o /your/path/to/sketch.txt

Windows

> ./print-pbtree-file.bat -f your/path/to/pbtree.pst -o /your/path/to/sketch.txt
