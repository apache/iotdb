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

## MLog 解析工具

0.12.x 版本之后，IoTDB 将元数据文件编码成二进制。

如果想要解析元数据成人可读的方式，可以使用本工具来解析指定元数据文件。

该工具可以同时解析 snapshot 文件和 mlog 文件。

### 使用方式

Linux/MacOS
> ./mLogParser.sh -f /your path/mlog.bin -o /your path/mlog.txt

Windows

> .\mLogParser.bat -f \your path\mlog.bin -o \your path\mlog.txt
