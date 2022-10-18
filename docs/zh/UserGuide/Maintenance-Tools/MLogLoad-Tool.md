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

## Mlog 加载工具

### 工具介绍

MLogLoad 工具用于将 MLog 中的元数据加载到正在运行的IoTDB中。

### 使用方法

Linux/MacOS

> ./mLogLoad.sh -mlog /yourpath/mlog.bin -tlog /yourpath/tlog.txt -h 127.0.0.1 -p 6667 -u root -pw root

Windows

> ./mLogLoad.bat -mlog /yourpath/mlog.bin -tlog /yourpath/tlog.txt -h 127.0.0.1 -p 6667 -u root -pw root

```
usage: MLogLoad -mlog <mlog file> -tlog <tlog file> [-h <receiver host>]
       [-p <receiver port>] [-u <user>] [-pw <password>] [-help]
 -mlog <mlog file>    Need to specify a binary mlog.bin file to parse
                      (required)
 -tlog <tlog file>    Could specify a binary tlog.txt file to parse, skip
                      tag related metadata if not specify (optional)
 -h <receiver host>   Could specify a specify the receiver host, default
                      is 127.0.0.1 (optional)
 -p <receiver port>   Could specify a specify the receiver port, default
                      is 6667 (optional)
 -u <user>            Could specify the user name, default is root
                      (optional)
 -pw <password>       Could specify the password, default is root
                      (optional)
 -help,--help         Display help information
```

### 使用示例

假定服务器 192.168.0.101:6667 上运行一个 IoTDB 实例，想从将本地的元数据文件 `/yourpath/mlog.bin` 加载进此IoTDB实例。

进入到 mLogLoad.sh 所在文件夹中，执行如下语句：

```
./mLogLoad.sh -f "/yourpath/mlog.bin" -h 192.168.0.101 -p 6667 -u root -pw root
```

等待脚本执行完成之后，可以检查 IoTDB 实例中元数据已经被正确加载。