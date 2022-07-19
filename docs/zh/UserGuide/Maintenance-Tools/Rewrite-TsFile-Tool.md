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

# IoTDB Rewrite-TsFile Tool

## 工具介绍

Rewrite-TsFile工具用于将TsFile中的数据写入正在运行的IoTDB中。

## 使用方法

若您在Windows环境中，请运行rewrite-tsfile.bat，若为Linux或Unix，请运行rewrite-tsfile.sh

```bash
./rewrite-tsfile.bat -f filePath [-h host] [-help] [-p port] [-pw password] -u user
-f 待加载的文件或文件夹路径，必要字段
-h IoTDB的Host地址，可选，默认127.0.0.1
-help 输出帮助菜单，可选
-p IoTDB的端口，可选，默认6667
-pw IoTDB登录密码，可选，默认root
-u IoTDb登录用户名，必要字段
```

## 使用范例

假定服务器192.168.0.101:6667上运行一个IoTDB实例，想从将本地保存的TsFile备份文件夹D:\IoTDB\data中的所有的TsFile文件都加载进此IoTDB实例。

首先移动到rewrite-tsfile.bat所在文件夹中，打开命令行，然后执行

```bash
./rewrite-tsfile.bat -f "D:\IoTDB\data" -h 192.168.0.101 -p 6667 -u root -pw root
```

等待脚本执行完成之后，可以检查IoTDB实例中数据已经被正确加载

## 常见问题

- 找不到或无法加载主类RewriteTsFileTool
  - 可能是由于未设置环境变量$IOTDB_HOME，请设置环境变量之后重试
- Missing require argument: f或Missing require argument: u
  - 输入命令缺少待-f字段（加载文件或文件夹路径），或者缺少-u字段（用户名），请添加之后重新执行
- 执行到中途崩溃了想重新加载怎么办
  - 最简单的办法，您重新执行刚才的命令，重新加载数据不会影响加载之后的正确性
  - 如果您想避免重新加载已经加载完成的文件来节省时间，您可以将上一次执行日志显示已经加载完成的TsFile从待加载文件夹中去掉，然后重新加载该文件夹
