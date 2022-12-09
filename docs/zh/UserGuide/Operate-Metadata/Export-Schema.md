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

## 元数据导出操作

元数据导出操作会以 mlog.bin 和 tlog.txt 的形式将当前 IoTDB 中的存储组、时间序列、元数据模板信息进行归档，并导出到指定目录中。

导出的 mlog.bin 和 tlog.txt 文件可以增量的方式加载到已有元数据的 IoTDB 实例中。

### 使用 SQL 方式导出元数据

元数据导出的 SQL 语句如下所示：
```
EXPORT SCHEMA '<path/dir>' 
```

### 使用脚本方式导出元数据

Linux/MacOS

> ./exportSchema.sh -d /yourpath/data/system/schema -o /yourpath/targetDir

Windows

> ./exportSchema.bat-d /yourpath/data/system/schema -o /yourpath/targetDir

使用脚本方式导出元数据时候，需要指定 IoTDB 的元数据文件目录与导出目标目录：
```
usage: ExportSchema -d <source directory path> -o <target directory path>
       [-help]
 -d <source directory path>   Need to specify a source directory path
 -o <target directory path>   Need to specify a target directory path
 -help,--help                 Display help information
```

### 常见问题

* 找不到或无法加载主类 ExportSchema
    * 可能是由于未设置环境变量 $IOTDB_HOME，请设置环境变量之后重试
* Encounter an error, because: File ... already exist.
    * 目标目录下已有 mlog.bin 或者 tlog.txt 文件，请检查目标目录之后重试
* Encounter an error, because: ... does not exist or is not a directory.
    * 源目录路径不存在或不是目录，请检查源目录之后重试
* Encounter an error, because: ... is not a valid directory.
    * 源目录不是 IoTDB 中的元数据文件目录，请检查目标目录之后重试

## 元数据加载操作

参考 [MLog 加载工具](https://iotdb.apache.org/zh/UserGuide/V0.13.x/Maintenance-Tools/MLogLoad-Tool.html)

