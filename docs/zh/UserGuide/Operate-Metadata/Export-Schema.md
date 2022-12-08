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

元数据导出操作会以 mlog.bin 和 tlog.txt 的形式将当前 IoTDB 中的存储组、时间序列、元数据模板信息导出到指定目录中。

导出的 mlog.bin 和 tlog.txt 文件可以增量的方式加载到已有元数据的 IoTDB 实例中。

### 元数据导出方式

元数据导出的 SQL 语句如下所示：
```
EXPORT SCHEMA '<path/dir>' 
```

### 元数据加载方式

参考 [MLog 加载工具](https://iotdb.apache.org/zh/UserGuide/V0.13.x/Maintenance-Tools/MLogLoad-Tool.html)

