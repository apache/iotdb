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

## TsFile 拆分工具

TsFile 拆分工具用来将一个 TsFile 拆分为多个 TsFile，工具位置为 tools/tsfile/split-tsfile-tool

使用方式：

Windows:

```
.\split-tsfile-tool.bat <TsFile 文件路径> (-level <新生成文件名的空间内合并次数，默认为10>) (-size <新生成文件的大小（字节），默认为 1048576000>)
```


Linux or MacOs:

```
./split-tsfile-tool.sh <TsFile 文件路径> (-level <新生成文件名的空间内合并次数，默认为10>) (-size <新生成文件的大小（字节），默认为 1048576000>)
```

> 例如，需要指定生成 100MB 的文件，且空间内合并次数为 6，则命令为 `./split-tsfile-tool.sh test.tsfile -level 6 -size 1048576000` (Linux or MacOs)

使用拆分工具需要注意如下事项：

1. 拆分工具针对单个已经封口的 TsFile 进行操作，需要确保此 TsFile 已经封口，如 TsFile 在 IoTDB 内，则需要有对应的 `.resource` 文件。
2. 拆分过程需确保文件已经从 IoTDB 中卸载。
3. 目前未处理 TsFile 对应的 mods 文件，如果希望拆分后继续放入 IoTDB 目录中通过重启加载，需要手动将 mods 文件拷贝多份，并修改命名，为每个新生成的文件配备一个 mods 文件。
4. 拆分工具目前尚不支持保存对齐时间序列的 TsFile。
