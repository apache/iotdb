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

0.12 版本的 IoTDB 会产生很大的文件，在运维过程中分析起来比较困难。因此，从 0.12.5 版本和 0.13 版本起，提供TsFile 分离工具，该工具可以将大的 TsFile 文件根据配置项拆分为数个小文件。该启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-{version}\tools\tsfileToolSet` 目录中。

使用方式：

Windows:

```
.\split-tsfile-tool.bat <TsFile 文件路径> (-level <所拆分文件的层级>) (-size <所拆分文件的大小>)
```


Linux or MacOs:

```
./split-tsfile-tool.sh <TsFile 文件路径> (-level <所拆分文件的层级>) (-size <所拆分文件的大小>)
```
> 注意：如果不传入`-level`，所拆分文件的层级为 10；如果不传入`-size`，所拆分文件的大小约为 1GB；`-size` 后参数单位为 byte。
> 例如，需要指定拆分为 100MB 的文件，且文件层级数为6，则命令为 `./split-tsfile-tool.sh test.tsfile -level 6 -size 1048576000` (Linux or MacOs)

拆分中可以调节的配置项如下：

1. 拆分所生成的文件大小通过命令传入参数确定的，默认为 1GB。这个配置项同样也是 0.13 版本中合并所能生成文件的目标大小。在 0.13 版本中，文件是否可以合并是通过文件大小确定的，可以通过此配置项控制重启后不继续合并。
2. 文件所在层级是通过命令传入参数确定的，默认为 10。在 0.12 版本中，文件是否可以合并是通过文件所在层级确定的，可以通过此配置项控制重启后不继续合并。
3. 文件中 chunk 点数可以通过 `chunk_point_num_lower_bound_in_compaction` 进行配置，默认为 100。这个配置项同样也是 0.13 版本中合并所能生成文件的 chunk 中点数。

使用拆分工具需要注意如下事项：

1. 拆分工具为离线运维工具，使用前需关闭 IoTDB，确保所拆分的文件已经完全落盘（即有`tsFile.resource`文件）。拆分后需移除原文件后重启。
2. 拆分工具目前尚不支持拆分带有删除区间的 TsFile（即有`.mods`文件）和写有对齐时间序列的 TsFile。
