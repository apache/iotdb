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

# TsFile Split Tool

IoTDB version 0.12 could produce large files, which leads to difficulties in maintenance. Therefore, since version 0.12.5 and 0.13, TsFile split tool is provided. The split tool can split the large TsFile into several small TsFile according to the configuration.

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-{version}\tools\tsfileToolSet` directory.

Command:

For Windows:

```
.\split-tsfile-tool.bat <path of your TsFile> (-level <LEVEL of the target files>) (-size <SIZE of the target files>)
```

For Linux or MacOs:

```
./split-tsfile-tool.sh <path of your TsFile> (-level <LEVEL of the target files>) (-size <SIZE of the target files>)
```

> Note that if `-level` is not set, the default level of target files is 10; if `-size` is not set, the default size of target files is about 1GB. The unit of `-size` is byte.
> For example, if the target files size is 100MB, and the level is 6, the command would be `./split-tsfile-tool.sh test.tsfile -level 6 -size 1048576000` (Linux or MacOs)

Here are some configurations:

1. The size of target files could be configured by the input param, which is 1GB by default. This configuration is also the target file size in compaction in 0.13. File size could determine whether the compaction is proceeded in 0.13, so this configuration could make sure there is no compaction after restarting.
2. The level of target files is determined by the input param, which is 10 by default. File level could determine whether the compaction is proceeded in 0.12, so this configuration could make sure there is no compaction after restarting.
3. The points number of chunk could be configured by `chunk_point_num_lower_bound_in_compaction`, which is 100 by default. This configuration is also the points number of target file in compaction in 0.13.

Here are some more tips:
1. TsFile split tool is offline maintenance tool. Before splitting a file, you should make sure the file to be split is closed (aka with `tsFile.resource`) and IoTDB is shut down. After splitting, restart IoTDB.
2. TsFile split tool doesn't support splitting TsFile with deletion interval (aka with `.mods` file) and with aligned timeseries.
