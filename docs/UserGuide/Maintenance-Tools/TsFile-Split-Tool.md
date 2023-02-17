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

TsFile split tool is used to split a TsFile into multiple TsFiles. The location is tools/tsfile/split-tsfile-tool

How to use:

For Windows:

```
.\split-tsfile-tool.bat <path of your TsFile> (-level <inner space compaction num in new file name, default is 10>) (-size <size of new files in byte, default is 1048576000>)
```

For Linux or MacOs:

```
./split-tsfile-tool.sh <path of your TsFile> (-level <inner space compaction num in new file name, default is 10>) (-size <size of new files in byte, default is 1048576000>)
```

> For example, if the new files size is 100MB, and the compaction num is 6, the command is `./split-tsfile-tool.sh test.tsfile -level 6 -size 1048576000` (Linux or MacOs)

Here are some more tips:
1. TsFile split tool is for one closed TsFile, need to ensure this TsFile is closed. If the TsFile is in IoTDB, a `.resource` file represent it is closed.
2. When doing split, make sure the TsFile is not in a running IoTDB.
3. Currently, we do not resolve the corresponding mods file, if you wish to put the new files into the IoTDB data dir and be loaded by restarting, you need to copy the related mods file(if exist) and rename them, make sure each new file has one mods.
4. This tools do not support aligned timeseries currently.