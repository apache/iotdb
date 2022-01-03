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

# TsFile自检工具
IoTDB Server提供了TsFile自检工具，目前该工具可以检查TsFile文件中的基本格式、TimeseriesMetadata的正确性以及TsFile中各部分存储的Statistics的正确性和一致性。

## 使用
第一步：创建一个TsFileSelfCheckTool类的对象。

``` java
TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
```

第二步：调用自检工具的check方法。第一个参数path是要检测的TsFile的路径。第二个参数是是否只检测TsFile开头和结尾的Magic String和Version Number。

``` java
tool.check(path, false);
```

* check方法的返回值有四种。
* 返回值为-1表示TsFile自检无错。
* 返回值为-2表示TsFile存在Statistics不一致问题。具体会有两种异常，一种是TimeSeriesMetadata的Statistics与其后面的ChunkMetadata的聚合统计的Statistics不一致。另一种是ChunkMetadata的Statistics与其索引的Chunk中的Page聚合统计的Statistics不一致。
* 返回值为-3表示TsFile版本不兼容。
* 返回值为-4表示给定路径不存在TsFile文件。