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

## TsFile Settle工具
TsFile Settle工具用于将一个或多个存在修改记录文件的TsFile重写，通过向DataNode发送RPC的方式提交TsFile合并任务来重写TsFile。
### 使用方式：
```shell
#MacOs or Linux
./settle-tsfile.sh -h [host] -p [port] -f [filePaths]
# Windows
.\settle-tsfile.bat -h [host] -p [port] -f [filePaths]
```
其中host和port参数为DataNodeInternalRPCService的host和port，如果不指定默认值分别为127.0.0.1和10730, filePaths参数指定要作为一个compaction任务提交的所有TsFile在此DataNode上的绝对路径，以空格分隔，需要传入至少一个路径。

### 使用示例：
```shell
./settle-tsfile.sh -h 127.0.0.1 -p 10730 -f /data/sequence/root.sg/0/0/1672133354759-2-0-0.tsfile /data/sequence/root.sg/0/0/1672306417865-3-0-0.tsfile /data/sequence/root.sg/0/0/1672306417865-3-0-0.tsfile
```
### 使用要求：
* 最少指定一个TsFile
* 所有指定的TsFile都在同一个空间内且连续，不支持跨空间合并
* 指定的文件路径为指定DataNode所在节点的该TsFile的绝对路径
* 指定的DataNode上配置了允许输入的TsFile所在的空间执行合并操作
* 指定的TsFile中至少有一个存在对应的.mods文件