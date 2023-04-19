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

## TsFile Settle tool
The TsFile Settle tool is used to rewrite one or more TsFiles that have modified record files, and submit the TsFile compaction task by sending an RPC to the DataNode to rewrite the TsFile.
### Usage：
```shell
#MacOs or Linux
./settle-tsfile.sh -h [host] -p [port] -f [filePaths]
# Windows
.\settle-tsfile.bat -h [host] -p [port] -f [filePaths]
```
The host and port parameters are the host and port of the DataNodeInternalRPCService. If not specified, the default values are 127.0.0.1 and 10730 respectively. The filePaths parameter specifies the absolute paths of all TsFiles to be submitted as a compaction task on this DataNode, separated by spaces. Pass in at least one path.
### Example：
```shell
./settle-tsfile.sh -h 127.0.0.1 -p 10730 -f /data/sequence/root.sg/0/0/1672133354759-2-0-0.tsfile /data/sequence/root.sg/0/0/1672306417865-3-0-0.tsfile /data/sequence/root.sg/0/0/1672306417865-3-0-0.tsfile
```
### Requirement：
* Specify at least one TsFile
* All specified TsFiles are in the same space and are continuous, and cross-space compaction is not supported
* The specified file path is the absolute path of the TsFile of the node where the specified DataNode is located
* The specified DataNode is configured to allow the space where the input TsFile is located to perform the compaction
* At least one of the specified TsFiles has a corresponding .mods file