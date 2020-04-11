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


# version-1 -> version-2

Last Updated on 2019-11-28 by Jialin Qiao.

| PR#   | Name                                                        | Author          | Changes                                                      |
| ---- | ------------------------------------------------------------ | --------------- | ------------------------------------------------------------ |
| 587  | [IOTDB-325] Refactor Statistics                              | qiaojialin      | Move start time, end time, count in PageHeader and ChunkMetadata into Statistics; Remove maxTombstoneTime in ChunkHeader |
| 855  | New TsFile                                                   | HTHou           | Remove ChunkGroupMetadata, store ChunkMetadata list by series, Add TimeseriesMetadata for each series |
| 1024 | [IOTDB-585] Fix recover version bug                          | qiaojialin      | Add MetaMarker.VERSION and version behind each flushing memtable (flushAllChunkGroups) |

# 0.8.0 (version-0) -> version-1

Last Updated on 2019-10-27 by Lei Rui.

| PR#   | Name                                                        | Author          | Changes                                                      |
| ---- | ------------------------------------------------------------ | --------------- | ------------------------------------------------------------ |
| 258  | [IOTDB-143]Development of merge                              | jt2594838       | Add totalChunkNum and invalidChunkNum to TsFileMetaData.     |
| 409  | \[IOTDB-165\]\[TsFile\] Delete a current version and add a number version and an exception. | Genius_pig      | (1) Delete a current version in TsFileMetaData; <br />(2) Change the TsFile magic number from 12 bytes to: 6 bytes magic string ("TsFile") + 6 bytes version number ({"000001", "000002", ""000003"}) ("v0.8.0" is the first version (we treat it as "000000"));<br />(3) The tail of a TsFile only has "TsFile" magic string, without the version number. |
| 466  | [IOTDB-208] Bloom filter                                     | SilverNarcissus | Add four fields for the bloom filter to TsFileMetaData: 1) bit vector byte array length, 2) bit vector byte array, 3) the number of bits, 4) the number of hash functions. |





