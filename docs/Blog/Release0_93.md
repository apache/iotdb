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

# Some Notes on Release 0.9.3 and upcoming 0.10.0

*Note:* This Blog Post was initially written by [Jialin Qiao](mailto:qjl16@mails.tsinghua.edu.cn) and has been published here: https://mp.weixin.qq.com/s/MUoUsoRRDUqkQb0-XekXbA.
This Text was translated with [DeepL](www.DeepL.com/Translator) and slightly corrected by [Julian Feinauer](mailto:jfeinauer@pragmaticminds.de).

## Notes on 0.9.3

It's been a long time since I've written an article, I've been working on development and took advantage of this release to write a bit now. The Release 0.9.3 is a minor release with no changes in file structure or RPC and can be upgraded painlessly. In fact, shortly after 0.9.2 was released, a serious bug was found, so the IoTDB Community decided to Release 0.9.3 shortly after.


Let me say a few general things.


The official website of IoTDB has gotten a big upgrade in the last few months, with a much more beautiful interface than before! You can have a look at: http://iotdb.apache.org/. 


The official website has also added Chinese and English design documents, although not yet complete, but basically all the big modules are documented in both languages and the IoTDB Community highly welcomes suggestions how to improve or contributions that do so:

English: http://iotdb.apache.org/SystemDesign/Architecture/Architecture.html
Chinese: http://iotdb.apache.org/zh/SystemDesign/Architecture/Architecture.html


The Release 0.9.3 is a minor/bugfix release in the 0.9 release series that mainly fixes one serious bug: if the wrong metadata operation is performed, such as deleting a non-existent database, the metadata is empty after server restart. This is caused by the metadata module beeing nulled when the metadata log was redone. The fix is to skip the wrong metadata log. In the preparation of version 0.10, we take an execution before logging approach and do not log errors again.

The [issue module](https://github.com/apache/iotdb/issues) is open on Github, so you can ask questions about bugs or new requirements, and we will answer them promptly.


## Fixes in 0.9.3

- [IOTDB-531] Fix the bug that JDBC UTL does not support domain names
- [IOTDB-563] Fix pentaho cannot be downloaded
- [IOTDB-608] Error metadata log skipped on reboot
- [IOTDB-634] Fixes data merge issues when setting the underlying file system to HDFS
- [IOTDB-636] Fix Grafana connector not using correct time granularity
- [IOTDB-528] Adding a downsampling method for the Grafana connector
- [IOTDB-635] When Grafana uses the wrong aggregation for a data type, modify to the generic aggregation function last retry
- Remove the official website about loading external TsFile documentation (this feature is version 0.10, not yet released)

To download and use the new Version of IoTDB go to: https://downloads.apache.org/incubator/iotdb/0.9.3-incubating/apache-iotdb-0.9.3-incubating-bin.zip

User documentation for this release can be found under: http://iotdb.apache.org/UserGuide/Master/0-Get%20Started/1-QuickStart.html

The github repository has a well-established example module that contains sample code for each of the other modules.


## Some previews About upcoming release 0.10.0

The release manual for version 0.9.3 is shorter because many features and improvements were put into version 0.10.0, including query engine upgrades, TsFile structure upgrades, cache optimization, tags, property management, and more. The file structure of 0.10.0 has been fixed and the upgrade tool is almost complete, 0.10.0 will be released soon.