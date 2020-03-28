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

# 调查报告

Apache IoTDB开始于清华大学软件学院。  IoTDB是用于使用列式存储，数据编码，预计算和索引技术管理大量时间序列数据的数据库。 它具有类似于SQL的界面，每个节点每秒可写入数百万个数据点，并且经过优化，可在数秒内获得数万亿个数据点的查询结果。 它还可以轻松地与Apache Hadoop MapReduce和Apache Spark集成以进行分析。

相关的研究论文如下：

* [PISA: An Index for Aggregating Big Time Series Data](https://dl.acm.org/citation.cfm?id=2983775&dl=ACM&coll=DL), Xiangdong Huang and Jianmin Wang and Raymond K. Wong and Jinrui Zhang and Chen Wang. CIKM 2016.
* [Matching Consecutive Subpatterns over Streaming Time Series](https://link.springer.com/chapter/10.1007/978-3-319-96893-3_8), Rong Kang and Chen Wang and Peng Wang and Yuting Ding and Jianmin Wang. APWeb/WAIM 2018.
* [KV-match: A Subsequence Matching Approach Supporting Normalization and Time Warping](https://www.semanticscholar.org/paper/KV-match%3A-A-Subsequence-Matching-Approach-and-Time-Wu-Wang/9ed84cb15b7e5052028fc5b4d667248713ac8592), Jiaye Wu and Peng Wang and Chen Wang and Wei Wang and Jianmin Wang. ICDE 2019.
