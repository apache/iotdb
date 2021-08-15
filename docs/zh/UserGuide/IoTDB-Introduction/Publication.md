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

## 研究论文

Apache IoTDB 始于清华大学软件学院。IoTDB 是一个用于管理大量时间序列数据的数据库，它采用了列式存储、数据编码、预计算和索引技术，具有类 SQL 的接口，可支持每秒每节点写入数百万数据点，可以秒级获得超过数万亿个数据点的查询结果。它还可以很容易地与 Apache Hadoop、MapReduce 和 Apache Spark 集成以进行分析。

相关研究论文如下：

* [PISA: An Index for Aggregating Big Time Series Data](https://dl.acm.org/citation.cfm?id=2983775&dl=ACM&coll=DL), Xiangdong Huang and Jianmin Wang and Raymond K. Wong and Jinrui Zhang and Chen Wang. CIKM 2016.
* [Matching Consecutive Subpatterns over Streaming Time Series](https://link.springer.com/chapter/10.1007/978-3-319-96893-3_8), Rong Kang and Chen Wang and Peng Wang and Yuting Ding and Jianmin Wang. APWeb/WAIM 2018.
* [KV-match: A Subsequence Matching Approach Supporting Normalization and Time Warping](https://www.semanticscholar.org/paper/KV-match%3A-A-Subsequence-Matching-Approach-and-Time-Wu-Wang/9ed84cb15b7e5052028fc5b4d667248713ac8592), Jiaye Wu and Peng Wang and Chen Wang and Wei Wang and Jianmin Wang. ICDE 2019.
* [The Design of Apache IoTDB distributed framework](http://ndbc2019.sdu.edu.cn/info/1002/1044.htm), Tianan Li, Jianmin Wang, Xiangdong Huang, Yi Xu, Dongfang Mao, Jun Yuan. NDBC 2019
* [Dual-PISA: An index for aggregation operations on time series data](https://www.sciencedirect.com/science/article/pii/S0306437918305489), Jialin Qiao, Xiangdong Huang, Jianmin Wang, Raymond K Wong. IS 2020

## Benchmark 工具

我们还研发了面向时间序列数据库的 Benchmark 工具： 

https://github.com/thulab/iotdb-benchmark
