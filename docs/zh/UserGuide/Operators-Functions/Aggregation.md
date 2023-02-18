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

# 聚合函数

聚合函数是多对一函数。它们对一组值进行聚合计算，得到单个聚合结果。

除了 `COUNT()` 之外，其他所有聚合函数都忽略空值，并在没有输入行或所有值为空时返回空值。 例如，`SUM()` 返回 null 而不是零，而 `AVG()` 在计数中不包括 null 值。

IoTDB 支持的聚合函数如下：

| 函数名      | 功能描述                                                     | 允许的输入类型           | 输出类型       |
| ----------- | ------------------------------------------------------------ | ------------------------ | -------------- |
| SUM         | 求和。                                                       | INT32 INT64 FLOAT DOUBLE | DOUBLE         |
| COUNT       | 计算数据点数。                                               | 所有类型                 | INT            |
| AVG         | 求平均值。                                                   | INT32 INT64 FLOAT DOUBLE | DOUBLE         |
| EXTREME     | 求具有最大绝对值的值。如果正值和负值的最大绝对值相等，则返回正值。 | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| MAX_VALUE   | 求最大值。                                                   | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| MIN_VALUE   | 求最小值。                                                   | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| FIRST_VALUE | 求时间戳最小的值。                                           | 所有类型                 | 与输入类型一致 |
| LAST_VALUE  | 求时间戳最大的值。                                           | 所有类型                 | 与输入类型一致 |
| MAX_TIME    | 求最大时间戳。                                               | 所有类型                 | Timestamp      |
| MIN_TIME    | 求最小时间戳。                                               | 所有类型                 | Timestamp      |