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

# 执行计划生成器 Planner

* org.apache.iotdb.db.qp.Planner

将 SQL 解析出的语法树转化成逻辑计划，逻辑优化，物理计划。

## SQL 解析

SQL 解析采用 Antlr4

* antlr/src/main/antlr4/org/apache/iotdb/db/qp/strategy/SqlBase.g4

`mvn clean compile -pl antlr` 之后生成代码位置：antlr/target/generated-sources/antlr4

## 逻辑计划生成器

* org.apache.iotdb.db.qp.strategy.LogicalGenerator

## 逻辑计划优化器

目前有三种逻辑计划优化器

* org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer

	路径优化器，将 SQL 中的查询路径进行拼接，与 MManager 进行交互去掉通配符，进行路径检查。

* org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer

	谓词去非优化器，将谓词逻辑中的非操作符去掉。

* org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer
	
	将谓词转化为析取范式。
	
* org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer

	将相同路径的谓词逻辑合并。
	
## 物理计划生成器

* org.apache.iotdb.db.qp.strategy.PhysicalGenerator
