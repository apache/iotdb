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

# 查询引擎

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625242-f648a100-467e-11ea-921c-b954a3ecae7a.png">

## 设计思想

查询引擎负责所有用户命令的解析、生成计划、交给对应的执行器、返回结果集。

## 相关类

* org.apache.iotdb.db.service.TSServiceImpl

	IoTDB 服务器端 RPC 实现，与客户端进行直接交互。
	
* org.apache.iotdb.db.qp.Planner
	
	解析 SQL，生成逻辑计划，逻辑优化，生成物理计划。

* org.apache.iotdb.db.qp.executor.PlanExecutor

	分发物理计划给对应的执行器，主要包括以下四个具体的执行器。
	
	* MManager: 元数据操作
	* StorageEngine: 数据写入
	* QueryRouter: 数据查询
	* LocalFileAuthorizer: 权限操作

* org.apache.iotdb.db.query.dataset.*

	分批构造结果集返回给客户端，包含部分查询逻辑。

## 查询流程

* SQL 解析
* 生成逻辑计划
* 生成物理计划
* 构造结果集生成器
* 分批返回结果集

## 相关文档

* [查询计划生成器](../QueryEngine/Planner.md)
* [计划执行器](../QueryEngine/PlanExecutor.md)