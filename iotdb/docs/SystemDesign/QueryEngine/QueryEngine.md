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

# QueryEngine

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/thulab/iotdb/files/6087969/query-engine.pdf">

## Design ideas

The query engine is responsible for parsing all user commands, generating plans, delivering them to the corresponding executors, and returning result sets.

## Related classes

* org.apache.iotdb.db.service.TSServiceImpl

  IoTDB server-side RPC implementation, which directly interacts with the client.

* org.apache.iotdb.db.qp.Planner

  Parse SQL, generate logical plans, optimize logical plans, and generate physical plans.

* org.apache.iotdb.db.qp.executor.PlanExecutor

  Distribute the physical plan to the corresponding actuators, including the following four specific actuators.

  * MManager: Metadata operations
  * StorageEngine: Data write
  * QueryRouter: Data query
  * LocalFileAuthorizer: Permission operation

* org.apache.iotdb.db.query.dataset.*

  The batch result set is returned to the client and contains part of the query logic.

## Query process

* SQL parsing
* Generate logical plans
* Generate physical plans
* Constructing a result set generator
* Returning result sets in batches

## Related documents

* [Query Plan Generator](../QueryEngine/Planner.md)
* [PlanExecutor](../QueryEngine/PlanExecutor.md)
