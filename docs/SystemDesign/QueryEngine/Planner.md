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

# Execution plan generator

* org.apache.iotdb.db.qp.Planner

Transform the syntax tree parsed by SQL into logical plans, logical optimizations, and physical plans.

## SQL  parsing

SQL parsing using Antlr4

* antlr/src/main/antlr4/org/apache/iotdb/db/qp/strategy/SqlBase.g4

`mvn clean compile -pl antlr` 

Generated code location ：antlr/target/generated-sources/antlr4

## Logical plan generator

* org.apache.iotdb.db.qp.strategy.LogicalGenerator

## Logical plan optimizer

There are currently three logical plan optimizers

* org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer

  The path optimizer splices query paths in SQL, interacts with MManager, removes wildcards, and performs path checking.

* org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer

  The predicate de-optimizer removes the non-operators in the predicate logic.

* org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer

  Turn predicates into disjunctive normal form.

* org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer

  Combine predicates of the same path logically.

## Physical plan generator

* org.apache.iotdb.db.qp.strategy.PhysicalGenerator

