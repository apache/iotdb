/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import java.util.List;
import java.util.Map;

public class TestPlanBuilder {

  private PlanNode root;

  public TestPlanBuilder() {}

  public PlanNode getRoot() {
    return root;
  }

  public TestPlanBuilder output(String id, List<String> columnNames, List<Symbol> outputSymbols) {
    this.root = new OutputNode(new PlanNodeId(id), this.root, columnNames, outputSymbols);
    return this;
  }

  public TestPlanBuilder limit(String id, long count) {
    this.root = new LimitNode(new PlanNodeId(id), this.root, count, null);
    return this;
  }

  public TestPlanBuilder offset(String id, long count) {
    this.root = new OffsetNode(new PlanNodeId(id), this.root, count);
    return this;
  }

  public TestPlanBuilder project(String id, Assignments assignments) {
    this.root = new ProjectNode(new PlanNodeId(id), this.root, assignments);
    return this;
  }

  public TestPlanBuilder filter(String id, Expression predicate) {
    this.root = new FilterNode(new PlanNodeId(id), this.root, predicate);
    return this;
  }

  public TestPlanBuilder deviceTableScan(
      String id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<DeviceEntry> deviceEntries,
      Map<Symbol, Integer> idAndAttributeIndexMap,
      Ordering scanOrder,
      Expression timePredicate,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      boolean pushLimitToEachDevice) {
    this.root =
        new DeviceTableScanNode(
            new PlanNodeId(id),
            qualifiedObjectName,
            outputSymbols,
            assignments,
            deviceEntries,
            idAndAttributeIndexMap,
            scanOrder,
            timePredicate,
            pushDownPredicate,
            pushDownLimit,
            pushDownOffset,
            pushLimitToEachDevice);
    return this;
  }
}
