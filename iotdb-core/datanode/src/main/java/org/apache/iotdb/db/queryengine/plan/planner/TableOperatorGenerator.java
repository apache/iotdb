/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

/** This Visitor is responsible for transferring Table PlanNode Tree to Table Operator Tree. */
public class TableOperatorGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  @Override
  public Operator visitTableScan(TableScanNode node, LocalExecutionPlanContext context) {
    return super.visitTableScan(node, context);
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    return super.visitFilter(node, context);
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalExecutionPlanContext context) {
    return super.visitProject(node, context);
  }

  @Override
  public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
    return super.visitLimit(node, context);
  }

  @Override
  public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
    return super.visitOffset(node, context);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    return super.visitMergeSort(node, context);
  }

  @Override
  public Operator visitOutput(OutputNode node, LocalExecutionPlanContext context) {
    return super.visitOutput(node, context);
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    return super.visitSort(node, context);
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    return super.visitTopK(node, context);
  }
}
