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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

/** If it's needed to use RelationalPlanVisitor? */
public abstract class RelationalPlanVisitor<R, C> {

  public abstract R visitPlan(PlanNode node, C context);

  public R visitSingleChildProcess(SingleChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitMultiChildProcess(MultiChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilter(FilterNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitProject(ProjectNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOutput(OutputNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOffset(OffsetNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTableScan(TableScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSort(SortNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTopK(TopKNode node, C context) {
    return visitPlan(node, context);
  }
}
