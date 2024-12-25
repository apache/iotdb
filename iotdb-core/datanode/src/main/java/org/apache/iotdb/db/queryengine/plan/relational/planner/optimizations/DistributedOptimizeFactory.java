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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.IterativeOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.RuleStatsRecorder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.EliminateLimitProjectWithTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.EliminateLimitWithTableScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitOverProjectWithMergeSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitWithMergeSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

public class DistributedOptimizeFactory {
  private final List<PlanOptimizer> planOptimizers;

  public DistributedOptimizeFactory(PlannerContext plannerContext) {
    RuleStatsRecorder ruleStats = new RuleStatsRecorder();

    this.planOptimizers =
        ImmutableList.of(
            // transfer Limit+Sort to TopK
            new IterativeOptimizer(
                plannerContext,
                ruleStats,
                ImmutableSet.of(
                    new MergeLimitWithMergeSort(), new MergeLimitOverProjectWithMergeSort())),
            // eliminate unnecessary SortNode
            new SortElimination(),
            // other optimize rules
            new IterativeOptimizer(
                plannerContext,
                ruleStats,
                ImmutableSet.of(
                    new EliminateLimitWithTableScan(), new EliminateLimitProjectWithTableScan())));
  }

  public List<PlanOptimizer> getPlanOptimizers() {
    return planOptimizers;
  }
}
