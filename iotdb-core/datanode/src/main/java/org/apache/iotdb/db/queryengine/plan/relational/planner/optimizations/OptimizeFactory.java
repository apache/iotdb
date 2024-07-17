/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.IterativeOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.RuleStatsRecorder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.InlineProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFilterColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneLimitColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOffsetColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOutputSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneProjectColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneSortColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantIdentityProjections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class OptimizeFactory {

  private final List<PlanOptimizer> planOptimizers;

  public OptimizeFactory(PlannerContext plannerContext) {

    PlanOptimizer simplifyExpression = new SimplifyExpressions();
    PlanOptimizer pushPredicateIntoTableScan = new PushPredicateIntoTableScan();

    Set<Rule<?>> columnPruningRules =
        ImmutableSet.of(
            new PruneFilterColumns(),
            new PruneLimitColumns(),
            new PruneOffsetColumns(),
            new PruneOutputSourceColumns(),
            new PruneProjectColumns(),
            new PruneSortColumns(),
            new PruneTableScanColumns(plannerContext.getMetadata()));

    Set<Rule<?>> inlineProjections =
        ImmutableSet.of(
            new InlineProjections(plannerContext), new RemoveRedundantIdentityProjections());

    this.planOptimizers =
        ImmutableList.of(
            simplifyExpression,
            new IterativeOptimizer(plannerContext, new RuleStatsRecorder(), columnPruningRules),
            new IterativeOptimizer(plannerContext, new RuleStatsRecorder(), inlineProjections),
            pushPredicateIntoTableScan,
            new IterativeOptimizer(plannerContext, new RuleStatsRecorder(), columnPruningRules),
            new IterativeOptimizer(plannerContext, new RuleStatsRecorder(), inlineProjections));
  }

  public List<PlanOptimizer> getPlanOptimizers() {
    return planOptimizers;
  }
}
