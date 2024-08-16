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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitOverProjectWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFilterColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneLimitColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOffsetColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOutputSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneProjectColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneSortColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTopKColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughOffset;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughProject;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantIdentityProjections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class LogicalOptimizeFactory {

  private final List<PlanOptimizer> planOptimizers;

  public LogicalOptimizeFactory(PlannerContext plannerContext) {

    PlanOptimizer simplifyExpressionOptimizer = new SimplifyExpressions();
    PlanOptimizer pushPredicateIntoTableScanOptimizer = new PushPredicateIntoTableScan();
    PlanOptimizer transformSortToStreamSortOptimizer = new TransformSortToStreamSort();

    Set<Rule<?>> columnPruningRules =
        ImmutableSet.of(
            new PruneFilterColumns(),
            new PruneLimitColumns(),
            new PruneOffsetColumns(),
            new PruneOutputSourceColumns(),
            new PruneProjectColumns(),
            new PruneSortColumns(),
            new PruneTableScanColumns(plannerContext.getMetadata()),
            new PruneTopKColumns());
    IterativeOptimizer columnPruningOptimizer =
        new IterativeOptimizer(plannerContext, new RuleStatsRecorder(), columnPruningRules);

    IterativeOptimizer inlineProjectionsOptimizer =
        new IterativeOptimizer(
            plannerContext,
            new RuleStatsRecorder(),
            ImmutableSet.of(
                new InlineProjections(plannerContext), new RemoveRedundantIdentityProjections()));

    IterativeOptimizer limitPushdownOptimizer =
        new IterativeOptimizer(
            plannerContext,
            new RuleStatsRecorder(),
            ImmutableSet.of(new PushLimitThroughOffset(), new PushLimitThroughProject()));

    PlanOptimizer pushLimitOffsetIntoTableScanOptimizer = new PushLimitOffsetIntoTableScan();

    IterativeOptimizer topKOptimizer =
        new IterativeOptimizer(
            plannerContext,
            new RuleStatsRecorder(),
            ImmutableSet.of(new MergeLimitWithSort(), new MergeLimitOverProjectWithSort()));

    this.planOptimizers =
        ImmutableList.of(
            simplifyExpressionOptimizer,
            columnPruningOptimizer,
            inlineProjectionsOptimizer,
            pushPredicateIntoTableScanOptimizer,
            // redo columnPrune and inlineProjections after pushPredicateIntoTableScan
            columnPruningOptimizer,
            inlineProjectionsOptimizer,
            limitPushdownOptimizer,
            pushLimitOffsetIntoTableScanOptimizer,
            transformSortToStreamSortOptimizer,
            topKOptimizer);
  }

  public List<PlanOptimizer> getPlanOptimizers() {
    return planOptimizers;
  }
}
