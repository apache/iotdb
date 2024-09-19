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

import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.IterativeOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.RuleStatsRecorder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.CanonicalizeExpressions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.InlineProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeFilters;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitOverProjectWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimits;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneAggregationColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneAggregationSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFilterColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneJoinColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneLimitColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOffsetColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOutputSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneProjectColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneSortColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTopKColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughOffset;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughProject;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveDuplicateConditions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveTrivialFilters;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.SimplifyExpressions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public class LogicalOptimizeFactory {

  private final List<PlanOptimizer> planOptimizers;

  public LogicalOptimizeFactory(PlannerContext plannerContext) {
    IrTypeAnalyzer typeAnalyzer = new IrTypeAnalyzer(plannerContext);
    Metadata metadata = plannerContext.getMetadata();
    RuleStatsRecorder ruleStats = new RuleStatsRecorder();

    PlanOptimizer pushPredicateIntoTableScanOptimizer = new PushPredicateIntoTableScan();
    PlanOptimizer transformSortToStreamSortOptimizer = new TransformSortToStreamSort();

    Set<Rule<?>> columnPruningRules =
        ImmutableSet.of(
            new PruneAggregationColumns(),
            // TODO After ValuesNode introduced
            // new RemoveEmptyGlobalAggregation(),
            new PruneAggregationSourceColumns(),
            new PruneFilterColumns(),
            new PruneLimitColumns(),
            new PruneOffsetColumns(),
            new PruneOutputSourceColumns(),
            new PruneProjectColumns(),
            new PruneSortColumns(),
            new PruneTableScanColumns(plannerContext.getMetadata()),
            new PruneTopKColumns(),
            new PruneJoinColumns());
    IterativeOptimizer columnPruningOptimizer =
        new IterativeOptimizer(plannerContext, ruleStats, columnPruningRules);

    //    Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
    //        new PushProjectionThroughUnion(),
    //        new PushProjectionThroughExchange(),
    //        // Dereference pushdown rules
    //        new PushDownDereferencesThroughMarkDistinct(typeAnalyzer),
    //        new PushDownDereferenceThroughProject(typeAnalyzer),
    //        new PushDownDereferenceThroughUnnest(typeAnalyzer),
    //        new PushDownDereferenceThroughSemiJoin(typeAnalyzer),
    //        new PushDownDereferenceThroughJoin(typeAnalyzer),
    //        new PushDownDereferenceThroughFilter(typeAnalyzer),
    //        new ExtractDereferencesFromFilterAboveScan(typeAnalyzer),
    //        new PushDownDereferencesThroughLimit(typeAnalyzer),
    //        new PushDownDereferencesThroughSort(typeAnalyzer),
    //        new PushDownDereferencesThroughAssignUniqueId(typeAnalyzer),
    //        new PushDownDereferencesThroughWindow(typeAnalyzer),
    //        new PushDownDereferencesThroughTopN(typeAnalyzer),
    //        new PushDownDereferencesThroughRowNumber(typeAnalyzer),
    //        new PushDownDereferencesThroughTopNRanking(typeAnalyzer));

    IterativeOptimizer inlineProjectionLimitFiltersOptimizer =
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new InlineProjections(plannerContext),
                new RemoveRedundantIdentityProjections(),
                new MergeFilters(),
                new MergeLimits()));

    Set<Rule<?>> simplifyOptimizerRules =
        ImmutableSet.<Rule<?>>builder()
            .addAll(new SimplifyExpressions(plannerContext, typeAnalyzer).rules())
            .addAll(new RemoveDuplicateConditions(metadata).rules())
            .addAll(new CanonicalizeExpressions(plannerContext, typeAnalyzer).rules())
            .add(new RemoveTrivialFilters())
            .build();
    IterativeOptimizer simplifyOptimizer =
        new IterativeOptimizer(plannerContext, ruleStats, simplifyOptimizerRules);

    Set<Rule<?>> limitPushdownRules =
        ImmutableSet.of(new PushLimitThroughOffset(), new PushLimitThroughProject());
    IterativeOptimizer limitPushdownOptimizer =
        new IterativeOptimizer(plannerContext, ruleStats, limitPushdownRules);

    PlanOptimizer unAliasSymbolReferences =
        new UnaliasSymbolReferences(plannerContext.getMetadata());

    PlanOptimizer transformAggregationToStreamableOptimizer =
        new TransformAggregationToStreamable();

    PlanOptimizer pushAggregationIntoTableScanOptimizer = new PushAggregationIntoTableScan();

    PlanOptimizer pushLimitOffsetIntoTableScanOptimizer = new PushLimitOffsetIntoTableScan();

    IterativeOptimizer topKOptimizer =
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(new MergeLimitWithSort(), new MergeLimitOverProjectWithSort()));

    this.planOptimizers =
        ImmutableList.of(
            new IterativeOptimizer(
                plannerContext,
                ruleStats,
                ImmutableSet.<Rule<?>>builder()
                    .addAll(columnPruningRules)
                    //                    .addAll(projectionPushdownRules)
                    //                    .addAll(new UnwrapRowSubscript().rules())
                    //                    .addAll(new PushCastIntoRow().rules())
                    .addAll(
                        ImmutableSet.of(
                            new MergeFilters(),
                            new InlineProjections(plannerContext),
                            new RemoveRedundantIdentityProjections(),
                            new MergeLimits(),
                            new RemoveTrivialFilters()
                            //                        new RemoveRedundantLimit(),
                            //                        new RemoveRedundantOffset(),
                            //                        new RemoveRedundantSort(),
                            //                        new RemoveRedundantSortBelowLimitWithTies(),
                            //                        new RemoveRedundantTopN(),
                            //                        new RemoveRedundantDistinctLimit(),
                            //                        new ReplaceRedundantJoinWithSource(),
                            //                        new RemoveRedundantJoin(),
                            //                        new ReplaceRedundantJoinWithProject(),
                            //                        new RemoveRedundantExists(),
                            //                        new RemoveRedundantWindow(),
                            //                        new SingleDistinctAggregationToGroupBy(),
                            //                        new MergeLimitWithDistinct(),
                            //                        new PruneCountAggregationOverScalar(metadata),
                            //                        new SimplifyCountOverConstant(plannerContext),
                            //                        new
                            // PreAggregateCaseAggregations(plannerContext, typeAnalyzer)))
                            ))
                    .build()),
            // MergeUnion and related projection pruning rules must run before limit pushdown rules,
            // otherwise
            // an intermediate limit node will prevent unions from being merged later on
            new IterativeOptimizer(
                plannerContext,
                ruleStats,
                ImmutableSet.<Rule<?>>builder()
                    //                    .addAll(projectionPushdownRules)
                    .addAll(columnPruningRules)
                    .addAll(limitPushdownRules)
                    .addAll(
                        ImmutableSet.of(
                            //                        new MergeUnion(),
                            //                        new RemoveEmptyUnionBranches(),
                            new MergeFilters(),
                            new RemoveTrivialFilters(),
                            new MergeLimits(),
                            new InlineProjections(plannerContext),
                            new RemoveRedundantIdentityProjections()))
                    .build()),
            simplifyOptimizer,
            unAliasSymbolReferences,
            columnPruningOptimizer,
            inlineProjectionLimitFiltersOptimizer,
            pushPredicateIntoTableScanOptimizer,
            // redo columnPrune and inlineProjections after pushPredicateIntoTableScan
            columnPruningOptimizer,
            inlineProjectionLimitFiltersOptimizer,
            limitPushdownOptimizer,
            pushLimitOffsetIntoTableScanOptimizer,
            transformAggregationToStreamableOptimizer,
            pushAggregationIntoTableScanOptimizer,
            transformSortToStreamSortOptimizer,
            topKOptimizer);
  }

  public List<PlanOptimizer> getPlanOptimizers() {
    return planOptimizers;
  }
}
