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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.EvaluateEmptyIntersect;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementExceptAll;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementExceptDistinctAsUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementIntersectAll;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementIntersectDistinctAsUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementPatternRecognition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.ImplementTableFunctionSource;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.InlineProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeExcept;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeFilters;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeIntersect;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitOverProjectWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitWithSort;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimits;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.OptimizeRowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneAggregationColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneAggregationSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneApplyColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneApplyCorrelation;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneApplySourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneAssignUniqueIdColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneCorrelatedJoinColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneCorrelatedJoinCorrelation;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneDistinctAggregation;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneEnforceSingleRowColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneExceptSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneExplainAnalyzeColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFillColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneFilterColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneGapFillColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneIntersectSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneJoinChildrenColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneJoinColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneLimitColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneMarkDistinctColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOffsetColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneOutputSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PrunePatternRecognitionSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneProjectColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneSortColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableFunctionProcessorColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableFunctionProcessorSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTableScanColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneTopKColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneUnionColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneUnionSourceColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PruneWindowColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughOffset;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughProject;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushLimitThroughUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushProjectionThroughUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushTopKThroughUnion;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveDuplicateConditions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveEmptyExceptBranches;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveEmptyUnionBranches;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantEnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantExists;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveTrivialFilters;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.RemoveUnreferencedScalarSubqueries;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.SimplifyCountOverConstant;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.SimplifyExpressions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithoutProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithoutProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithoutProjection;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedJoinToJoin;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformExistsApplyToCorrelatedJoin;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.TransformUncorrelatedSubqueryToJoin;

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

    Set<Rule<?>> columnPruningRules =
        ImmutableSet.of(
            new PruneAggregationColumns(),
            // TODO After ValuesNode introduced
            // new RemoveEmptyGlobalAggregation(),
            new PruneAggregationSourceColumns(),
            new PruneApplyColumns(),
            new PruneApplyCorrelation(),
            new PruneApplySourceColumns(),
            new PruneAssignUniqueIdColumns(),
            new PruneCorrelatedJoinColumns(),
            new PruneCorrelatedJoinCorrelation(),
            new PruneEnforceSingleRowColumns(),
            new PruneExceptSourceColumns(),
            new PruneFilterColumns(),
            new PruneIntersectSourceColumns(),
            new PruneGapFillColumns(),
            new PruneFillColumns(),
            new PruneLimitColumns(),
            new PruneMarkDistinctColumns(),
            new PruneOffsetColumns(),
            new PruneOutputSourceColumns(),
            new PruneExplainAnalyzeColumns(),
            new PruneProjectColumns(),
            new PruneSortColumns(),
            new PruneTableFunctionProcessorColumns(),
            new PruneTableFunctionProcessorSourceColumns(),
            new PruneTableScanColumns(plannerContext.getMetadata()),
            new PruneTopKColumns(),
            new PruneWindowColumns(),
            new PruneJoinColumns(),
            new PruneJoinChildrenColumns(),
            new PrunePatternRecognitionSourceColumns(),
            new PruneUnionColumns(),
            new PruneUnionSourceColumns());
    IterativeOptimizer columnPruningOptimizer =
        new IterativeOptimizer(plannerContext, ruleStats, columnPruningRules);

    Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(new PushProjectionThroughUnion());
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
        ImmutableSet.of(
            new PushLimitThroughOffset(),
            new PushLimitThroughProject(),
            new PushLimitThroughUnion());

    ImmutableList.Builder<PlanOptimizer> optimizerBuilder = ImmutableList.builder();

    optimizerBuilder.add(
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.<Rule<?>>builder()
                .addAll(new CanonicalizeExpressions(plannerContext, typeAnalyzer).rules())
                .add(new OptimizeRowPattern())
                .add(new ImplementPatternRecognition())
                .build()),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.<Rule<?>>builder()
                .addAll(columnPruningRules)
                .addAll(projectionPushdownRules)
                // addAll(newUnwrapRowSubscript().rules()).
                // addAll(new PushCastIntoRow().rules())
                .addAll(
                    ImmutableSet.of(
                        new ImplementTableFunctionSource(),
                        new RemoveEmptyUnionBranches(),
                        new EvaluateEmptyIntersect(),
                        new RemoveEmptyExceptBranches(),
                        new MergeFilters(),
                        new InlineProjections(plannerContext),
                        new RemoveRedundantIdentityProjections(),
                        new MergeUnion(),
                        new MergeLimits(),
                        new RemoveTrivialFilters(),
                        //                        new RemoveRedundantLimit(),
                        //                        new RemoveRedundantOffset(),
                        //                        new RemoveRedundantSort(),
                        //                        new RemoveRedundantSortBelowLimitWithTies(),
                        //                        new RemoveRedundantTopN(),
                        //                        new RemoveRedundantDistinctLimit(),
                        //                        new ReplaceRedundantJoinWithSource(),
                        //                        new RemoveRedundantJoin(),
                        //                        new ReplaceRedundantJoinWithProject(),
                        new RemoveRedundantEnforceSingleRowNode(),
                        new RemoveRedundantExists(),
                        //                        new RemoveRedundantWindow(),
                        new SingleDistinctAggregationToGroupBy(),
                        // Our AggregationPushDown does not support AggregationNode with distinct,
                        // so there is no need to put it after AggregationPushDown,
                        // put it here to avoid extra ColumnPruning.
                        new MultipleDistinctAggregationToMarkDistinct(),
                        //                        new MergeLimitWithDistinct(),
                        //                        new PruneCountAggregationOverScalar(metadata),
                        new SimplifyCountOverConstant(plannerContext)
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
                .addAll(projectionPushdownRules)
                .addAll(columnPruningRules)
                .addAll(limitPushdownRules)
                .addAll(
                    ImmutableSet.of(
                        new RemoveEmptyUnionBranches(),
                        new EvaluateEmptyIntersect(),
                        new RemoveEmptyExceptBranches(),
                        new MergeUnion(),
                        new MergeFilters(),
                        new RemoveTrivialFilters(),
                        new MergeLimits(),
                        new InlineProjections(plannerContext),
                        new RemoveRedundantIdentityProjections()))
                .build()),
        simplifyOptimizer,
        new UnaliasSymbolReferences(plannerContext.getMetadata()),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.<Rule<?>>builder()
                .addAll(
                    ImmutableSet.of(
                        new MergeUnion(),
                        new MergeIntersect(),
                        new MergeExcept(),
                        new PruneDistinctAggregation()))
                .build()),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.<Rule<?>>builder()
                .add(
                    new ImplementIntersectDistinctAsUnion(metadata),
                    new ImplementExceptDistinctAsUnion(metadata),
                    new ImplementIntersectAll(metadata),
                    new ImplementExceptAll(metadata))
                .build()),
        columnPruningOptimizer,
        inlineProjectionLimitFiltersOptimizer,
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(new TransformExistsApplyToCorrelatedJoin(plannerContext))),
        new TransformQuantifiedComparisonApplyToCorrelatedJoin(metadata),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new RemoveRedundantEnforceSingleRowNode(),
                new RemoveUnreferencedScalarSubqueries(),
                new TransformUncorrelatedSubqueryToJoin(),
                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                new TransformCorrelatedJoinToJoin(plannerContext),
                new TransformCorrelatedGlobalAggregationWithProjection(plannerContext),
                new TransformCorrelatedGlobalAggregationWithoutProjection(plannerContext),
                new TransformCorrelatedDistinctAggregationWithProjection(plannerContext),
                new TransformCorrelatedDistinctAggregationWithoutProjection(plannerContext),
                new TransformCorrelatedGroupedAggregationWithProjection(plannerContext),
                new TransformCorrelatedGroupedAggregationWithoutProjection(plannerContext))),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new RemoveUnreferencedScalarApplyNodes(),
                //                            new TransformCorrelatedInPredicateToJoin(metadata), //
                // must be run after columnPruningOptimizer
                new TransformCorrelatedScalarSubquery(
                    metadata), // must be run after TransformCorrelatedAggregation rules
                new TransformCorrelatedJoinToJoin(plannerContext))),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new InlineProjections(plannerContext), new RemoveRedundantIdentityProjections()
                /*new TransformCorrelatedSingleRowSubqueryToProject(),
                new RemoveAggregationInSemiJoin())*/ )),
        new CheckSubqueryNodesAreRewritten(),
        new IterativeOptimizer(
            plannerContext, ruleStats, ImmutableSet.of(new PruneDistinctAggregation())),
        simplifyOptimizer,
        new PushPredicateIntoTableScan(plannerContext, typeAnalyzer),
        // Currently, we inline symbols but do not simplify them in predicate push down.
        // So we have to add extra simplifyOptimizer here
        simplifyOptimizer,
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new RemoveEmptyUnionBranches(),
                new EvaluateEmptyIntersect(),
                new RemoveEmptyExceptBranches()
                // Currently, Distinct is not supported, so we cant use this rule for now.
                // new TransformFilteringSemiJoinToInnerJoin()
                )),

        // redo columnPrune and inlineProjections after pushPredicateIntoTableScan
        columnPruningOptimizer,
        inlineProjectionLimitFiltersOptimizer,
        new IterativeOptimizer(plannerContext, ruleStats, limitPushdownRules),
        new PushLimitOffsetIntoTableScan(),
        new TransformAggregationToStreamable(),
        new PushAggregationIntoTableScan(),
        new TransformSortToStreamSort(),
        new IterativeOptimizer(
            plannerContext,
            ruleStats,
            ImmutableSet.of(
                new MergeLimitWithSort(),
                new MergeLimitOverProjectWithSort(),
                new PushTopKThroughUnion())),
        new ParallelizeGrouping());

    this.planOptimizers = optimizerBuilder.build();
  }

  public List<PlanOptimizer> getPlanOptimizers() {
    return planOptimizers;
  }
}
