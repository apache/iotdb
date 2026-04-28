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

package org.apache.iotdb.commons.queryengine.plan.planner.plan.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;

public interface ICoreQueryPlanVisitor<R, C> extends IPlanVisitor<R, C> {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Query Node
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // source --------------------------------------------------------------------------------------

  default R visitSourceNode(SourceNode node, C context) {
    return visitPlan(node, context);
  }

  // single child --------------------------------------------------------------------------------

  default R visitSingleChildProcess(SingleChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  // two child -----------------------------------------------------------------------------------

  default R visitTwoChildProcess(TwoChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  // multi child --------------------------------------------------------------------------------

  default R visitMultiChildProcess(MultiChildProcessNode node, C context) {
    return visitPlan(node, context);
  }

  // =============================== Used for Table Model ====================================
  default R visitFilter(FilterNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitApply(ApplyNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  default R visitAssignUniqueId(AssignUniqueId node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitEnforceSingleRow(EnforceSingleRowNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitCorrelatedJoin(CorrelatedJoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  default R visitProject(ProjectNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitLimit(LimitNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitOffset(OffsetNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitMergeSort(MergeSortNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitOutput(OutputNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitCollect(CollectNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitGapFill(GapFillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitFill(FillNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitPreviousFill(PreviousFillNode node, C context) {
    return visitFill(node, context);
  }

  default R visitLinearFill(LinearFillNode node, C context) {
    return visitFill(node, context);
  }

  default R visitValueFill(ValueFillNode node, C context) {
    return visitFill(node, context);
  }

  default R visitSort(SortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitStreamSort(StreamSortNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitGroup(GroupNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitTopK(TopKNode node, C context) {
    return visitMultiChildProcess(node, context);
  }

  default R visitTopKRanking(TopKRankingNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitRowNumber(RowNumberNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitValuesNode(ValuesNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitJoin(JoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  default R visitSemiJoin(SemiJoinNode node, C context) {
    return visitTwoChildProcess(node, context);
  }

  default R visitGroupReference(GroupReference node, C context) {
    return visitPlan(node, context);
  }

  default R visitAggregation(AggregationNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitMarkDistinct(MarkDistinctNode node, C context) {
    return visitSingleChildProcess(node, context);
  }

  default R visitWindowFunction(WindowNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitTableFunction(TableFunctionNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitTableFunctionProcessor(TableFunctionProcessorNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitPatternRecognition(PatternRecognitionNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitUnion(UnionNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitIntersect(IntersectNode node, C context) {
    return visitPlan(node, context);
  }

  default R visitExcept(ExceptNode node, C context) {
    return visitPlan(node, context);
  }
}
