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

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.INTERMEDIATE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;

public class Util {
  private Util() {}

  /**
   * Split AggregationNode into two-stage.
   *
   * @return left of pair is FINAL AggregationNode, right is PARTIAL
   */
  public static Pair<AggregationNode, AggregationNode> split(
      AggregationNode node, SymbolAllocator symbolAllocator, QueryId queryId) {
    Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new LinkedHashMap<>();
    Map<Symbol, AggregationNode.Aggregation> finalAggregation = new LinkedHashMap<>();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation originalAggregation = entry.getValue();
      ResolvedFunction resolvedFunction = originalAggregation.getResolvedFunction();
      Type intermediateType =
          TableBuiltinAggregationFunction.getIntermediateType(
              resolvedFunction.getSignature().getName(),
              resolvedFunction.getSignature().getArgumentTypes());
      Symbol intermediateSymbol =
          symbolAllocator.newSymbol(resolvedFunction.getSignature().getName(), intermediateType);
      // TODO put symbol and its type to TypeProvide or later process: add all map contents of
      // SymbolAllocator to the TypeProvider
      checkState(
          !originalAggregation.getOrderingScheme().isPresent(),
          "Aggregate with ORDER BY does not support partial aggregation");
      intermediateAggregation.put(
          intermediateSymbol,
          new AggregationNode.Aggregation(
              resolvedFunction,
              originalAggregation.getArguments(),
              originalAggregation.isDistinct(),
              originalAggregation.getFilter(),
              originalAggregation.getOrderingScheme(),
              originalAggregation.getMask()));

      // rewrite final aggregation in terms of intermediate function
      finalAggregation.put(
          entry.getKey(),
          new AggregationNode.Aggregation(
              resolvedFunction,
              ImmutableList.of(intermediateSymbol.toSymbolReference()),
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    return new Pair<>(
        new AggregationNode(
            node.getPlanNodeId(),
            null,
            finalAggregation,
            node.getGroupingSets(),
            node.getPreGroupedSymbols(),
            FINAL,
            node.getHashSymbol(),
            node.getGroupIdSymbol()),
        new AggregationNode(
            queryId.genPlanNodeId(),
            null,
            intermediateAggregation,
            node.getGroupingSets(),
            node.getPreGroupedSymbols(),
            node.getStep() == SINGLE ? PARTIAL : INTERMEDIATE,
            node.getHashSymbol(),
            node.getGroupIdSymbol()));
  }

  /**
   * Split AggregationTableScanNode into two-stage.
   *
   * @return left of pair is FINAL AggregationNode, right is PARTIAL AggregationTableScanNode
   */
  public static Pair<AggregationNode, AggregationTableScanNode> split(
      AggregationTableScanNode node, SymbolAllocator symbolAllocator, QueryId queryId) {
    Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new LinkedHashMap<>();
    Map<Symbol, AggregationNode.Aggregation> finalAggregation = new LinkedHashMap<>();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation originalAggregation = entry.getValue();
      ResolvedFunction resolvedFunction = originalAggregation.getResolvedFunction();
      Type intermediateType =
          TableBuiltinAggregationFunction.getIntermediateType(
              resolvedFunction.getSignature().getName(),
              resolvedFunction.getSignature().getArgumentTypes());
      Symbol intermediateSymbol =
          symbolAllocator.newSymbol(resolvedFunction.getSignature().getName(), intermediateType);

      checkState(
          !originalAggregation.getOrderingScheme().isPresent(),
          "Aggregate with ORDER BY does not support partial aggregation");
      intermediateAggregation.put(
          intermediateSymbol,
          new AggregationNode.Aggregation(
              resolvedFunction,
              originalAggregation.getArguments(),
              originalAggregation.isDistinct(),
              originalAggregation.getFilter(),
              originalAggregation.getOrderingScheme(),
              originalAggregation.getMask()));

      // rewrite final aggregation in terms of intermediate function
      finalAggregation.put(
          entry.getKey(),
          new AggregationNode.Aggregation(
              resolvedFunction,
              ImmutableList.of(intermediateSymbol.toSymbolReference()),
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    return new Pair<>(
        new AggregationNode(
            node.getPlanNodeId(),
            null,
            finalAggregation,
            node.getGroupingSets(),
            node.getPreGroupedSymbols(),
            FINAL,
            Optional.empty(),
            node.getGroupIdSymbol()),
        new AggregationTableScanNode(
            queryId.genPlanNodeId(),
            node.getQualifiedObjectName(),
            node.getOutputSymbols(),
            node.getAssignments(),
            ImmutableList.of(),
            node.getIdAndAttributeIndexMap(),
            node.getScanOrder(),
            node.getTimePredicate().orElse(null),
            node.getPushDownPredicate(),
            node.getPushDownLimit(),
            node.getPushDownOffset(),
            node.isPushLimitToEachDevice(),
            node.getProjection(),
            intermediateAggregation,
            node.getGroupingSets(),
            node.getPreGroupedSymbols(),
            PARTIAL,
            node.getGroupIdSymbol()));
  }
}
