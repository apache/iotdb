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

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableBuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.INTERMEDIATE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;

public class Util {
  /**
   * Split AggregationNode into two-stage.
   *
   * @return left of pair is FINAL AggregationNode, right is PARTIAL
   */
  public static Pair<AggregationNode, AggregationNode> split(
      AggregationNode node, SymbolAllocator symbolAllocator, QueryId queryId) {
    Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
    Map<Symbol, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation originalAggregation = entry.getValue();
      ResolvedFunction resolvedFunction = originalAggregation.getResolvedFunction();
      List<Type> intermediateTypes =
          TableBuiltinAggregationFunction.getIntermediateTypes(
              resolvedFunction.getSignature().getName(),
              resolvedFunction.getSignature().getReturnType());
      Type intermediateType =
          intermediateTypes.size() == 1
              ? intermediateTypes.get(0)
              : RowType.anonymous(intermediateTypes);
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
    Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
    Map<Symbol, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation originalAggregation = entry.getValue();
      ResolvedFunction resolvedFunction = originalAggregation.getResolvedFunction();
      List<Type> intermediateTypes =
          TableBuiltinAggregationFunction.getIntermediateTypes(
              resolvedFunction.getSignature().getName(),
              resolvedFunction.getSignature().getReturnType());
      Type intermediateType =
          intermediateTypes.size() == 1
              ? intermediateTypes.get(0)
              : RowType.anonymous(intermediateTypes);
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
  //    public static final Analysis ANALYSIS = constructAnalysis();

  //    public static Analysis constructAnalysis() {
  //        try {
  //            SeriesPartitionExecutor executor =
  //                    SeriesPartitionExecutor.getSeriesPartitionExecutor(
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  //            Analysis analysis = new Analysis();
  //
  //            String device1 = "root.sg.d1";
  //            String device2 = "root.sg.d22";
  //            String device3 = "root.sg.d333";
  //            String device4 = "root.sg.d4444";
  //            String device5 = "root.sg.d55555";
  //            String device6 = "root.sg.d666666";
  //
  //            TRegionReplicaSet dataRegion1 =
  //                    new TRegionReplicaSet(
  //                            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
  //                            Arrays.asList(
  //                                    genDataNodeLocation(11, "192.0.1.1"),
  // genDataNodeLocation(12, "192.0.1.2")));
  //
  //            DataPartition dataPartition =
  //                    new DataPartition(
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  //
  //            Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot,
  // List<TRegionReplicaSet>>>>
  //                    dataPartitionMap = new HashMap<>();
  //            Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
  // sgPartitionMap =
  //                    new HashMap<>();
  //
  //            List<TRegionReplicaSet> d1DataRegions = new ArrayList<>();
  //            d1DataRegions.add(dataRegion1);
  //            Map<TTimePartitionSlot, List<TRegionReplicaSet>> d1DataRegionMap = new HashMap<>();
  //            d1DataRegionMap.put(new TTimePartitionSlot(), d1DataRegions);
  //
  //
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device1), d1DataRegionMap);
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device2), d2DataRegionMap);
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device3), d3DataRegionMap);
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device4), d4DataRegionMap);
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device5), d5DataRegionMap);
  //            sgPartitionMap.put(executor.getSeriesPartitionSlot(device6), d6DataRegionMap);
  //
  //            dataPartitionMap.put("root.sg", sgPartitionMap);
  //
  //            dataPartition.setDataPartitionMap(dataPartitionMap);
  //
  //            analysis.setDataPartitionInfo(dataPartition);
  //
  //            // construct AggregationExpression for GroupByLevel
  //            Map<String, Set<Expression>> aggregationExpression = new HashMap<>();
  //            Set<Expression> s1Expression = new HashSet<>();
  //            s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")));
  //            s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s1")));
  //            s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s1")));
  //            s1Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s1")));
  //
  //            Set<Expression> s2Expression = new HashSet<>();
  //            s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")));
  //            s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d22.s2")));
  //            s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d333.s2")));
  //            s2Expression.add(new TimeSeriesOperand(new PartialPath("root.sg.d4444.s2")));
  //
  //            aggregationExpression.put("root.sg.*.s1", s1Expression);
  //            aggregationExpression.put("root.sg.*.s2", s2Expression);
  //            // analysis.setAggregationExpressions(aggregationExpression);
  //
  //            // construct schema partition
  //            SchemaPartition schemaPartition =
  //                    new SchemaPartition(
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
  //
  // IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());
  //            Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap =
  //                    new HashMap<>();
  //
  //            TRegionReplicaSet schemaRegion1 =
  //                    new TRegionReplicaSet(
  //                            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 11),
  //                            Arrays.asList(
  //                                    genDataNodeLocation(11, "192.0.1.1"),
  // genDataNodeLocation(12, "192.0.1.2")));
  //
  //            TRegionReplicaSet schemaRegion2 =
  //                    new TRegionReplicaSet(
  //                            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 21),
  //                            Arrays.asList(
  //                                    genDataNodeLocation(21, "192.0.2.1"),
  // genDataNodeLocation(22, "192.0.2.2")));
  //
  //            Map<TSeriesPartitionSlot, TRegionReplicaSet> schemaRegionMap = new HashMap<>();
  //            schemaRegionMap.put(executor.getSeriesPartitionSlot(device1), schemaRegion1);
  //            schemaRegionMap.put(executor.getSeriesPartitionSlot(device2), schemaRegion2);
  //            schemaRegionMap.put(executor.getSeriesPartitionSlot(device3), schemaRegion2);
  //            schemaPartitionMap.put("root.sg", schemaRegionMap);
  //            schemaPartition.setSchemaPartitionMap(schemaPartitionMap);
  //
  //            analysis.setDataPartitionInfo(dataPartition);
  //            analysis.setSchemaPartitionInfo(schemaPartition);
  //            analysis.setSchemaTree(genSchemaTree());
  //            // to avoid some special case which is not the point of test
  //            analysis.setStatement(Mockito.mock(QueryStatement.class));
  //            Mockito.when(analysis.getStatement().isQuery()).thenReturn(false);
  //            return analysis;
  //        } catch (IllegalPathException e) {
  //            return new Analysis();
  //        }
  //    }

}
