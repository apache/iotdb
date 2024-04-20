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

public class Util {
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
