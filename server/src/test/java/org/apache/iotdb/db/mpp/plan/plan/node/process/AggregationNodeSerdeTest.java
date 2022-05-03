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
package org.apache.iotdb.db.mpp.plan.plan.node.process;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import org.apache.commons.compress.utils.Sets;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.iotdb.tsfile.read.filter.factory.FilterType.VALUE_FILTER;
import static org.junit.Assert.assertEquals;

public class AggregationNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(1, 100, 1, 1, false, false, false);
    SeriesAggregationScanNode seriesAggregationScanNode =
        new SeriesAggregationScanNode(
            new PlanNodeId("TestSeriesAggregateScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.BOOLEAN),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_TIME,
                    AggregationStep.INTERMEDIATE,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
            Sets.newHashSet("s1"),
            OrderBy.TIMESTAMP_ASC,
            new In<>(Sets.newHashSet("s1", "s2"), VALUE_FILTER, true),
            groupByTimeParameter,
            new TRegionReplicaSet(
                new TConsensusGroupId(TConsensusGroupType.DataRegion, 1), new ArrayList<>()));
    AggregationNode aggregationNode =
        new AggregationNode(
            new PlanNodeId("TestAggregateNode"),
            Collections.singletonList(seriesAggregationScanNode),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_TIME,
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
            groupByTimeParameter);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    aggregationNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), aggregationNode);
  }
}
