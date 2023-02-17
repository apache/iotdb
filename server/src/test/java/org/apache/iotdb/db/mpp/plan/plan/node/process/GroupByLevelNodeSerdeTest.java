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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class GroupByLevelNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException, IOException {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(1, 100, 1, 1, true, true, true);
    SeriesAggregationScanNode seriesAggregationScanNode1 =
        new SeriesAggregationScanNode(
            new PlanNodeId("TestSeriesAggregateScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_TIME.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
            Ordering.ASC,
            null,
            groupByTimeParameter,
            null);
    SeriesAggregationScanNode seriesAggregationScanNode2 =
        new SeriesAggregationScanNode(
            new PlanNodeId("TestSeriesAggregateScanNode"),
            new MeasurementPath("root.sg.d2.s1", TSDataType.INT32),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_TIME.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d2.s1"))))),
            Ordering.ASC,
            null,
            groupByTimeParameter,
            null);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("TestGroupByLevelNode"),
            Arrays.asList(seriesAggregationScanNode1, seriesAggregationScanNode2),
            Collections.singletonList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_TIME.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")),
                        new TimeSeriesOperand(new PartialPath("root.sg.d2.s1"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(new PartialPath("root.sg.*.s1")))),
            groupByTimeParameter,
            Ordering.ASC);

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    groupByLevelNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), groupByLevelNode);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    groupByLevelNode.serialize(dataOutputStream);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    assertEquals(PlanNodeDeserializeHelper.deserialize(buffer), groupByLevelNode);
  }
}
