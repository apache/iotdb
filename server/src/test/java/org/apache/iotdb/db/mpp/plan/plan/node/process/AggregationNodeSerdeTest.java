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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import org.apache.commons.compress.utils.Sets;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                    AggregationType.MAX_TIME.name().toLowerCase(),
                    AggregationStep.INTERMEDIATE,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
            Ordering.ASC,
            new In<>(Sets.newHashSet("s1", "s2"), VALUE_FILTER, true),
            groupByTimeParameter,
            null);
    AggregationNode aggregationNode =
        new AggregationNode(
            new PlanNodeId("TestAggregateNode"),
            Collections.singletonList(seriesAggregationScanNode),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_TIME.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
            groupByTimeParameter,
            Ordering.ASC);

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    aggregationNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), aggregationNode);
  }

  /** Test AVG with COUNT and SUM (former or later). */
  @Test
  public void getDeduplicatedDescriptorsTest1() {
    PartialPath seriesPath1 = new PartialPath(new String[] {"root", "sg", "d1", "s1"});
    List<AggregationType> aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.COUNT);
    aggregationTypeList.add(AggregationType.AVG);
    aggregationTypeList.add(AggregationType.SUM);
    List<AggregationDescriptor> descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    List<AggregationDescriptor> deduplicatedDescriptors =
        AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.AVG, deduplicatedDescriptors.get(0).getAggregationType());

    aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.COUNT);
    aggregationTypeList.add(AggregationType.SUM);
    aggregationTypeList.add(AggregationType.AVG);
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.AVG, deduplicatedDescriptors.get(0).getAggregationType());

    aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.AVG);
    aggregationTypeList.add(AggregationType.COUNT);
    aggregationTypeList.add(AggregationType.SUM);
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.AVG, deduplicatedDescriptors.get(0).getAggregationType());

    // try output final
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.SINGLE,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(3, deduplicatedDescriptors.size());
    assertEquals(AggregationType.AVG, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.COUNT, deduplicatedDescriptors.get(1).getAggregationType());
    assertEquals(AggregationType.SUM, deduplicatedDescriptors.get(2).getAggregationType());
  }

  /** Test FIRST_VALUE with MIN_TIME (former or later). */
  @Test
  public void getDeduplicatedDescriptorsTest2() {
    PartialPath seriesPath1 = new PartialPath(new String[] {"root", "sg", "d1", "s1"});
    List<AggregationType> aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.FIRST_VALUE);
    aggregationTypeList.add(AggregationType.MIN_TIME);
    List<AggregationDescriptor> descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    List<AggregationDescriptor> deduplicatedDescriptors =
        AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.FIRST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());

    aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.MIN_TIME);
    aggregationTypeList.add(AggregationType.FIRST_VALUE);
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.FIRST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());

    // try output final
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.SINGLE,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(2, deduplicatedDescriptors.size());
    assertEquals(AggregationType.MIN_TIME, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.FIRST_VALUE, deduplicatedDescriptors.get(1).getAggregationType());
  }

  /** Test LAST_VALUE with MAX_TIME (former or later). */
  @Test
  public void getDeduplicatedDescriptorsTest3() {
    PartialPath seriesPath1 = new PartialPath(new String[] {"root", "sg", "d1", "s1"});
    List<AggregationType> aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.LAST_VALUE);
    aggregationTypeList.add(AggregationType.MAX_TIME);
    List<AggregationDescriptor> descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    List<AggregationDescriptor> deduplicatedDescriptors =
        AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.LAST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());

    aggregationTypeList = new ArrayList<>();
    aggregationTypeList.add(AggregationType.MAX_TIME);
    aggregationTypeList.add(AggregationType.LAST_VALUE);
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.PARTIAL,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(1, deduplicatedDescriptors.size());
    assertEquals(AggregationType.LAST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());

    // try output final
    descriptorList = new ArrayList<>();
    for (AggregationType aggregationType : aggregationTypeList) {
      descriptorList.add(
          new AggregationDescriptor(
              aggregationType.name().toLowerCase(),
              AggregationStep.SINGLE,
              Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    }
    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(2, deduplicatedDescriptors.size());
    assertEquals(AggregationType.MAX_TIME, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.LAST_VALUE, deduplicatedDescriptors.get(1).getAggregationType());
  }

  /** Test AVG with COUNT but work on different time series. */
  @Test
  public void getDeduplicatedDescriptorsTest4() {
    PartialPath seriesPath1 = new PartialPath(new String[] {"root", "sg", "d1", "s1"});
    PartialPath seriesPath2 = new PartialPath(new String[] {"root", "sg", "d1", "s2"});
    List<AggregationDescriptor> descriptorList = new ArrayList<>();
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.COUNT.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath2))));

    List<AggregationDescriptor> deduplicatedDescriptors =
        AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(2, deduplicatedDescriptors.size());
    assertEquals(AggregationType.AVG, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.COUNT, deduplicatedDescriptors.get(1).getAggregationType());

    descriptorList = new ArrayList<>();
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.FIRST_VALUE.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.MIN_TIME.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath2))));

    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(2, deduplicatedDescriptors.size());
    assertEquals(AggregationType.FIRST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.MIN_TIME, deduplicatedDescriptors.get(1).getAggregationType());

    descriptorList = new ArrayList<>();
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.LAST_VALUE.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath1))));
    descriptorList.add(
        new AggregationDescriptor(
            AggregationType.MAX_TIME.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(seriesPath2))));

    deduplicatedDescriptors = AggregationNode.getDeduplicatedDescriptors(descriptorList);
    assertEquals(2, deduplicatedDescriptors.size());
    assertEquals(AggregationType.LAST_VALUE, deduplicatedDescriptors.get(0).getAggregationType());
    assertEquals(AggregationType.MAX_TIME, deduplicatedDescriptors.get(1).getAggregationType());
  }
}
