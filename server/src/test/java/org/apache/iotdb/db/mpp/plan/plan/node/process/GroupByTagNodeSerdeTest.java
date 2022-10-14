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
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class GroupByTagNodeSerdeTest {

  @Test
  public void testSerializeAndDeserialize() throws IllegalPathException, IOException {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(1, 100, 1, 1, true, true, true);
    CrossSeriesAggregationDescriptor s1MaxTime =
        new CrossSeriesAggregationDescriptor(
            AggregationType.MAX_TIME.name().toLowerCase(),
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))),
            new FunctionExpression(
                "max_time",
                new LinkedHashMap<>(),
                Collections.singletonList(
                    new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")))));
    CrossSeriesAggregationDescriptor s1Avg =
        new CrossSeriesAggregationDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.FINAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))),
            new FunctionExpression(
                "avg",
                new LinkedHashMap<>(),
                Collections.singletonList(
                    new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")))));
    AggregationDescriptor s1MaxTimePartial =
        new AggregationDescriptor(
            AggregationType.MAX_TIME.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))));
    AggregationDescriptor s1AvgTimePartial =
        new AggregationDescriptor(
            AggregationType.AVG.name().toLowerCase(),
            AggregationStep.PARTIAL,
            Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))));
    SortedMap<List<String>, List<CrossSeriesAggregationDescriptor>>
        tagValuesToAggregationDescriptors = new TreeMap<>(GroupByTagNode::tagValuesComparator);
    tagValuesToAggregationDescriptors.put(
        Arrays.asList("v1", "v2"), Arrays.asList(s1MaxTime, s1Avg));
    GroupByTagNode expectedNode =
        new GroupByTagNode(
            new PlanNodeId("TestGroupByTagNode"),
            Collections.singletonList(
                new SeriesAggregationScanNode(
                    new PlanNodeId("TestSeriesAggregateScanNode1"),
                    new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
                    Arrays.asList(s1MaxTimePartial, s1AvgTimePartial),
                    Ordering.ASC,
                    null,
                    groupByTimeParameter,
                    null)),
            groupByTimeParameter,
            Ordering.ASC,
            Collections.singletonList("k1"),
            tagValuesToAggregationDescriptors,
            Arrays.asList("MAX_TIME(root.sg.d1.s1)", "AVG(root.sg.d1.s1)"));

    ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    expectedNode.serialize(byteBuffer);
    byteBuffer.flip();
    Assert.assertEquals(expectedNode, PlanNodeDeserializeHelper.deserialize(byteBuffer));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    expectedNode.serialize(dataOutputStream);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    Assert.assertEquals(expectedNode, PlanNodeDeserializeHelper.deserialize(buffer));
  }

  @Test
  public void testTagValueComparator() {
    List<Pair<List<String>, List<String>>> testCases =
        Arrays.asList(
            new Pair<>(Arrays.asList("x", "y", "z"), Arrays.asList("A", "B", "C")),
            new Pair<>(Arrays.asList("x", "y", "z"), Arrays.asList("x", null, "z")),
            new Pair<>(Arrays.asList(null, "y", "z"), Arrays.asList("x", null, null)),
            new Pair<>(Arrays.asList(null, null, null), Arrays.asList(null, null, null)),
            new Pair<>(Arrays.asList("x", "y", "z"), Arrays.asList("x", "y", "z")),
            new Pair<>(Arrays.asList("x", "yz"), Arrays.asList("x", "y")));
    int[] expected = new int[] {1, -1, 1, 0, 0, 1};
    for (int i = 0; i < testCases.size(); i++) {
      Pair<List<String>, List<String>> testCase = testCases.get(i);
      if (expected[i] == 0) {
        Assert.assertEquals(
            testCase.toString(),
            expected[i],
            GroupByTagNode.tagValuesComparator(testCase.left, testCase.right));
      } else if (expected[i] > 0) {
        Assert.assertTrue(
            testCase.toString(),
            GroupByTagNode.tagValuesComparator(testCase.left, testCase.right) > 0);
      } else {
        Assert.assertTrue(
            testCase.toString(),
            GroupByTagNode.tagValuesComparator(testCase.left, testCase.right) < 0);
      }
    }
  }
}
