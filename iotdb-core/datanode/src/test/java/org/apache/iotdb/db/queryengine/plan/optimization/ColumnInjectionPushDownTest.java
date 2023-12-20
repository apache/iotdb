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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ColumnInjectionPushDownTest {

  private static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));

      MeasurementPath d2s1 = new MeasurementPath("root.sg.d2.a.s1", TSDataType.INT32);
      d2s1.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s1", d2s1);

      AlignedPath aligned_d2s1 =
          new AlignedPath(
              "root.sg.d2.a",
              Collections.singletonList("s1"),
              Collections.singletonList(d2s1.getMeasurementSchema()));
      schemaMap.put("aligned_root.sg.d2.a.s1", aligned_d2s1);

      MeasurementPath d2s2 = new MeasurementPath("root.sg.d2.a.s2", TSDataType.DOUBLE);
      d2s2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s2", d2s2);

      AlignedPath aligned_d2s2 =
          new AlignedPath(
              "root.sg.d2.a",
              Collections.singletonList("s2"),
              Collections.singletonList(d2s2.getMeasurementSchema()));
      schemaMap.put("aligned_root.sg.d2.a.s2", aligned_d2s2);

      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s2", "s1"),
              Arrays.asList(d2s2.getMeasurementSchema(), d2s1.getMeasurementSchema()));
      schemaMap.put("root.sg.d2.a", alignedPath);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    OptimizationTestUtil.checkPushDown(new ColumnInjectionPushDown(), sql, rawPlan, optPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    OptimizationTestUtil.checkCannotPushDown(new ColumnInjectionPushDown(), sql, rawPlan);
  }

  @Test
  public void testPushDownAggregationSourceAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("1", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("1", groupByTimeParameter)
            .fill("2", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .fill("2", FillPolicy.PREVIOUS)
            .getRoot());
  }

  @Test
  public void testPushDownAlignedAggregationSourceAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d2.a group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .alignedAggregationScan(
                "0",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("1", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "0",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d2.a group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .alignedAggregationScan(
                "0",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("1", groupByTimeParameter)
            .fill("2", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "0",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .fill("2", FillPolicy.PREVIOUS)
            .getRoot());
  }

  @Test
  public void testPushDownSlidingWindowAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 5), true);
    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.PARTIAL, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(getAggregationDescriptor(AggregationStep.FINAL, "root.sg.d1.s1"));
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms, 5ms);",
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("1", aggregationDescriptorList2, groupByTimeParameter, false)
            .columnInject("2", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("1", aggregationDescriptorList2, groupByTimeParameter, true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms, 5ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("1", aggregationDescriptorList2, groupByTimeParameter, false)
            .columnInject("2", groupByTimeParameter)
            .fill("3", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("1", aggregationDescriptorList2, groupByTimeParameter, true)
            .fill("3", FillPolicy.PREVIOUS)
            .getRoot());
  }

  @Test
  public void testPushDownRawDataAggregationAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 where s1 > 10 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .filter(
                "1",
                Collections.singletonList(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("10")),
                true)
            .rawDataAggregation("2", aggregationDescriptorList, groupByTimeParameter, true)
            .columnInject("3", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .filter(
                "1",
                Collections.singletonList(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("10")),
                true)
            .rawDataAggregation("2", aggregationDescriptorList, groupByTimeParameter, true)
            .getRoot());
  }

  @Test
  public void testCannotPushDownTimeJoinAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s2"));

    checkCannotPushDown(
        "select __endTime, count(s1), count(s2) from root.sg.d1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .aggregationTimeJoin(
                0,
                Arrays.asList(schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                Arrays.asList(aggregationDescriptorList2, aggregationDescriptorList1),
                groupByTimeParameter)
            .columnInject("3", groupByTimeParameter)
            .getRoot());
    checkCannotPushDown(
        "select __endTime, count(s1), count(s2) from root.sg.d1 group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationTimeJoin(
                0,
                Arrays.asList(schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                Arrays.asList(aggregationDescriptorList2, aggregationDescriptorList1),
                groupByTimeParameter)
            .columnInject("3", groupByTimeParameter)
            .fill("4", FillPolicy.PREVIOUS)
            .getRoot());
  }

  @Test
  public void testPushDownAggregationSourceAlignByDevice() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    List<String> outputColumnNames = Arrays.asList("Device", "__endTime", "count(s1)");
    List<String> devices = Arrays.asList("root.sg.d1", "root.sg.d2.a");
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 2));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1, root.sg.d2.a group by ([0, 100), 10ms) align by device;",
        new TestPlanBuilder()
            .deviceView(
                "4",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "0",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1,
                            groupByTimeParameter,
                            false)
                        .columnInject("1", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "2",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            false)
                        .columnInject("3", groupByTimeParameter)
                        .getRoot()))
            .getRoot(),
        new TestPlanBuilder()
            .deviceView(
                "4",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "0",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1,
                            groupByTimeParameter,
                            true)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "2",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            true)
                        .getRoot()))
            .getRoot());
  }

  @Test
  public void testPushDownSlidingWindowAlignByDevice() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 5), true);

    List<AggregationDescriptor> aggregationDescriptorList1_1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.PARTIAL, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList1_2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.PARTIAL, "root.sg.d2.a.s1"));

    List<AggregationDescriptor> aggregationDescriptorList2_1 =
        Collections.singletonList(getAggregationDescriptor(AggregationStep.FINAL, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2_2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.FINAL, "root.sg.d2.a.s1"));

    List<String> outputColumnNames = Arrays.asList("Device", "__endTime", "count(s1)");
    List<String> devices = Arrays.asList("root.sg.d1", "root.sg.d2.a");
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 2));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1, root.sg.d2.a group by ([0, 100), 10ms, 5ms) align by device;",
        new TestPlanBuilder()
            .deviceView(
                "6",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "0",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1_1,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "1", aggregationDescriptorList2_1, groupByTimeParameter, false)
                        .columnInject("2", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "3",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList1_2,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "4", aggregationDescriptorList2_2, groupByTimeParameter, false)
                        .columnInject("5", groupByTimeParameter)
                        .getRoot()))
            .getRoot(),
        new TestPlanBuilder()
            .deviceView(
                "6",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "0",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1_1,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "1", aggregationDescriptorList2_1, groupByTimeParameter, true)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "3",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList1_2,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "4", aggregationDescriptorList2_2, groupByTimeParameter, true)
                        .getRoot()))
            .getRoot());
  }

  @Test
  public void testPushDownRawDataAggregationAlignByDevice() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);

    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    List<String> outputColumnNames = Arrays.asList("Device", "__endTime", "count(s1)");
    List<String> devices = Arrays.asList("root.sg.d1", "root.sg.d2.a");
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 2));

    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1, root.sg.d2.a where s1 > 10 group by ([0, 100), 10ms) align by device;",
        new TestPlanBuilder()
            .deviceView(
                "8",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .scan("0", schemaMap.get("root.sg.d1.s1"))
                        .filter(
                            "1",
                            Collections.singletonList(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
                            ExpressionFactory.gt(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                                ExpressionFactory.intValue("10")),
                            true)
                        .rawDataAggregation(
                            "2", aggregationDescriptorList1, groupByTimeParameter, true)
                        .columnInject("3", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .scanAligned("4", schemaMap.get("aligned_root.sg.d2.a.s1"))
                        .filter(
                            "5",
                            Collections.singletonList(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1"))),
                            ExpressionFactory.gt(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                                ExpressionFactory.intValue("10")),
                            true)
                        .rawDataAggregation(
                            "6", aggregationDescriptorList2, groupByTimeParameter, true)
                        .columnInject("7", groupByTimeParameter)
                        .getRoot()))
            .getRoot(),
        new TestPlanBuilder()
            .deviceView(
                "8",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .scan("0", schemaMap.get("root.sg.d1.s1"))
                        .filter(
                            "1",
                            Collections.singletonList(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
                            ExpressionFactory.gt(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                                ExpressionFactory.intValue("10")),
                            true)
                        .rawDataAggregation(
                            "2", aggregationDescriptorList1, groupByTimeParameter, true)
                        .getRoot(),
                    new TestPlanBuilder()
                        .scanAligned("4", schemaMap.get("aligned_root.sg.d2.a.s1"))
                        .filter(
                            "5",
                            Collections.singletonList(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1"))),
                            ExpressionFactory.gt(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1")),
                                ExpressionFactory.intValue("10")),
                            true)
                        .rawDataAggregation(
                            "6", aggregationDescriptorList2, groupByTimeParameter, true)
                        .getRoot()))
            .getRoot());
  }

  @Test
  public void testPartialPushDownTimeJoinAlignByDevice() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList1_1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList1_2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s2"));

    List<AggregationDescriptor> aggregationDescriptorList2 =
        Arrays.asList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s2"),
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    List<String> outputColumnNames = Arrays.asList("Device", "__endTime", "count(s1)", "count(s2)");
    List<String> devices = Arrays.asList("root.sg.d1", "root.sg.d2.a");
    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new LinkedHashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 3, 2));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 3, 2));

    checkPushDown(
        "select __endTime, count(s1), count(s2) from root.sg.d1, root.sg.d2.a group by ([0, 100), 10ms) align by device;",
        new TestPlanBuilder()
            .deviceView(
                "6",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationTimeJoin(
                            0,
                            Arrays.asList(
                                schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                            Arrays.asList(
                                aggregationDescriptorList1_2, aggregationDescriptorList1_1),
                            groupByTimeParameter)
                        .columnInject("3", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "4",
                            schemaMap.get("root.sg.d2.a"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            false)
                        .columnInject("5", groupByTimeParameter)
                        .getRoot()))
            .getRoot(),
        new TestPlanBuilder()
            .deviceView(
                "6",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationTimeJoin(
                            0,
                            Arrays.asList(
                                schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                            Arrays.asList(
                                aggregationDescriptorList1_2, aggregationDescriptorList1_1),
                            groupByTimeParameter)
                        .columnInject("3", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "4",
                            schemaMap.get("root.sg.d2.a"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            true)
                        .getRoot()))
            .getRoot());
  }

  private AggregationDescriptor getAggregationDescriptor(AggregationStep step, String path) {
    return new AggregationDescriptor(
        TAggregationType.COUNT.name().toLowerCase(),
        step,
        Collections.singletonList(new TimeSeriesOperand(schemaMap.get(path))),
        new HashMap<>());
  }
}
