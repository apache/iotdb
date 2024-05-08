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

import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.getAggregationDescriptor;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;

public class ColumnInjectionPushDownTest {

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    OptimizationTestUtil.checkPushDown(
        Arrays.asList(new PredicatePushDown(), new AggregationPushDown()),
        new ColumnInjectionPushDown(),
        sql,
        rawPlan,
        optPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    OptimizationTestUtil.checkCannotPushDown(
        Arrays.asList(new PredicatePushDown(), new AggregationPushDown()),
        new ColumnInjectionPushDown(),
        sql,
        rawPlan);
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
                "3",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "3",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationScan(
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .fill("3", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .fill("3", FillPolicy.PREVIOUS)
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
                "3",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "3",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d2.a group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .alignedAggregationScan(
                "4",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .fill("3", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "4",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true)
            .fill("3", FillPolicy.PREVIOUS)
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
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("2", aggregationDescriptorList2, groupByTimeParameter, false)
            .columnInject("3", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("2", aggregationDescriptorList2, groupByTimeParameter, true)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms, 5ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationScan(
                "5",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("2", aggregationDescriptorList2, groupByTimeParameter, false)
            .columnInject("3", groupByTimeParameter)
            .fill("4", FillPolicy.PREVIOUS)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "5",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false)
            .slidingWindow("2", aggregationDescriptorList2, groupByTimeParameter, true)
            .fill("4", FillPolicy.PREVIOUS)
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
            .aggregationScan(
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false,
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("10")))
            .columnInject("3", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "4",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                true,
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("10")))
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
                5,
                Arrays.asList(schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                Arrays.asList(aggregationDescriptorList2, aggregationDescriptorList1),
                groupByTimeParameter)
            .columnInject("4", groupByTimeParameter)
            .getRoot());
    checkCannotPushDown(
        "select __endTime, count(s1), count(s2) from root.sg.d1 group by ([0, 100), 10ms) fill(previous);",
        new TestPlanBuilder()
            .aggregationTimeJoin(
                6,
                Arrays.asList(schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                Arrays.asList(aggregationDescriptorList2, aggregationDescriptorList1),
                groupByTimeParameter)
            .columnInject("4", groupByTimeParameter)
            .fill("5", FillPolicy.PREVIOUS)
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
                "6",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "7",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1,
                            groupByTimeParameter,
                            false)
                        .columnInject("2", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "8",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
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
                        .aggregationScan(
                            "7",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1,
                            groupByTimeParameter,
                            true)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "8",
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
                "8",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationScan(
                            "9",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1_1,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "2", aggregationDescriptorList2_1, groupByTimeParameter, false)
                        .columnInject("3", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "10",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList1_2,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "6", aggregationDescriptorList2_2, groupByTimeParameter, false)
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
                        .aggregationScan(
                            "9",
                            schemaMap.get("root.sg.d1.s1"),
                            aggregationDescriptorList1_1,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "2", aggregationDescriptorList2_1, groupByTimeParameter, true)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "10",
                            schemaMap.get("aligned_root.sg.d2.a.s1"),
                            aggregationDescriptorList1_2,
                            groupByTimeParameter,
                            false)
                        .slidingWindow(
                            "6", aggregationDescriptorList2_2, groupByTimeParameter, true)
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
        "select __endTime, count(s1) from root.sg.d1, root.sg.d2.a where s1 is null group by ([0, 100), 10ms) align by device;",
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
                            ExpressionFactory.isNull(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
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
                            ExpressionFactory.isNull(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1"))),
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
                            ExpressionFactory.isNull(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1"))),
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
                            ExpressionFactory.isNull(
                                ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s1"))),
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
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2.a", Arrays.asList(1, 2, 3));

    checkPushDown(
        "select __endTime, count(s1), count(s2) from root.sg.d1, root.sg.d2.a group by ([0, 100), 10ms) align by device;",
        new TestPlanBuilder()
            .deviceView(
                "8",
                outputColumnNames,
                devices,
                deviceToMeasurementIndexesMap,
                Arrays.asList(
                    new TestPlanBuilder()
                        .aggregationTimeJoin(
                            9,
                            Arrays.asList(
                                schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                            Arrays.asList(
                                aggregationDescriptorList1_2, aggregationDescriptorList1_1),
                            groupByTimeParameter)
                        .project(
                            "12", Arrays.asList("count(root.sg.d1.s1)", "count(root.sg.d1.s2)"))
                        .columnInject("4", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "13",
                            schemaMap.get("desc_root.sg.d2.a"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            false)
                        .project(
                            "14", Arrays.asList("count(root.sg.d2.a.s1)", "count(root.sg.d2.a.s2)"))
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
                        .aggregationTimeJoin(
                            9,
                            Arrays.asList(
                                schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                            Arrays.asList(
                                aggregationDescriptorList1_2, aggregationDescriptorList1_1),
                            groupByTimeParameter)
                        .project(
                            "12", Arrays.asList("count(root.sg.d1.s1)", "count(root.sg.d1.s2)"))
                        .columnInject("4", groupByTimeParameter)
                        .getRoot(),
                    new TestPlanBuilder()
                        .alignedAggregationScan(
                            "13",
                            schemaMap.get("desc_root.sg.d2.a"),
                            aggregationDescriptorList2,
                            groupByTimeParameter,
                            true)
                        .project(
                            "14",
                            Arrays.asList(
                                "__endTime", "count(root.sg.d2.a.s1)", "count(root.sg.d2.a.s2)"))
                        .getRoot()))
            .getRoot());
  }
}
