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
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.TimeDuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.gt;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.intValue;
import static org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory.timeSeries;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.getAggregationDescriptor;
import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;

public class AggregationPushDownTest {

  @Test
  public void testSingleSourceAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));

    checkPushDown(
        "select count(s1) from root.sg.d1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .rawDataAggregation("1", aggregationDescriptorList, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "2",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .getRoot());
    checkPushDown(
        "select count(s1) from root.sg.d1 where s1 > 1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scan(
                "0",
                schemaMap.get("root.sg.d1.s1"),
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("1")))
            .rawDataAggregation("2", aggregationDescriptorList, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "3",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false,
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                    ExpressionFactory.intValue("1")))
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .rawDataAggregation("1", aggregationDescriptorList, groupByTimeParameter, true)
            .columnInject("2", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationScan(
                "3",
                schemaMap.get("root.sg.d1.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .getRoot());
  }

  @Test
  public void testSingleAlignedSourceAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    checkPushDown(
        "select count(s1) from root.sg.d2.a group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("aligned_root.sg.d2.a.s1"))
            .rawDataAggregation("1", aggregationDescriptorList, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "2",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .getRoot());
    checkPushDown(
        "select __endTime, count(s1) from root.sg.d2.a group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scanAligned("0", schemaMap.get("aligned_root.sg.d2.a.s1"))
            .rawDataAggregation("1", aggregationDescriptorList, groupByTimeParameter, true)
            .columnInject("2", groupByTimeParameter)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "3",
                schemaMap.get("aligned_root.sg.d2.a.s1"),
                aggregationDescriptorList,
                groupByTimeParameter,
                false)
            .columnInject("2", groupByTimeParameter)
            .getRoot());
  }

  @Test
  public void testMultiSourceAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s2"));
    List<AggregationDescriptor> allAggregationDescriptorList =
        new ArrayList<>(aggregationDescriptorList2);
    allAggregationDescriptorList.addAll(aggregationDescriptorList1);

    checkPushDown(
        "select count(s1), count(s2) from root.sg.d1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .fullOuterTimeJoin(
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")))
            .rawDataAggregation("3", allAggregationDescriptorList, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .aggregationTimeJoin(
                4,
                Arrays.asList(schemaMap.get("root.sg.d1.s2"), schemaMap.get("root.sg.d1.s1")),
                Arrays.asList(aggregationDescriptorList2, aggregationDescriptorList1),
                groupByTimeParameter)
            .getRoot());
  }

  @Test
  public void testSlidingWindowAlignByTime() {
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
            .scan("0", schemaMap.get("root.sg.d1.s1"))
            .rawDataAggregation("1", aggregationDescriptorList1, groupByTimeParameter, true)
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
            .slidingWindow("2", aggregationDescriptorList2, groupByTimeParameter, false)
            .columnInject("3", groupByTimeParameter)
            .getRoot());
  }

  @Test
  public void testCannotPushDownTimeJoinAlignByTime() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList =
        Arrays.asList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s2"),
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));

    checkCannotPushDown(
        "select count(s1), count(s2) from root.sg.d1 where s1 > 1 and s2 > 1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .innerTimeJoin(
                "5",
                Ordering.ASC,
                Arrays.asList("0", "1"),
                Arrays.asList(schemaMap.get("root.sg.d1.s1"), schemaMap.get("root.sg.d1.s2")),
                Arrays.asList(
                    ExpressionFactory.gt(
                        ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                        ExpressionFactory.intValue("1")),
                    ExpressionFactory.gt(
                        ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s2")),
                        ExpressionFactory.intValue("1"))))
            .rawDataAggregation("4", aggregationDescriptorList, groupByTimeParameter, false)
            .getRoot());
    checkCannotPushDown(
        "select count(s1), count(s2) from root.sg.d1 where s1 > 1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "5",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "0",
                        schemaMap.get("root.sg.d1.s1"),
                        ExpressionFactory.gt(
                            ExpressionFactory.timeSeries(schemaMap.get("root.sg.d1.s1")),
                            ExpressionFactory.intValue("1")))
                    .getRoot(),
                new TestPlanBuilder().scan("1", schemaMap.get("root.sg.d1.s2")).getRoot())
            .rawDataAggregation("4", aggregationDescriptorList, groupByTimeParameter, false)
            .getRoot());
  }

  @Test
  public void testRemoveProject() {
    GroupByTimeParameter groupByTimeParameter =
        new GroupByTimeParameter(0, 100, new TimeDuration(0, 10), new TimeDuration(0, 10), true);
    List<AggregationDescriptor> aggregationDescriptorList1 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d2.a.s1"));

    checkPushDown(
        "select count(s1) from root.sg.d2.a where s2 > 1 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .scanAligned(
                "0",
                schemaMap.get("root.sg.d2.a"),
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s2")),
                    ExpressionFactory.intValue("1")))
            .project("3", Collections.singletonList("root.sg.d2.a.s1"))
            .rawDataAggregation("2", aggregationDescriptorList1, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .alignedAggregationScan(
                "4",
                schemaMap.get("root.sg.d2.a"),
                aggregationDescriptorList1,
                groupByTimeParameter,
                false,
                ExpressionFactory.gt(
                    ExpressionFactory.timeSeries(schemaMap.get("root.sg.d2.a.s2")),
                    ExpressionFactory.intValue("1")))
            .getRoot());

    List<AggregationDescriptor> aggregationDescriptorList2 =
        Collections.singletonList(
            getAggregationDescriptor(AggregationStep.SINGLE, "root.sg.d1.s1"));
    checkPushDown(
        "select count(s1) from root.sg.d1 where time > 100 and s2 > 10 group by ([0, 100), 10ms);",
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "5",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "1",
                        schemaMap.get("root.sg.d1.s2"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).getRoot())
            .project("6", Collections.singletonList("root.sg.d1.s1"))
            .rawDataAggregation("4", aggregationDescriptorList2, groupByTimeParameter, false)
            .getRoot(),
        new TestPlanBuilder()
            .leftOuterTimeJoin(
                "5",
                Ordering.ASC,
                new TestPlanBuilder()
                    .scan(
                        "1",
                        schemaMap.get("root.sg.d1.s2"),
                        gt(timeSeries(schemaMap.get("root.sg.d1.s2")), intValue("10")))
                    .getRoot(),
                new TestPlanBuilder().scan("0", schemaMap.get("root.sg.d1.s1")).getRoot())
            .rawDataAggregation("4", aggregationDescriptorList2, groupByTimeParameter, false)
            .getRoot());
  }

  private void checkPushDown(String sql, PlanNode rawPlan, PlanNode optPlan) {
    OptimizationTestUtil.checkPushDown(
        Collections.singletonList(new PredicatePushDown()),
        new AggregationPushDown(),
        sql,
        rawPlan,
        optPlan);
  }

  private void checkCannotPushDown(String sql, PlanNode rawPlan) {
    OptimizationTestUtil.checkCannotPushDown(
        Collections.singletonList(new PredicatePushDown()),
        new AggregationPushDown(),
        sql,
        rawPlan);
  }
}
