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

package org.apache.iotdb.db.queryengine.plan.planner.logical;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.optimization.OptimizationTestUtil.schemaMap;
import static org.apache.iotdb.db.queryengine.plan.planner.logical.LogicalPlannerTestUtil.parseSQLToPlanNode;
import static org.junit.Assert.fail;

public class DataQueryLogicalPlannerTest {

  @Test
  public void testLastQuery() {
    String sql = "SELECT last * FROM root.sg.** WHERE time > 100 ORDER BY timeseries ASC";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    LastQueryScanNode d1s1 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s1"), null);
    LastQueryScanNode d1s2 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s2"), null);
    LastQueryScanNode d1s3 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s3"), null);
    LastQueryScanNode d2s1 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s1"), null);
    LastQueryScanNode d2s2 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s2"), null);
    LastQueryScanNode d2s4 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s4"), null);
    AlignedLastQueryScanNode d2a =
        new AlignedLastQueryScanNode(
            queryId.genPlanNodeId(), (AlignedPath) schemaMap.get("root.sg.d2.a"), null);

    List<PlanNode> sourceNodeList = Arrays.asList(d1s1, d1s2, d1s3, d2a, d2s1, d2s2, d2s4);
    LastQueryNode lastQueryNode =
        new LastQueryNode(queryId.genPlanNodeId(), sourceNodeList, Ordering.ASC, false);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, lastQueryNode);
  }

  @Test
  public void testLastQuerySortWithLimit() {
    String sql = "SELECT last * FROM root.sg.d1 ORDER BY time DESC LIMIT 1";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    LastQueryScanNode d1s3 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s3"), null);
    LastQueryScanNode d1s1 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s1"), null);
    LastQueryScanNode d1s2 =
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s2"), null);

    List<PlanNode> sourceNodeList = Arrays.asList(d1s3, d1s1, d1s2);
    LastQueryNode lastQueryNode =
        new LastQueryNode(queryId.genPlanNodeId(), sourceNodeList, null, false);
    SortNode sortNode =
        new SortNode(
            queryId.genPlanNodeId(),
            lastQueryNode,
            new OrderByParameter(Collections.singletonList(new SortItem("TIME", Ordering.DESC))));
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), sortNode, 1);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testSimpleRawDataQuery() {
    String sql = "SELECT ** FROM root.sg.d2 WHERE time > 100 LIMIT 10 OFFSET 10";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s4"),
            Ordering.ASC));
    sourceNodeList.add(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            (AlignedPath) schemaMap.get("root.sg.d2.a"),
            Ordering.ASC,
            true));

    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), fullOuterTimeJoinNode, 10);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 10);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testRawDataQuery() {
    String sql =
        "SELECT s1 FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC LIMIT 100 OFFSET 100 SLIMIT 1 SOFFSET 1";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList);

    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));

    // Duplicate timeFilter has been removed, here only valueFilters are kept
    LogicAndExpression predicate = new LogicAndExpression(valueFilter1, valueFilter2);

    FilterNode filterNode =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode,
            new Expression[] {new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))},
            predicate,
            false,
            Ordering.DESC,
            true);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), filterNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testRawDataQueryAlignByDevice() {
    String sql =
        "SELECT * FROM root.sg.* WHERE time > 100 and s1 > 10 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s3"),
            Ordering.DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode1 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    GreaterThanExpression predicate1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
            new ConstantOperand(TSDataType.INT64, "10"));

    FilterNode filterNode1 =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s3")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))
            },
            predicate1,
            false,
            Ordering.DESC,
            true);

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s4"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode2 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    GreaterThanExpression predicate2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
            new ConstantOperand(TSDataType.INT32, "10"));

    FilterNode filterNode2 =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s4"))
            },
            predicate2,
            false,
            Ordering.DESC,
            true);

    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), Arrays.asList(2, 3, 4));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.DESC))),
            Arrays.asList(ColumnHeaderConstant.DEVICE, "s3", "s1", "s2", "s4"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), filterNode1);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), filterNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testSimpleAggregationQuery() {
    String sql =
        "SELECT last_value(s1), first_value(s1), sum(s2) FROM root.sg.** WHERE time > 100 LIMIT 10 OFFSET 10";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.ASC));
    sourceNodeList.add(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            ((AlignedPath) schemaMap.get("root.sg.d2.a")),
            Ordering.ASC,
            false));

    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC, sourceNodeList);

    RawDataAggregationNode rawDataAggregationNode =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    TAggregationType.FIRST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    TAggregationType.SUM.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    TAggregationType.FIRST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.FIRST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.SUM.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.SUM.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            null,
            Ordering.ASC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), rawDataAggregationNode, 10);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 10);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testAggregationQueryWithoutValueFilter() {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.** WHERE time > 100 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));
    sourceNodeList.add(
        new AlignedSeriesScanNode(
            queryId.genPlanNodeId(),
            (AlignedPath) schemaMap.get("root.sg.d2.a"),
            Ordering.DESC,
            false));

    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList);

    RawDataAggregationNode rawDataAggregationNode =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            null,
            Ordering.DESC);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            queryId.genPlanNodeId(),
            Collections.singletonList(rawDataAggregationNode),
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s1")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))),
                    1,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s1")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s2")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2"))),
                    1,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s2")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s1")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))),
                    1,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s1"))))),
            null,
            Ordering.DESC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testAggregationQueryWithoutValueFilterAlignByDevice() {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode1 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    RawDataAggregationNode rawDataAggregationNode1 =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode1,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            null,
            Ordering.DESC);

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode2 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    RawDataAggregationNode rawDataAggregationNode2 =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode2,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            null,
            Ordering.DESC);

    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), Arrays.asList(1, 2, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.DESC))),
            Arrays.asList(
                ColumnHeaderConstant.DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), rawDataAggregationNode1);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), rawDataAggregationNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testAggregationQueryWithValueFilter() {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList);

    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression predicate = new LogicAndExpression(valueFilter1, valueFilter2);
    FilterNode filterNode =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate,
            false,
            Ordering.DESC,
            true);

    RawDataAggregationNode aggregationNode =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            filterNode,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            null,
            Ordering.DESC);

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            queryId.genPlanNodeId(),
            Collections.singletonList(aggregationNode),
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s1")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s2")))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.*.s1"))))),
            null,
            Ordering.DESC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testAggregationQueryWithValueFilterAlignByDevice() {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
    // fake initResultNodeContext()
    queryId.genPlanNodeId();

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Ordering.DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode1 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    GreaterThanExpression predicate1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT64, "10"));

    FilterNode filterNode1 =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate1,
            false,
            Ordering.DESC,
            true);

    RawDataAggregationNode aggregationNode1 =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            filterNode1,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            null,
            Ordering.DESC);

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Ordering.DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Ordering.DESC));

    FullOuterTimeJoinNode fullOuterTimeJoinNode2 =
        new FullOuterTimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    GreaterThanExpression predicate2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));

    FilterNode filterNode2 =
        new FilterNode(
            queryId.genPlanNodeId(),
            fullOuterTimeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            },
            predicate2,
            false,
            Ordering.DESC,
            true);

    RawDataAggregationNode aggregationNode2 =
        new RawDataAggregationNode(
            queryId.genPlanNodeId(),
            filterNode2,
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            null,
            Ordering.DESC);

    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), Arrays.asList(1, 2, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(OrderByKey.DEVICE, Ordering.ASC),
                    new SortItem(OrderByKey.TIME, Ordering.DESC))),
            Arrays.asList(
                ColumnHeaderConstant.DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1"), aggregationNode1);
    deviceViewNode.addChildDeviceNode(
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d2"), aggregationNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    PlanNode actualPlan = parseSQLToPlanNode(sql);
    Assert.assertEquals(actualPlan, limitNode);
  }

  @Test
  public void testGroupByTagWithValueFilter() {
    String sql = "select max_value(s1) from root.** where s1>1 group by tags(key1)";
    try {
      parseSQLToPlanNode(sql);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("Only time filters are supported in GROUP BY TAGS query"));
    }
  }

  @Test
  public void testGroupByTagWithIllegalSpecialLimitClause() {
    String[] inputSql =
        new String[] {
          "select max_value(s1) from root.** group by tags(key1) align by device",
          "select max_value(s1) from root.** group by tags(key1) limit 1",
          "select max_value(s1) from root.** group by([0, 10000), 5ms), tags(key1) limit 1 offset 1 slimit 1 soffset 1"
        };
    String[] expectedMsg =
        new String[] {
          "GROUP BY TAGS does not support align by device now",
          "Limit or slimit are not supported yet in GROUP BY TAGS",
          "Limit or slimit are not supported yet in GROUP BY TAGS",
        };
    for (int i = 0; i < inputSql.length; i++) {
      try {
        parseSQLToPlanNode(inputSql[i]);
        fail();
      } catch (Exception e) {
        Assert.assertTrue(inputSql[i], e.getMessage().contains(expectedMsg[i]));
      }
    }
  }

  @Test
  public void testGroupByTagWithDuplicatedAliasWithTagKey() {
    String sql = "select max_value(s1) as key1 from root.** group by tags(key1)";
    try {
      parseSQLToPlanNode(sql);
      fail();
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("Output column is duplicated with the tag key: key1"));
    }
  }
}
