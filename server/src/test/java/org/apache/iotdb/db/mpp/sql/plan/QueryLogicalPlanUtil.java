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

package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.apache.commons.compress.utils.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class generates logical plans for test cases of the query statements. */
public class QueryLogicalPlanUtil {

  // test cases of query statement
  public static final List<String> querySQLs = new ArrayList<>();

  // key: query statement; value: expected logical plan
  public static final Map<String, PlanNode> sqlToPlanMap = new HashMap<>();

  public static final Map<String, MeasurementPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.INT32));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  /* 0. Simple Query */
  static {
    String sql = "SELECT * FROM root.sg.d1 LIMIT 10 OFFSET 10";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("0"),
            schemaMap.get("root.sg.d1.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("1"),
            schemaMap.get("root.sg.d1.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_ASC));
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("2"), OrderBy.TIMESTAMP_ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("3"), timeJoinNode, 10);
    LimitNode limitNode = new LimitNode(new PlanNodeId("4"), offsetNode, 10);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* 1. Raw Data Query */
  static {
    String sql =
        "SELECT s1 FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 SLIMIT 1 SOFFSET 1";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s2"),
            Sets.newHashSet("s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d2.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_3"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimeSeriesOperand(SQLConstant.TIME_PATH),
            new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression expression =
        new LogicAndExpression(
            new LogicAndExpression(timeFilter, valueFilter1),
            new LogicAndExpression(timeFilter, valueFilter2));

    FilterNode filterNode =
        new FilterNode(new PlanNodeId("test_query_4"), timeJoinNode, expression);

    ProjectNode projectNode =
        new ProjectNode(
            new PlanNodeId("test_query_4"), filterNode, Collections.singletonList("root.sg.d2.s1"));

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_5"),
            projectNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_6"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_7"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }

  /* 2. Raw Data Query (align by device) */
  static {
    String sql =
        "SELECT * FROM root.sg.* WHERE time > 100 and s1 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimeSeriesOperand(SQLConstant.TIME_PATH),
            new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression expression =
        new LogicAndExpression(
            new LogicAndExpression(timeFilter, valueFilter1),
            new LogicAndExpression(timeFilter, valueFilter2));

    FilterNode filterNode1 =
        new FilterNode(new PlanNodeId("test_query_6"), timeJoinNode1, expression);
    FilterNode filterNode2 =
        new FilterNode(new PlanNodeId("test_query_7"), timeJoinNode2, expression);

    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("test_query_8"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList("s1", "s2"));
    deviceViewNode.addChildDeviceNode("root.sg.d1", filterNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", filterNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_9"),
            deviceViewNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_10"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_11"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }

  /* 3. Aggregation Query (without value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("test_query_5"),
            sourceNodeList,
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Arrays.asList(
                "count(root.sg.*.s1)", "max_value(root.sg.*.s2)", "last_value(root.sg.*.s1)"));

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_6"),
            groupByLevelNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_7"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_8"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }

  /* 4. Aggregation Query (without value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));
    sourceNodeList2.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("test_query_6"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList("count(s1)", "max_value(s2)", "last_value(s1)"));
    deviceViewNode.addChildDeviceNode("root.sg.d1", timeJoinNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", timeJoinNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_7"),
            deviceViewNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_8"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_9"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }

  /* 5. Aggregation Query (with value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimeSeriesOperand(SQLConstant.TIME_PATH),
            new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression expression =
        new LogicAndExpression(
            new LogicAndExpression(timeFilter, valueFilter1),
            new LogicAndExpression(timeFilter, valueFilter2));
    FilterNode filterNode =
        new FilterNode(new PlanNodeId("test_query_5"), timeJoinNode, expression);

    AggregationNode aggregationNode =
        new AggregationNode(
            new PlanNodeId("test_query_6"),
            Collections.singletonList(filterNode),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")))),
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))));

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("test_query_7"),
            Collections.singletonList(aggregationNode),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Arrays.asList(
                "count(root.sg.*.s1)", "max_value(root.sg.*.s2)", "last_value(root.sg.*.s1)"));

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_8"),
            groupByLevelNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_9"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_10"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }

  /* 6. Aggregation Query (with value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            Sets.newHashSet("s1", "s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    GreaterThanExpression timeFilter =
        new GreaterThanExpression(
            new TimeSeriesOperand(SQLConstant.TIME_PATH),
            new ConstantOperand(TSDataType.INT64, "100"));
    GreaterThanExpression valueFilter1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    GreaterThanExpression valueFilter2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));
    LogicAndExpression expression =
        new LogicAndExpression(
            new LogicAndExpression(timeFilter, valueFilter1),
            new LogicAndExpression(timeFilter, valueFilter2));

    FilterNode filterNode1 =
        new FilterNode(new PlanNodeId("test_query_6"), timeJoinNode1, expression);
    FilterNode filterNode2 =
        new FilterNode(new PlanNodeId("test_query_7"), timeJoinNode2, expression);

    AggregationNode aggregationNode1 =
        new AggregationNode(
            new PlanNodeId("test_query_8"),
            Collections.singletonList(filterNode1),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))));
    AggregationNode aggregationNode2 =
        new AggregationNode(
            new PlanNodeId("test_query_9"),
            Collections.singletonList(filterNode2),
            Arrays.asList(
                new AggregationDescriptor(
                    AggregationType.COUNT,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.LAST_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    AggregationType.MAX_VALUE,
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))));

    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            new PlanNodeId("test_query_10"),
            Arrays.asList(OrderBy.DEVICE_ASC, OrderBy.TIMESTAMP_DESC),
            Arrays.asList("count(s1)", "max_value(s2)", "last_value(s1)"));
    deviceViewNode.addChildDeviceNode("root.sg.d1", aggregationNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", aggregationNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_11"),
            deviceViewNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_12"), filterNullNode, 100);
    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_13"), offsetNode, 100);

    //    querySQLs.add(sql);
    //    sqlToPlanMap.put(sql, limitNode);
  }
}
