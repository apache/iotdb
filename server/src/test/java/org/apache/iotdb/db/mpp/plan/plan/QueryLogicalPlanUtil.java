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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.time.ZonedDateTime;
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

  public static final Map<String, PartialPath> schemaMap = new HashMap<>();

  static {
    try {
      schemaMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d1.s3", new MeasurementPath("root.sg.d1.s3", TSDataType.BOOLEAN));
      schemaMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      schemaMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.d2.s4", new MeasurementPath("root.sg.d2.s4", TSDataType.TEXT));
      schemaMap.put("root.sg.*.s1", new MeasurementPath("root.sg.*.s1", TSDataType.INT32));
      schemaMap.put("root.sg.*.s2", new MeasurementPath("root.sg.*.s2", TSDataType.DOUBLE));
      schemaMap.put("root.sg.*.*.s1", new MeasurementPath("root.sg.*.*.s1", TSDataType.INT32));
      schemaMap.put("root.sg.*.*.s2", new MeasurementPath("root.sg.*.*.s2", TSDataType.DOUBLE));

      MeasurementPath aS1 = new MeasurementPath("root.sg.d2.a.s1", TSDataType.INT32);
      MeasurementPath aS2 = new MeasurementPath("root.sg.d2.a.s2", TSDataType.DOUBLE);
      AlignedPath alignedPath =
          new AlignedPath(
              "root.sg.d2.a",
              Arrays.asList("s1", "s2"),
              Arrays.asList(aS1.getMeasurementSchema(), aS2.getMeasurementSchema()));
      aS1.setUnderAlignedEntity(true);
      aS2.setUnderAlignedEntity(true);
      schemaMap.put("root.sg.d2.a.s1", aS1);
      schemaMap.put("root.sg.d2.a.s2", aS2);
      schemaMap.put("root.sg.d2.a", alignedPath);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  /* Last Query */
  static {
    String sql = "SELECT last * FROM root.sg.** WHERE time > 100 ORDER BY timeseries ASC";

    QueryId queryId = new QueryId("test");
    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s1")));
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s2")));
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d1.s3")));
    sourceNodeList.add(
        new AlignedLastQueryScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath((MeasurementPath) schemaMap.get("root.sg.d2.a.s1"))));
    sourceNodeList.add(
        new AlignedLastQueryScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath((MeasurementPath) schemaMap.get("root.sg.d2.a.s2"))));
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s1")));
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s2")));
    sourceNodeList.add(
        new LastQueryScanNode(
            queryId.genPlanNodeId(), (MeasurementPath) schemaMap.get("root.sg.d2.s4")));

    LastQueryNode lastQueryNode =
        new LastQueryNode(
            queryId.genPlanNodeId(),
            sourceNodeList,
            TimeFilter.gt(100),
            new OrderByParameter(
                Collections.singletonList(new SortItem(SortKey.TIMESERIES, Ordering.ASC))));

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, lastQueryNode);
  }

  /* Simple Query */
  static {
    String sql = "SELECT ** FROM root.sg.d2 WHERE time > 100 LIMIT 10 OFFSET 10";

    QueryId queryId = new QueryId("test");
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
            queryId.genPlanNodeId(), (AlignedPath) schemaMap.get("root.sg.d2.a"), Ordering.ASC));

    for (PlanNode sourceNode : sourceNodeList) {
      if (sourceNode instanceof SeriesScanNode) {
        ((SeriesScanNode) sourceNode).setTimeFilter(TimeFilter.gt(100));
      } else if (sourceNode instanceof AlignedSeriesScanNode) {
        ((AlignedSeriesScanNode) sourceNode).setTimeFilter(TimeFilter.gt(100));
      }
    }

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), timeJoinNode, 10);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 10);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Raw Data Query */
  static {
    String sql =
        "SELECT s1 FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC LIMIT 100 OFFSET 100 SLIMIT 1 SOFFSET 1";

    QueryId queryId = new QueryId("test");
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
    sourceNodeList.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList);

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
            timeJoinNode,
            new Expression[] {new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))},
            predicate,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.DESC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), filterNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Raw Data Query (align by device) */
  static {
    String sql =
        "SELECT * FROM root.sg.* WHERE time > 100 and s1 > 10 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
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
    sourceNodeList1.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    GreaterThanExpression predicate1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
            new ConstantOperand(TSDataType.INT64, "10"));

    FilterNode filterNode1 =
        new FilterNode(
            queryId.genPlanNodeId(),
            timeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s3")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))
            },
            predicate1,
            false,
            ZonedDateTime.now().getOffset(),
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
    sourceNodeList2.add(
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s4"),
            Ordering.DESC));
    sourceNodeList2.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    GreaterThanExpression predicate2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
            new ConstantOperand(TSDataType.INT32, "10"));

    FilterNode filterNode2 =
        new FilterNode(
            queryId.genPlanNodeId(),
            timeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s4"))
            },
            predicate2,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.DESC);

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(2, 3, 4));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, Ordering.DESC))),
            Arrays.asList(ColumnHeaderConstant.DEVICE, "s3", "s1", "s2", "s4"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", filterNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", filterNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Simple Aggregation Query */
  static {
    String sql =
        "SELECT last_value(s1), first_value(s1), sum(s2) FROM root.sg.** WHERE time > 100 LIMIT 10 OFFSET 10";

    QueryId queryId = new QueryId("test");
    List<PlanNode> sourceNodeList = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    try {
      sourceNodeList.add(
          new AlignedSeriesAggregationScanNode(
              queryId.genPlanNodeId(),
              new AlignedPath(
                  "root.sg.d2.a",
                  Arrays.asList("s2", "s1"),
                  ((AlignedPath) schemaMap.get("root.sg.d2.a")).getSchemaList()),
              Arrays.asList(
                  new AggregationDescriptor(
                      TAggregationType.SUM.name().toLowerCase(),
                      AggregationStep.SINGLE,
                      Collections.singletonList(
                          new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2")))),
                  new AggregationDescriptor(
                      TAggregationType.FIRST_VALUE.name().toLowerCase(),
                      AggregationStep.SINGLE,
                      Collections.singletonList(
                          new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))))),
              Ordering.ASC,
              null));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.SUM.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            Ordering.ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.SUM.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Ordering.ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.FIRST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Ordering.ASC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.FIRST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Ordering.ASC,
            null));
    sourceNodeList.add(
        new AlignedSeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            new AlignedPath((MeasurementPath) schemaMap.get("root.sg.d2.a.s1")),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList.forEach(
        node -> {
          if (node instanceof SeriesAggregationScanNode) {
            ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          } else {
            ((AlignedSeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          }
        });

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.ASC, sourceNodeList);
    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), timeJoinNode, 10);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 10);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (without value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.** WHERE time > 100 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    QueryId queryId = new QueryId("test");
    List<PlanNode> sourceNodeList = new ArrayList<>();
    Filter timeFilter = TimeFilter.gt(100);
    try {
      sourceNodeList.add(
          new AlignedSeriesAggregationScanNode(
              queryId.genPlanNodeId(),
              new AlignedPath(
                  "root.sg.d2.a",
                  Arrays.asList("s2", "s1"),
                  ((AlignedPath) schemaMap.get("root.sg.d2.a")).getSchemaList()),
              Arrays.asList(
                  new AggregationDescriptor(
                      TAggregationType.MAX_VALUE.name().toLowerCase(),
                      AggregationStep.PARTIAL,
                      Collections.singletonList(
                          new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2")))),
                  new AggregationDescriptor(
                      TAggregationType.LAST_VALUE.name().toLowerCase(),
                      AggregationStep.PARTIAL,
                      Collections.singletonList(
                          new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1")))),
                  new AggregationDescriptor(
                      TAggregationType.COUNT.name().toLowerCase(),
                      AggregationStep.PARTIAL,
                      Collections.singletonList(
                          new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))))),
              Ordering.DESC,
              null));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            Ordering.DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Ordering.DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.PARTIAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList.forEach(
        node -> {
          if (node instanceof SeriesAggregationScanNode) {
            ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          } else {
            ((AlignedSeriesAggregationScanNode) node).setTimeFilter(timeFilter);
          }
        });

    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            queryId.genPlanNodeId(),
            sourceNodeList,
            Arrays.asList(
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s1"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s1"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s2"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s2"))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s2"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s1"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.a.s1"))),
                    1,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.*.s1")))),
            null,
            Ordering.DESC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (without value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
    Filter timeFilter = TimeFilter.gt(100);
    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2"))))),
            Ordering.DESC,
            null));
    sourceNodeList1.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d1.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList1.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s2"),
            Collections.singletonList(
                new AggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))))),
            Ordering.DESC,
            null));
    sourceNodeList2.add(
        new SeriesAggregationScanNode(
            queryId.genPlanNodeId(),
            (MeasurementPath) schemaMap.get("root.sg.d2.s1"),
            Arrays.asList(
                new AggregationDescriptor(
                    TAggregationType.COUNT.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")))),
                new AggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.SINGLE,
                    Collections.singletonList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1"))))),
            Ordering.DESC,
            null));
    sourceNodeList2.forEach(node -> ((SeriesAggregationScanNode) node).setTimeFilter(timeFilter));

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(2, 1, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(2, 1, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, Ordering.DESC))),
            Arrays.asList(
                ColumnHeaderConstant.DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", timeJoinNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", timeJoinNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (with value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC LIMIT 100 OFFSET 100";

    QueryId queryId = new QueryId("test");
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
    sourceNodeList.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList);

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
            timeJoinNode,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.DESC);

    AggregationNode aggregationNode =
        new AggregationNode(
            queryId.genPlanNodeId(),
            Collections.singletonList(filterNode),
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
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s1"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.MAX_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s2"))),
                new CrossSeriesAggregationDescriptor(
                    TAggregationType.LAST_VALUE.name().toLowerCase(),
                    AggregationStep.FINAL,
                    Arrays.asList(
                        new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
                        new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1"))),
                    2,
                    Collections.emptyMap(),
                    new TimeSeriesOperand(schemaMap.get("root.sg.*.s1")))),
            null,
            Ordering.DESC);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), groupByLevelNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }

  /* Aggregation Query (with value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY DEVICE,TIME DESC LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    QueryId queryId = new QueryId("test");
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
    sourceNodeList1.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList1);

    GreaterThanExpression predicate1 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            new ConstantOperand(TSDataType.INT64, "10"));

    FilterNode filterNode1 =
        new FilterNode(
            queryId.genPlanNodeId(),
            timeJoinNode1,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d1.s2")),
            },
            predicate1,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.DESC);

    AggregationNode aggregationNode1 =
        new AggregationNode(
            queryId.genPlanNodeId(),
            Collections.singletonList(filterNode1),
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
    sourceNodeList2.forEach(
        planNode -> ((SeriesScanNode) planNode).setTimeFilter(TimeFilter.gt(100)));

    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(queryId.genPlanNodeId(), Ordering.DESC, sourceNodeList2);

    GreaterThanExpression predicate2 =
        new GreaterThanExpression(
            new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            new ConstantOperand(TSDataType.INT32, "10"));

    FilterNode filterNode2 =
        new FilterNode(
            queryId.genPlanNodeId(),
            timeJoinNode2,
            new Expression[] {
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s1")),
              new TimeSeriesOperand(schemaMap.get("root.sg.d2.s2")),
            },
            predicate2,
            false,
            ZonedDateTime.now().getOffset(),
            Ordering.DESC);

    AggregationNode aggregationNode2 =
        new AggregationNode(
            queryId.genPlanNodeId(),
            Collections.singletonList(filterNode2),
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

    Map<String, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>();
    deviceToMeasurementIndexesMap.put("root.sg.d1", Arrays.asList(1, 2, 3));
    deviceToMeasurementIndexesMap.put("root.sg.d2", Arrays.asList(1, 2, 3));
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            queryId.genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, Ordering.DESC))),
            Arrays.asList(
                ColumnHeaderConstant.DEVICE, "count(s1)", "max_value(s2)", "last_value(s1)"),
            deviceToMeasurementIndexesMap);
    deviceViewNode.addChildDeviceNode("root.sg.d1", aggregationNode1);
    deviceViewNode.addChildDeviceNode("root.sg.d2", aggregationNode2);

    OffsetNode offsetNode = new OffsetNode(queryId.genPlanNodeId(), deviceViewNode, 100);
    LimitNode limitNode = new LimitNode(queryId.genPlanNodeId(), offsetNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, limitNode);
  }
}
