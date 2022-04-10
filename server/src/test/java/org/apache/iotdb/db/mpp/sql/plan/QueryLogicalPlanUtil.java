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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.apache.commons.compress.utils.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_3"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    IExpression leftExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d1.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d2.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression expression = BinaryExpression.and(leftExpression, rightExpression);

    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("test_query_4"),
            timeJoinNode,
            expression,
            Collections.singletonList("root.sg.d2.s1"));

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_5"),
            filterNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_6"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_7"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
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
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    IExpression leftExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d1.s1"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d2.s1"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression expression = BinaryExpression.and(leftExpression, rightExpression);

    List<String> outputColumnNames =
        Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.s1", "root.sg.d2.s2");
    FilterNode filterNode1 =
        new FilterNode(
            new PlanNodeId("test_query_6"), timeJoinNode1, expression, outputColumnNames);
    FilterNode filterNode2 =
        new FilterNode(
            new PlanNodeId("test_query_7"), timeJoinNode2, expression, outputColumnNames);

    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(new PlanNodeId("test_query_8"), OrderBy.TIMESTAMP_DESC);
    deviceMergeNode.addChildDeviceNode("root.sg.d1", filterNode1);
    deviceMergeNode.addChildDeviceNode("root.sg.d2", filterNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_9"),
            deviceMergeNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_10"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_11"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
  }

  /* 3. Aggregation Query (without value filter) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "GROUP BY LEVEL = 1 ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    List<AggregationType> aggregationTypeList1 =
        Arrays.asList(AggregationType.COUNT, AggregationType.LAST_VALUE);
    List<AggregationType> aggregationTypeList2 =
        Collections.singletonList(AggregationType.MAX_VALUE);
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            aggregationTypeList1,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            aggregationTypeList2,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            aggregationTypeList1,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            aggregationTypeList2,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    int[] groupByLevels = new int[] {1};
    Map<String, String> groupedPathMap = new HashMap<>();
    groupedPathMap.put("count(root.sg.d2.s1)", "count(root.sg.*.s1)");
    groupedPathMap.put("count(root.sg.d1.s1)", "count(root.sg.*.s1)");
    groupedPathMap.put("max_value(root.sg.d1.s2)", "max_value(root.sg.*.s2)");
    groupedPathMap.put("max_value(root.sg.d2.s2)", "max_value(root.sg.*.s2)");
    groupedPathMap.put("last_value(root.sg.d2.s1)", "last_value(root.sg.*.s1)");
    groupedPathMap.put("last_value(root.sg.d1.s1)", "last_value(root.sg.*.s1)");
    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("test_query_5"), timeJoinNode, groupByLevels, groupedPathMap);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_6"),
            groupByLevelNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_7"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_8"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
  }

  /* 4. Aggregation Query (without value filter and align by device) */
  static {
    String sql =
        "SELECT count(s1), max_value(s2), last_value(s1) FROM root.sg.* WHERE time > 100 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    List<AggregationType> aggregationTypeList1 =
        Arrays.asList(AggregationType.COUNT, AggregationType.LAST_VALUE);
    List<AggregationType> aggregationTypeList2 =
        Collections.singletonList(AggregationType.MAX_VALUE);
    Filter timeFilter = TimeFilter.gt(100);
    sourceNodeList1.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_0"),
            schemaMap.get("root.sg.d1.s1"),
            aggregationTypeList1,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList1.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            aggregationTypeList2,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList2.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            aggregationTypeList1,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));
    sourceNodeList2.add(
        new SeriesAggregateScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            aggregationTypeList2,
            OrderBy.TIMESTAMP_DESC,
            timeFilter));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(new PlanNodeId("test_query_6"), OrderBy.TIMESTAMP_DESC);
    deviceMergeNode.addChildDeviceNode("root.sg.d1", timeJoinNode1);
    deviceMergeNode.addChildDeviceNode("root.sg.d2", timeJoinNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_7"),
            deviceMergeNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_8"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_9"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
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
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    IExpression leftExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d1.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d2.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression expression = BinaryExpression.and(leftExpression, rightExpression);
    List<String> outputColumnNames =
        Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.s1", "root.sg.d2.s2");
    FilterNode filterNode =
        new FilterNode(new PlanNodeId("test_query_5"), timeJoinNode, expression, outputColumnNames);

    Map<PartialPath, Set<AggregationType>> aggregateFuncMap = new HashMap<>();
    aggregateFuncMap.put(
        schemaMap.get("root.sg.d1.s1"),
        Sets.newHashSet(AggregationType.COUNT, AggregationType.LAST_VALUE));
    aggregateFuncMap.put(
        schemaMap.get("root.sg.d1.s2"), Sets.newHashSet(AggregationType.MAX_VALUE));
    aggregateFuncMap.put(
        schemaMap.get("root.sg.d2.s1"),
        Sets.newHashSet(AggregationType.COUNT, AggregationType.LAST_VALUE));
    aggregateFuncMap.put(
        schemaMap.get("root.sg.d2.s2"), Sets.newHashSet(AggregationType.MAX_VALUE));
    AggregateNode aggregateNode =
        new AggregateNode(
            new PlanNodeId("test_query_6"),
            aggregateFuncMap,
            Collections.singletonList(filterNode));

    int[] groupByLevels = new int[] {1};
    Map<String, String> groupedPathMap = new HashMap<>();
    groupedPathMap.put("count(root.sg.d2.s1)", "count(root.sg.*.s1)");
    groupedPathMap.put("count(root.sg.d1.s1)", "count(root.sg.*.s1)");
    groupedPathMap.put("max_value(root.sg.d1.s2)", "max_value(root.sg.*.s2)");
    groupedPathMap.put("max_value(root.sg.d2.s2)", "max_value(root.sg.*.s2)");
    groupedPathMap.put("last_value(root.sg.d2.s1)", "last_value(root.sg.*.s1)");
    groupedPathMap.put("last_value(root.sg.d1.s1)", "last_value(root.sg.*.s1)");
    GroupByLevelNode groupByLevelNode =
        new GroupByLevelNode(
            new PlanNodeId("test_query_5"), aggregateNode, groupByLevels, groupedPathMap);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_7"),
            groupByLevelNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_8"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_9"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
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
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"),
            schemaMap.get("root.sg.d1.s2"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"),
            schemaMap.get("root.sg.d2.s1"),
            OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"),
            schemaMap.get("root.sg.d2.s2"),
            OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    IExpression leftExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d1.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            schemaMap.get("root.sg.d2.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression expression = BinaryExpression.and(leftExpression, rightExpression);

    List<String> outputColumnNames =
        Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2", "root.sg.d2.s1", "root.sg.d2.s2");
    FilterNode filterNode1 =
        new FilterNode(
            new PlanNodeId("test_query_6"), timeJoinNode1, expression, outputColumnNames);
    FilterNode filterNode2 =
        new FilterNode(
            new PlanNodeId("test_query_7"), timeJoinNode2, expression, outputColumnNames);

    Map<PartialPath, Set<AggregationType>> aggregateFuncMap1 = new HashMap<>();
    Map<PartialPath, Set<AggregationType>> aggregateFuncMap2 = new HashMap<>();
    aggregateFuncMap1.put(
        schemaMap.get("root.sg.d1.s1"),
        Sets.newHashSet(AggregationType.COUNT, AggregationType.LAST_VALUE));
    aggregateFuncMap1.put(
        schemaMap.get("root.sg.d1.s2"), Sets.newHashSet(AggregationType.MAX_VALUE));
    aggregateFuncMap2.put(
        schemaMap.get("root.sg.d2.s1"),
        Sets.newHashSet(AggregationType.COUNT, AggregationType.LAST_VALUE));
    aggregateFuncMap2.put(
        schemaMap.get("root.sg.d2.s2"), Sets.newHashSet(AggregationType.MAX_VALUE));
    AggregateNode aggregateNode1 =
        new AggregateNode(
            new PlanNodeId("test_query_8"),
            aggregateFuncMap1,
            Collections.singletonList(filterNode1));
    AggregateNode aggregateNode2 =
        new AggregateNode(
            new PlanNodeId("test_query_9"),
            aggregateFuncMap2,
            Collections.singletonList(filterNode2));

    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(new PlanNodeId("test_query_10"), OrderBy.TIMESTAMP_DESC);
    deviceMergeNode.addChildDeviceNode("root.sg.d1", aggregateNode1);
    deviceMergeNode.addChildDeviceNode("root.sg.d2", aggregateNode2);

    FilterNullNode filterNullNode =
        new FilterNullNode(
            new PlanNodeId("test_query_11"),
            deviceMergeNode,
            FilterNullPolicy.CONTAINS_NULL,
            new ArrayList<>());

    LimitNode limitNode = new LimitNode(new PlanNodeId("test_query_12"), filterNullNode, 100);
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("test_query_13"), limitNode, 100);

    querySQLs.add(sql);
    sqlToPlanMap.put(sql, offsetNode);
  }
}
