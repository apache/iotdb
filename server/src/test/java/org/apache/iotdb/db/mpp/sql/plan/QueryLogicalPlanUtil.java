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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.FilterNullPolicy;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.util.*;

public class QueryLogicalPlanUtil {

  public static final List<String> querySQLs = new ArrayList<>();
  public static final Map<String, PlanNode> sqlToPlanMap = new HashMap<>();

  public static final Map<String, MeasurementPath> pathMap = new HashMap<>();

  static {
    try {
      pathMap.put("root.sg.d1.s1", new MeasurementPath("root.sg.d1.s1", TSDataType.INT32));
      pathMap.put("root.sg.d1.s2", new MeasurementPath("root.sg.d1.s2", TSDataType.INT32));
      pathMap.put("root.sg.d2.s1", new MeasurementPath("root.sg.d2.s1", TSDataType.INT32));
      pathMap.put("root.sg.d2.s2", new MeasurementPath("root.sg.d2.s2", TSDataType.INT32));
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
  }

  static {
    // raw data query
    String sql =
        "SELECT s1 FROM root.sg.* WHERE time > 100 and s2 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 SLIMIT 1 SOFFSET 1";

    List<PlanNode> sourceNodeList = new ArrayList<>();
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"), pathMap.get("root.sg.d1.s2"), OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"), pathMap.get("root.sg.d2.s1"), OrderBy.TIMESTAMP_DESC));
    sourceNodeList.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"), pathMap.get("root.sg.d2.s2"), OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode =
        new TimeJoinNode(new PlanNodeId("test_query_3"), OrderBy.TIMESTAMP_DESC, sourceNodeList);

    IExpression leftExpression =
        new SingleSeriesExpression(
            pathMap.get("root.sg.d1.s2"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            pathMap.get("root.sg.d2.s2"),
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

  static {
    // raw data query (align by device)
    String sql =
        "SELECT * FROM root.sg.* WHERE time > 100 and s1 > 10 "
            + "ORDER BY TIME DESC WITHOUT NULL ANY LIMIT 100 OFFSET 100 ALIGN BY DEVICE";

    List<PlanNode> sourceNodeList1 = new ArrayList<>();
    List<PlanNode> sourceNodeList2 = new ArrayList<>();
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_0"), pathMap.get("root.sg.d1.s1"), OrderBy.TIMESTAMP_DESC));
    sourceNodeList1.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_1"), pathMap.get("root.sg.d1.s2"), OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_2"), pathMap.get("root.sg.d2.s1"), OrderBy.TIMESTAMP_DESC));
    sourceNodeList2.add(
        new SeriesScanNode(
            new PlanNodeId("test_query_3"), pathMap.get("root.sg.d2.s2"), OrderBy.TIMESTAMP_DESC));

    TimeJoinNode timeJoinNode1 =
        new TimeJoinNode(new PlanNodeId("test_query_4"), OrderBy.TIMESTAMP_DESC, sourceNodeList1);
    TimeJoinNode timeJoinNode2 =
        new TimeJoinNode(new PlanNodeId("test_query_5"), OrderBy.TIMESTAMP_DESC, sourceNodeList2);

    IExpression leftExpression =
        new SingleSeriesExpression(
            pathMap.get("root.sg.d1.s1"),
            FilterFactory.and(ValueFilter.gt(10), TimeFilter.gt(100)));
    IExpression rightExpression =
        new SingleSeriesExpression(
            pathMap.get("root.sg.d2.s1"),
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
}
