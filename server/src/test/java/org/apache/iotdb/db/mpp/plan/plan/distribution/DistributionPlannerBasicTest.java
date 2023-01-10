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

package org.apache.iotdb.db.mpp.plan.plan.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DistributionPlannerBasicTest {

  @Test
  public void testSingleSeriesScan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Ordering.ASC);

    Analysis analysis = Util.constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
  }

  @Test
  public void testSingleSeriesScanRewriteSource() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SeriesScanNode root =
        new SeriesScanNode(
            queryId.genPlanNodeId(),
            new MeasurementPath("root.sg.d1.s1", TSDataType.INT32),
            Ordering.ASC);

    Analysis analysis = Util.constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    assertEquals(2, rootAfterRewrite.getChildren().size());
  }

  @Test
  public void testRewriteSourceNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select d1.s1,d1.s2,d22.s1 from root.sg limit 10";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root));
    PlanNode newRoot = planner.rewriteSource();
    assertEquals(4, newRoot.getChildren().get(0).getChildren().size());
  }

  @Test
  public void testRewriteMetaSourceNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    SchemaQueryMergeNode metaMergeNode = new SchemaQueryMergeNode(queryId.genPlanNodeId(), false);
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1.s1"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1.s2"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    metaMergeNode.addChild(
        new TimeSeriesSchemaScanNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d22.s1"),
            null,
            null,
            10,
            0,
            false,
            false,
            false));
    LimitNode root2 = new LimitNode(queryId.genPlanNodeId(), metaMergeNode, 10);
    Analysis analysis = Util.constructAnalysis();
    DistributionPlanner planner2 =
        new DistributionPlanner(
            analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root2));
    PlanNode newRoot2 = planner2.rewriteSource();
    assertEquals(newRoot2.getChildren().get(0).getChildren().size(), 2);
  }

  @Test
  public void testAddExchangeNode() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select d1.s1,d1.s2,d22.s1 from root.sg limit 10";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(new MPPQueryContext(queryId), root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    assertEquals(4, rootWithExchange.getChildren().get(0).getChildren().size());
    int exchangeNodeCount = 0;
    for (PlanNode child : rootWithExchange.getChildren().get(0).getChildren()) {
      exchangeNodeCount += child instanceof ExchangeNode ? 1 : 0;
    }
    assertEquals(2, exchangeNodeCount);
  }

  @Test
  public void testSplitFragment() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select d1.s1,d1.s2,d22.s1 from root.sg limit 10";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    PlanNode rootAfterRewrite = planner.rewriteSource();
    PlanNode rootWithExchange = planner.addExchangeNode(rootAfterRewrite);
    SubPlan subPlan = planner.splitFragment(rootWithExchange);
    assertEquals(subPlan.getChildren().size(), 2);
  }

  @Test
  public void testParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query");
    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());

    String sql = "select d1.s1,d1.s2,d333.s1 from root.sg limit 10";
    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(3, plan.getInstances().size());
  }

  @Test
  public void testSingleAlignedSeries() throws IllegalPathException {
    QueryId queryId = new QueryId("test_query_aligned");
    String sql = "select s1, s2 from root.sg.d666666 limit 10";
    MPPQueryContext context =
        new MPPQueryContext(sql, queryId, null, new TEndPoint(), new TEndPoint());

    Analysis analysis = Util.analyze(sql, context);
    PlanNode root = Util.genLogicalPlan(analysis, context);

    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, root));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(2, plan.getInstances().size());
  }

  @Test
  public void testInsertRowNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {
              "s1",
            },
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10},
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    Analysis analysis = Util.constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    context.setQueryType(QueryType.WRITE);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, insertRowNode));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }

  @Test
  public void testInsertRowsNodeParallelPlan() throws IllegalPathException {
    QueryId queryId = new QueryId("test_write");
    InsertRowNode insertRowNode1 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            1L,
            new Object[] {10},
            false);
    insertRowNode1.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    InsertRowNode insertRowNode2 =
        new InsertRowNode(
            queryId.genPlanNodeId(),
            new PartialPath("root.sg.d1"),
            false,
            new String[] {"s1"},
            new TSDataType[] {TSDataType.INT32},
            10000L,
            new Object[] {10},
            false);
    insertRowNode2.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
        });

    InsertRowsNode node = new InsertRowsNode(queryId.genPlanNodeId());
    node.setInsertRowNodeList(Arrays.asList(insertRowNode1, insertRowNode2));
    node.setInsertRowNodeIndexList(Arrays.asList(0, 1));

    Analysis analysis = Util.constructAnalysis();

    MPPQueryContext context =
        new MPPQueryContext("", queryId, null, new TEndPoint(), new TEndPoint());
    context.setQueryType(QueryType.WRITE);
    DistributionPlanner planner =
        new DistributionPlanner(analysis, new LogicalQueryPlan(context, node));
    DistributedQueryPlan plan = planner.planFragments();
    assertEquals(1, plan.getInstances().size());
  }
}
