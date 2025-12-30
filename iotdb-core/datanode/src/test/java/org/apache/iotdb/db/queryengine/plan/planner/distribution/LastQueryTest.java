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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LastQueryTest {

  @Test
  public void testSortLastQueryScanNode() throws IllegalPathException {
    LastQueryNode lastQueryNode = new LastQueryNode(new PlanNodeId("test"), null, true);

    lastQueryNode.addDeviceLastQueryScanNode(
        new PlanNodeId("test_last_query_scan1"),
        new PartialPath("root.test.d1"),
        true,
        Arrays.asList(
            new MeasurementSchema("s3", TSDataType.INT32),
            new MeasurementSchema("s1", TSDataType.BOOLEAN),
            new MeasurementSchema("s2", TSDataType.INT32)),
        null,
        null);
    lastQueryNode.addDeviceLastQueryScanNode(
        new PlanNodeId("test_last_query_scan2"),
        new PartialPath("root.test.d0"),
        false,
        Collections.singletonList(new MeasurementSchema("s0", TSDataType.BOOLEAN)),
        null,
        null);

    Analysis analysis = Util.constructAnalysis();
    SourceRewriter sourceRewriter = new SourceRewriter(analysis);
    DistributionPlanContext context =
        new DistributionPlanContext(
            new MPPQueryContext("", new QueryId("test"), null, new TEndPoint(), new TEndPoint()));
    context.setOneSeriesInMultiRegion(true);
    context.setQueryMultiRegion(true);
    List<PlanNode> result = sourceRewriter.visitLastQuery(lastQueryNode, context);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0) instanceof LastQueryMergeNode);
    LastQueryMergeNode mergeNode = (LastQueryMergeNode) result.get(0);
    Assert.assertEquals(1, mergeNode.getChildren().size());
    Assert.assertTrue(mergeNode.getChildren().get(0) instanceof LastQueryNode);

    LastQueryNode lastQueryNode2 = (LastQueryNode) mergeNode.getChildren().get(0);
    Assert.assertEquals(2, lastQueryNode2.getChildren().size());
    Assert.assertTrue(lastQueryNode2.getChildren().get(0) instanceof LastQueryScanNode);

    LastQueryScanNode scanNodeChild1 = (LastQueryScanNode) lastQueryNode2.getChildren().get(0);
    Assert.assertTrue(scanNodeChild1.getDevicePath().toString().contains("d0"));
    Assert.assertEquals("s0", scanNodeChild1.getMeasurementSchemas().get(0).getMeasurementName());

    LastQueryScanNode scanNodeChild2 = (LastQueryScanNode) lastQueryNode2.getChildren().get(1);
    Assert.assertTrue(scanNodeChild2.getDevicePath().toString().contains("d1"));
    Assert.assertEquals("s1", scanNodeChild2.getMeasurementSchemas().get(0).getMeasurementName());
    Assert.assertEquals("s2", scanNodeChild2.getMeasurementSchemas().get(1).getMeasurementName());
    Assert.assertEquals("s3", scanNodeChild2.getMeasurementSchemas().get(2).getMeasurementName());
  }

  @Test
  public void testLastQuery1Series1Region() throws IllegalPathException {
    String d2s1Path = "root.sg.d22.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_1_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Collections.singletonList(d2s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(1, distributedQueryPlan.getInstances().size());
    Assert.assertTrue(
        distributedQueryPlan
                .getInstances()
                .get(0)
                .getFragment()
                .getPlanNodeTree()
                .getChildren()
                .get(0)
            instanceof LastQueryNode);
  }

  @Test
  public void testLastQuery1Series2Region() throws IllegalPathException {
    String d1s1Path = "root.sg.d1.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Collections.singletonList(d1s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(2, distributedQueryPlan.getInstances().size());
    PlanNode rootNode =
        distributedQueryPlan
            .getInstances()
            .get(0)
            .getFragment()
            .getPlanNodeTree()
            .getChildren()
            .get(0);
    Assert.assertTrue(rootNode instanceof LastQueryMergeNode);
    rootNode
        .getChildren()
        .forEach(
            child ->
                Assert.assertTrue(child instanceof LastQueryNode || child instanceof ExchangeNode));
  }

  @Test
  public void testLastQuery2Series3Region() throws IllegalPathException {
    String d1s1Path = "root.sg.d1.s1";
    String d2s1Path = "root.sg.d22.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Arrays.asList(d1s1Path, d2s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(3, distributedQueryPlan.getInstances().size());
    PlanNode rootNode =
        distributedQueryPlan
            .getInstances()
            .get(0)
            .getFragment()
            .getPlanNodeTree()
            .getChildren()
            .get(0);
    Assert.assertTrue(rootNode instanceof LastQueryMergeNode);
    rootNode
        .getChildren()
        .forEach(
            child ->
                Assert.assertTrue(child instanceof LastQueryNode || child instanceof ExchangeNode));
  }

  @Test
  public void testLastQuery2Series2Region() throws IllegalPathException {
    String d3s1Path = "root.sg.d333.s1";
    String d4s1Path = "root.sg.d4444.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "", new QueryId("test_last_1_series_2_region"), null, new TEndPoint(), new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Arrays.asList(d3s1Path, d4s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(2, distributedQueryPlan.getInstances().size());
    PlanNode rootNode =
        distributedQueryPlan
            .getInstances()
            .get(0)
            .getFragment()
            .getPlanNodeTree()
            .getChildren()
            .get(0);
    Assert.assertTrue(rootNode instanceof LastQueryMergeNode);
    rootNode
        .getChildren()
        .forEach(
            child ->
                Assert.assertTrue(child instanceof LastQueryNode || child instanceof ExchangeNode));
  }

  @Test
  public void testLastQuery2Series2DiffRegion() throws IllegalPathException {
    String d3s1Path = "root.sg.d22.s1";
    String d4s1Path = "root.sg.d55555.s1";
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("test_last_2_series_2_diff_region"),
            null,
            new TEndPoint(),
            new TEndPoint());
    DistributionPlanner planner =
        new DistributionPlanner(
            Util.constructAnalysis(),
            constructLastQuery(Arrays.asList(d3s1Path, d4s1Path), context));

    DistributedQueryPlan distributedQueryPlan = planner.planFragments();
    Assert.assertEquals(2, distributedQueryPlan.getInstances().size());
    PlanNode rootNode =
        distributedQueryPlan
            .getInstances()
            .get(0)
            .getFragment()
            .getPlanNodeTree()
            .getChildren()
            .get(0);
    Assert.assertTrue(rootNode instanceof LastQueryCollectNode);
    rootNode
        .getChildren()
        .forEach(
            child ->
                Assert.assertTrue(child instanceof LastQueryNode || child instanceof ExchangeNode));
  }

  private LogicalQueryPlan constructLastQuery(List<String> paths, MPPQueryContext context)
      throws IllegalPathException {
    LastQueryNode root = new LastQueryNode(context.getQueryId().genPlanNodeId(), null, false);
    for (String path : paths) {
      MeasurementPath selectPath = new MeasurementPath(path);
      root.addDeviceLastQueryScanNode(
          context.getQueryId().genPlanNodeId(),
          selectPath.getDevicePath(),
          selectPath.isUnderAlignedEntity(),
          Collections.singletonList(selectPath.getMeasurementSchema()),
          null,
          null);
    }

    return new LogicalQueryPlan(context, root);
  }
}
