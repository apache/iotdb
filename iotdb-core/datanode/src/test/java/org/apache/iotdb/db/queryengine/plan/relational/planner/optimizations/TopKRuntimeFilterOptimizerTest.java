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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer.Context;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TopKRuntimeFilterOptimizerTest {

  private boolean originalEnableTopKRuntimeFilter;

  @Before
  public void setUp() {
    originalEnableTopKRuntimeFilter =
        IoTDBDescriptor.getInstance().getConfig().isEnableTopKRuntimeFilter();
    IoTDBDescriptor.getInstance().getConfig().setEnableTopKRuntimeFilter(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableTopKRuntimeFilter(originalEnableTopKRuntimeFilter);
  }

  @Test
  public void marksTopKAndScanWithCurrentTopKId() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertEquals(topKId.getId(), optimized.getTopKRuntimeFilterSourceId());
    Assert.assertTrue(optimized.isTopKRuntimeFilterAscending());
    DeviceTableScanNode optimizedScan = (DeviceTableScanNode) optimized.getChildren().get(0);
    Assert.assertEquals(topKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void marksOutputTopKScanWithCurrentTopKId() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));
    OutputNode outputNode =
        new OutputNode(
            new PlanNodeId("output"),
            topKNode,
            Collections.singletonList("time"),
            topKNode.getOutputSymbols());

    OutputNode optimized =
        (OutputNode) new TopKRuntimeFilterOptimizer().optimize(outputNode, queryContext());

    TopKNode optimizedTopK = (TopKNode) optimized.getChild();
    Assert.assertEquals(topKId.getId(), optimizedTopK.getTopKRuntimeFilterSourceId());
    DeviceTableScanNode optimizedScan = (DeviceTableScanNode) optimizedTopK.getChildren().get(0);
    Assert.assertEquals(topKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void marksDescendingWhenOrderByTimeDesc() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertEquals(topKId.getId(), optimized.getTopKRuntimeFilterSourceId());
    Assert.assertFalse(optimized.isTopKRuntimeFilterAscending());
  }

  @Test
  public void skipsWhenScanNotDirectChild() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    DeviceTableScanNode scanNode = createScan(new PlanNodeId("scan"));
    topKNode.addChild(new LimitNode(new PlanNodeId("limit"), scanNode, 10, Optional.empty()));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertNull(optimized.getTopKRuntimeFilterSourceId());
    PlanNode limit = optimized.getChildren().get(0);
    DeviceTableScanNode optimizedScan = (DeviceTableScanNode) limit.getChildren().get(0);
    Assert.assertNull(optimizedScan.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void skipsTopKWithoutScan() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    topKNode.addChild(new ExchangeNode(new PlanNodeId("exchange")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertNull(optimized.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void siblingTopKsUnderUnionUseDistinctFilterIds() {
    PlanNodeId leftTopKId = new PlanNodeId("left-topk");
    PlanNodeId rightTopKId = new PlanNodeId("right-topk");
    TopKNode leftTopK = createTopK(leftTopKId, SortOrder.DESC_NULLS_LAST);
    leftTopK.addChild(createScan(new PlanNodeId("left-scan")));
    TopKNode rightTopK = createTopK(rightTopKId, SortOrder.DESC_NULLS_LAST);
    rightTopK.addChild(createScan(new PlanNodeId("right-scan")));

    Symbol timeSymbol = new Symbol("time");
    UnionNode unionNode =
        new UnionNode(
            new PlanNodeId("union"),
            ImmutableList.of(leftTopK, rightTopK),
            ImmutableListMultimap.of(timeSymbol, timeSymbol, timeSymbol, timeSymbol),
            ImmutableList.of(timeSymbol));

    UnionNode optimized =
        (UnionNode) new TopKRuntimeFilterOptimizer().optimize(unionNode, queryContext());

    TopKNode optimizedLeft = (TopKNode) optimized.getChildren().get(0);
    TopKNode optimizedRight = (TopKNode) optimized.getChildren().get(1);
    Assert.assertEquals(leftTopKId.getId(), optimizedLeft.getTopKRuntimeFilterSourceId());
    Assert.assertEquals(rightTopKId.getId(), optimizedRight.getTopKRuntimeFilterSourceId());
    Assert.assertNotEquals(
        optimizedLeft.getTopKRuntimeFilterSourceId(),
        optimizedRight.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void skipsWhenConfigDisabled() {
    IoTDBDescriptor.getInstance().getConfig().setEnableTopKRuntimeFilter(false);
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertNull(optimized.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void distributedSingleRegionKeepsLogicalTopKSourceId() {
    PlanNodeId logicalTopKId = new PlanNodeId("logical-topk");
    TopKNode logicalTopK = createMarkedLogicalTopK(logicalTopKId);
    DeviceTableScanNode regionScan = createScan(new PlanNodeId("region-scan"));

    TopKNode distributedTopK =
        (TopKNode)
            simulateDistributedTopK(logicalTopK, Collections.singletonList(regionScan)).get(0);

    Assert.assertEquals(logicalTopKId.getId(), distributedTopK.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void distributedMultiRegionCopiesSourceIdToRegionTopKAndClearsCoordinator() {
    PlanNodeId logicalTopKId = new PlanNodeId("logical-topk");
    TopKNode logicalTopK = createMarkedLogicalTopK(logicalTopKId);
    List<DeviceTableScanNode> regionScans =
        ImmutableList.of(
            createScan(new PlanNodeId("scan-r1")), createScan(new PlanNodeId("scan-r2")));

    TopKNode coordinator = (TopKNode) simulateDistributedTopK(logicalTopK, regionScans).get(0);

    Assert.assertNull(coordinator.getTopKRuntimeFilterSourceId());
    Assert.assertEquals(2, coordinator.getChildren().size());
    for (PlanNode child : coordinator.getChildren()) {
      TopKNode regionTopK = (TopKNode) child;
      Assert.assertEquals(logicalTopKId.getId(), regionTopK.getTopKRuntimeFilterSourceId());
    }
  }

  @Test
  public void distributedUnionBranchesKeepDistinctSourceIds() {
    PlanNodeId leftTopKId = new PlanNodeId("left-topk");
    PlanNodeId rightTopKId = new PlanNodeId("right-topk");
    TopKNode leftTopK = createMarkedLogicalTopK(leftTopKId);
    TopKNode rightTopK = createMarkedLogicalTopK(rightTopKId);

    TopKNode distributedLeftCoordinator =
        (TopKNode)
            simulateDistributedTopK(
                    leftTopK,
                    ImmutableList.of(
                        createScan(new PlanNodeId("left-scan-r1")),
                        createScan(new PlanNodeId("left-scan-r2"))))
                .get(0);
    TopKNode distributedRightTopK =
        (TopKNode)
            simulateDistributedTopK(
                    rightTopK,
                    Collections.singletonList(createScan(new PlanNodeId("right-scan-r1"))))
                .get(0);

    Assert.assertNull(distributedLeftCoordinator.getTopKRuntimeFilterSourceId());
    Assert.assertEquals(
        leftTopKId.getId(),
        ((TopKNode) distributedLeftCoordinator.getChildren().get(0))
            .getTopKRuntimeFilterSourceId());
    Assert.assertEquals(
        leftTopKId.getId(),
        ((TopKNode) distributedLeftCoordinator.getChildren().get(1))
            .getTopKRuntimeFilterSourceId());

    Assert.assertEquals(rightTopKId.getId(), distributedRightTopK.getTopKRuntimeFilterSourceId());
    Assert.assertNotEquals(
        ((TopKNode) distributedLeftCoordinator.getChildren().get(0)).getTopKRuntimeFilterSourceId(),
        distributedRightTopK.getTopKRuntimeFilterSourceId());
  }

  /**
   * Mirrors {@code TableDistributedPlanGenerator#visitTopK} runtime-filter replication after scan
   * distribution, without running full table partition logic.
   */
  private static List<PlanNode> simulateDistributedTopK(
      TopKNode logicalTopK, List<DeviceTableScanNode> distributedScans) {
    String sourceId = logicalTopK.getTopKRuntimeFilterSourceId();
    QueryId queryId = QueryId.MOCK_QUERY_ID;
    if (distributedScans.size() == 1) {
      logicalTopK.setChildren(Collections.singletonList(distributedScans.get(0)));
      return Collections.singletonList(logicalTopK);
    }

    TopKNode coordinator = (TopKNode) logicalTopK.clone();
    coordinator.setTopKRuntimeFilterSourceId(null);
    List<PlanNode> regionTopKs = new ArrayList<>(distributedScans.size());
    for (DeviceTableScanNode distributedScan : distributedScans) {
      TopKNode regionTopK =
          new TopKNode(
              queryId.genPlanNodeId(),
              Collections.singletonList(distributedScan),
              logicalTopK.getOrderingScheme(),
              logicalTopK.getCount(),
              logicalTopK.getOutputSymbols(),
              logicalTopK.isChildrenDataInOrder());
      regionTopK.setTopKRuntimeFilterSourceId(sourceId);
      regionTopKs.add(regionTopK);
    }
    coordinator.setChildren(regionTopKs);
    return Collections.singletonList(coordinator);
  }

  /** Logical optimizer output: TopK and scan share the TopK plan node id as source id. */
  private TopKNode createMarkedLogicalTopK(PlanNodeId topKId) {
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    DeviceTableScanNode scan = createScan(new PlanNodeId(topKId.getId() + "-scan"));
    topKNode.addChild(scan);
    topKNode.setTopKRuntimeFilterSourceId(topKId.getId());
    scan.setTopKRuntimeFilterSourceId(topKId.getId());
    return topKNode;
  }

  private static Context queryContext() {
    Analysis analysis = Mockito.mock(Analysis.class);
    Mockito.when(analysis.isQuery()).thenReturn(true);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getAnalysis()).thenReturn(analysis);
    return context;
  }

  private TopKNode createTopK(PlanNodeId id, SortOrder sortOrder) {
    Symbol timeSymbol = new Symbol("time");
    OrderingScheme orderingScheme =
        new OrderingScheme(ImmutableList.of(timeSymbol), ImmutableMap.of(timeSymbol, sortOrder));
    return new TopKNode(id, orderingScheme, 10, Collections.singletonList(timeSymbol), false);
  }

  private DeviceTableScanNode createScan(PlanNodeId id) {
    return new DeviceTableScanNode(
        id, null, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
  }
}
