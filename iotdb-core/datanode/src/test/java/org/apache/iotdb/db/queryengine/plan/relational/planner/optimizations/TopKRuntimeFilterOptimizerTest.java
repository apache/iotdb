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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer.Context;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class TopKRuntimeFilterOptimizerTest {

  /** Single region: the sole TopK sits directly over the scan and uses its own id as root. */
  @Test
  public void marksTopKAndScanWithOwnRootIdInSingleRegion() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertNotNull(optimized.getTopKRuntimeFilterSourceId());
    Assert.assertTrue(optimized.isTopKRuntimeFilterAscending());
    Assert.assertEquals(topKId, optimized.getTopKRuntimeFilterSourceId());
    DeviceTableScanNode optimizedScan = (DeviceTableScanNode) optimized.getChildren().get(0);
    Assert.assertEquals(topKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void marksDescendingWhenOrderByTimeDesc() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    topKNode.addChild(createScan(new PlanNodeId("scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertNotNull(optimized.getTopKRuntimeFilterSourceId());
    Assert.assertFalse(optimized.isTopKRuntimeFilterAscending());
  }

  /**
   * Multi region: the coordinator TopK establishes the root id but is not a producer; each region
   * TopK directly above a scan becomes the producer and both it and its scan carry the root id.
   */
  @Test
  public void regionTopKAndScanShareRootTopKId() {
    PlanNodeId rootTopKId = new PlanNodeId("root-topk");
    PlanNodeId regionTopKId = new PlanNodeId("region-topk");
    TopKNode rootTopK = createTopK(rootTopKId, SortOrder.DESC_NULLS_LAST);
    ExchangeNode exchange = new ExchangeNode(new PlanNodeId("exchange"));
    TopKNode regionTopK = createTopK(regionTopKId, SortOrder.DESC_NULLS_LAST);
    regionTopK.addChild(createScan(new PlanNodeId("scan")));
    exchange.addChild(regionTopK);
    rootTopK.addChild(exchange);

    TopKNode optimizedRoot =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(rootTopK, queryContext());

    // Coordinator TopK is not a producer (its child is an Exchange, not a scan).
    Assert.assertNull(optimizedRoot.getTopKRuntimeFilterSourceId());

    TopKNode optimizedRegion = (TopKNode) optimizedRoot.getChildren().get(0).getChildren().get(0);
    Assert.assertNotNull(optimizedRegion.getTopKRuntimeFilterSourceId());
    Assert.assertEquals(rootTopKId, optimizedRegion.getTopKRuntimeFilterSourceId());

    DeviceTableScanNode optimizedScan = (DeviceTableScanNode) optimizedRegion.getChildren().get(0);
    Assert.assertEquals(rootTopKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
  }

  /** Scan not a direct child of the TopK (a Limit in between): the structure is not marked. */
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
  public void marksTreeAlignedDeviceViewScanWhenDirectChild() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    topKNode.addChild(createTreeAlignedScan(new PlanNodeId("tree-scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertEquals(topKId, optimized.getTopKRuntimeFilterSourceId());
    TreeAlignedDeviceViewScanNode optimizedScan =
        (TreeAlignedDeviceViewScanNode) optimized.getChildren().get(0);
    Assert.assertEquals(topKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
  }

  @Test
  public void marksExternalTsFileScanWhenDirectChild() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    topKNode.addChild(createExternalTsFileScan(new PlanNodeId("external-scan")));

    TopKNode optimized =
        (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, queryContext());

    Assert.assertEquals(topKId, optimized.getTopKRuntimeFilterSourceId());
    ExternalTsFileScanNode optimizedScan = (ExternalTsFileScanNode) optimized.getChildren().get(0);
    Assert.assertEquals(topKId.getId(), optimizedScan.getTopKRuntimeFilterSourceId());
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

  private TreeAlignedDeviceViewScanNode createTreeAlignedScan(PlanNodeId id) {
    return new TreeAlignedDeviceViewScanNode(
        id,
        null,
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyMap(),
        Ordering.ASC,
        null,
        null,
        10,
        0,
        true,
        false,
        "root.test",
        Collections.emptyMap());
  }

  private ExternalTsFileScanNode createExternalTsFileScan(PlanNodeId id) {
    ExternalTsFileQueryResource resource = Mockito.mock(ExternalTsFileQueryResource.class);
    Mockito.when(resource.getSharedDeviceEntries()).thenReturn(Collections.emptyList());
    return new ExternalTsFileScanNode(
        id,
        null,
        Collections.emptyList(),
        Collections.emptyMap(),
        null,
        10,
        0,
        null,
        Ordering.ASC,
        true,
        Collections.emptyMap(),
        resource,
        Collections.emptyList(),
        0,
        null);
  }
}
