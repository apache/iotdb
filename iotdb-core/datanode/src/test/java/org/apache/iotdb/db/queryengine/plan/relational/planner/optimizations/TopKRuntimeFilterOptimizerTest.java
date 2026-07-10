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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

public class TopKRuntimeFilterOptimizerTest {

  @Test
  public void marksTopKAndScanWhenQualified() {
    PlanNodeId topKId = new PlanNodeId("topk");
    PlanNodeId scanId = new PlanNodeId("scan");
    TopKNode topKNode = createTopK(topKId, SortOrder.ASC_NULLS_LAST);
    DeviceTableScanNode scanNode = createScan(scanId);
    topKNode.addChild(new LimitNode(new PlanNodeId("limit"), scanNode, 10, Optional.empty()));

    TopKNode optimized = (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, null);

    Assert.assertTrue(optimized.isUseTopKRuntimeFilter());
    Assert.assertTrue(optimized.isTopKRuntimeFilterAscending());
    DeviceTableScanNode optimizedScan =
        (DeviceTableScanNode) optimized.getChildren().get(0).getChildren().get(0);
    Assert.assertEquals(topKId, optimizedScan.getTopKRuntimeFilterSourceId().orElse(null));
  }

  @Test
  public void skipsTopKWithExchangeChild() {
    PlanNodeId topKId = new PlanNodeId("topk");
    TopKNode topKNode = createTopK(topKId, SortOrder.DESC_NULLS_LAST);
    topKNode.addChild(new ExchangeNode(new PlanNodeId("exchange"), null, null, null, null));

    TopKNode optimized = (TopKNode) new TopKRuntimeFilterOptimizer().optimize(topKNode, null);

    Assert.assertFalse(optimized.isUseTopKRuntimeFilter());
  }

  private TopKNode createTopK(PlanNodeId id, SortOrder sortOrder) {
    Symbol timeSymbol = new Symbol("time");
    OrderingScheme orderingScheme =
        new OrderingScheme(Collections.singletonList(timeSymbol), sortOrder);
    return new TopKNode(id, orderingScheme, 10, Collections.singletonList(timeSymbol), false);
  }

  private DeviceTableScanNode createScan(PlanNodeId id) {
    return new DeviceTableScanNode(
        id, null, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
  }
}
