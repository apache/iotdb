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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata.DEVICE_VIEW_TEST_TABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata.TREE_VIEW_DB;

public class TopKRuntimeFilterTreeViewPlanTest {

  private static final String DEFAULT_TREE_DEVICE_VIEW_TABLE_FULL_NAME =
      String.format("%s.\"%s\"", TREE_VIEW_DB, DEVICE_VIEW_TEST_TABLE);

  @Before
  public void setup() {
    TsTable tsTable = new TsTable(DEVICE_VIEW_TEST_TABLE);
    tsTable.addProp(TsTable.TTL_PROPERTY, Long.MAX_VALUE + "");
    tsTable.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.test.**");
    DataNodeTableCache.getInstance().preUpdateTable(TREE_VIEW_DB, tsTable, null);
    DataNodeTableCache.getInstance().commitUpdateTable(TREE_VIEW_DB, DEVICE_VIEW_TEST_TABLE, null);
  }

  @After
  public void tearDown() {
    DataNodeTableCache.getInstance().invalid(TREE_VIEW_DB);
  }

  @Test
  public void marksRuntimeFilterOnTreeViewRegionTopKAndScan() {
    PlanTester planTester = new PlanTester();
    planTester.createPlan(
        "SELECT time, s1 FROM "
            + DEFAULT_TREE_DEVICE_VIEW_TABLE_FULL_NAME
            + " ORDER BY time DESC LIMIT 10");

    boolean foundProducer = false;
    boolean foundConsumer = false;
    String rootTopKId = null;
    for (int i = 1; i <= 4; i++) {
      RuntimeFilterMark mark = collectRuntimeFilterMark(planTester.getFragmentPlan(i));
      if (mark.producerTopKId != null) {
        foundProducer = true;
        rootTopKId = mark.producerTopKId;
      }
      if (mark.scanSourceId != null) {
        foundConsumer = true;
        if (rootTopKId != null) {
          Assert.assertEquals(rootTopKId, mark.scanSourceId);
        }
      }
    }

    Assert.assertTrue(foundProducer);
    Assert.assertTrue(foundConsumer);
  }

  private static RuntimeFilterMark collectRuntimeFilterMark(PlanNode root) {
    RuntimeFilterMark mark = new RuntimeFilterMark();
    collectRuntimeFilterMark(root, mark);
    return mark;
  }

  private static void collectRuntimeFilterMark(PlanNode node, RuntimeFilterMark mark) {
    if (node instanceof TopKNode) {
      TopKNode topKNode = (TopKNode) node;
      if (topKNode.getTopKRuntimeFilterSourceId() != null) {
        mark.producerTopKId = topKNode.getTopKRuntimeFilterSourceId();
      }
    }
    if (node instanceof DeviceTableScanNode) {
      DeviceTableScanNode scanNode = (DeviceTableScanNode) node;
      if (scanNode.getTopKRuntimeFilterSourceId() != null) {
        mark.scanSourceId = scanNode.getTopKRuntimeFilterSourceId();
        Assert.assertTrue(
            scanNode instanceof TreeAlignedDeviceViewScanNode
                || scanNode instanceof TreeNonAlignedDeviceViewScanNode);
      }
    }
    for (PlanNode child : node.getChildren()) {
      collectRuntimeFilterMark(child, mark);
    }
  }

  private static final class RuntimeFilterMark {
    private String producerTopKId;
    private String scanSourceId;
  }
}
