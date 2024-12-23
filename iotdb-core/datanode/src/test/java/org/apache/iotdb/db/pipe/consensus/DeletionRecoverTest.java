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

package org.apache.iotdb.db.pipe.consensus;

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class DeletionRecoverTest {
  private static final String[] FAKE_DATA_REGION_IDS = {"2", "3"};
  private static final int THIS_DATANODE_ID =
      IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private static final String DELETION_BASE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
  private final int deletionCount = 10;
  private DeletionResourceManager deletionResourceManager;

  public void setUp(boolean isRelational, String FAKE_DATA_REGION_ID) throws Exception {
    File baseDir = new File(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_ID);
    if (baseDir.exists()) {
      FileUtils.deleteFileOrDirectory(baseDir);
    }
    DeletionResourceManager.buildForTest();
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_ID);
    // Create some deletion files
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    for (int i = 0; i < deletionCount; i++) {
      AbstractDeleteDataNode deleteDataNode;
      if (isRelational) {
        deleteDataNode =
            new RelationalDeleteDataNode(
                new PlanNodeId("testPlan"),
                Collections.singletonList(
                    new TableDeletionEntry(
                        new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10))));
      } else {
        deleteDataNode =
            new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      }
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      deletionResourceManager.registerDeletionResource(deleteDataNode);
    }
    // Manually close to ensure all deletions are persisted
    deletionResourceManager.close();
  }

  @After
  public void tearDown() throws Exception {
    for (String FAKE_DATA_REGION_ID : FAKE_DATA_REGION_IDS) {
      File baseDir = new File(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_ID);
      if (baseDir.exists()) {
        FileUtils.deleteFileOrDirectory(baseDir);
      }
    }
  }

  @Test
  public void testDeletionRecoverTreeModel() throws Exception {
    setUp(false, FAKE_DATA_REGION_IDS[0]);
    Assert.assertEquals(0, deletionResourceManager.getAllDeletionResources().size());
    deletionResourceManager.recoverForTest();
    Assert.assertEquals(deletionCount, deletionResourceManager.getAllDeletionResources().size());
  }

  @Test
  public void testDeletionRecoverTableModel() throws Exception {
    setUp(true, FAKE_DATA_REGION_IDS[1]);
    Assert.assertEquals(0, deletionResourceManager.getAllDeletionResources().size());
    deletionResourceManager.recoverForTest();
    Assert.assertEquals(deletionCount, deletionResourceManager.getAllDeletionResources().size());
  }
}
