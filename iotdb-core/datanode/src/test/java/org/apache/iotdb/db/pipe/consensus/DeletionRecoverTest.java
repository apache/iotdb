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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class DeletionRecoverTest {
  private static final String FAKE_DATA_REGION_ID = "1";
  private static final int THIS_DATANODE_ID =
      IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private static final String DELETION_BASE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
  private static final String BASE_PATH = DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_ID;
  private final int deletionCount = 10;
  private DeletionResourceManager deletionResourceManager;

  @Before
  public void setUp() throws Exception {
    File baseDir = new File(BASE_PATH);
    if (baseDir.exists()) {
      FileUtils.deleteFileOrDirectory(baseDir);
    }
    DeletionResourceManager.buildForTest();
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_ID);
    // Create some deletion files
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    for (int i = 0; i < deletionCount; i++) {
      DeleteDataNode deleteDataNode =
          new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      deletionResourceManager.registerDeletionResource(deleteDataNode);
    }
    // Manually close to ensure all deletions are persisted
    deletionResourceManager.close();
  }

  @Test
  public void testDeletionRecover() throws Exception {
    Assert.assertEquals(0, deletionResourceManager.getAllDeletionResources().size());
    deletionResourceManager.recoverForTest();
    Assert.assertEquals(deletionCount, deletionResourceManager.getAllDeletionResources().size());
  }
}
