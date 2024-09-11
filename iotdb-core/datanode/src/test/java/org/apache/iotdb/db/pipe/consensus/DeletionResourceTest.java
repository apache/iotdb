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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource.Status;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeletionResourceTest {
  private static final String[] FAKE_DATE_REGION_IDS = {"2", "3", "4", "5", "6"};
  private static final String DELETION_BASE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getPipeConsensusDeletionFileDir();
  private static final int THIS_DATANODE_ID =
      IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private DeletionResourceManager deletionResourceManager;

  @Before
  public void setUp() throws Exception {
    DeletionResourceManager.buildForTest();
  }

  @After
  public void tearDown() throws Exception {
    for (String FAKE_DATE_REGION_ID : FAKE_DATE_REGION_IDS) {
      File baseDir = new File(DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_ID);
      if (baseDir.exists()) {
        FileUtils.deleteFileOrDirectory(baseDir);
      }
    }
  }

  @Test
  public void testCreateBaseDir() {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_IDS[0]);
    File baseDir = new File(DELETION_BASE_DIR);
    File dataRegionDir = new File(baseDir + File.separator + FAKE_DATE_REGION_IDS[0]);
    Assert.assertTrue(baseDir.exists());
    Assert.assertTrue(dataRegionDir.exists());
  }

  @Test
  public void testAddBatchDeletionResource()
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_IDS[1]);
    int deletionCount = 10;
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    for (int i = 0; i < deletionCount; i++) {
      DeleteDataNode deleteDataNode =
          new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      PipeSchemaRegionWritePlanEvent deletionEvent =
          new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
      deletionResourceManager.registerDeletionResource(deletionEvent);
    }

    Stream<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_IDS[1]));
    ;
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testAddDeletionResourceTimeout()
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_IDS[2]);
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    PipeSchemaRegionWritePlanEvent deletionEvent =
        new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
    // Only register one deletionResource
    deletionResourceManager.registerDeletionResource(deletionEvent);
    // Sleep to wait deletion being persisted
    Thread.sleep(5000);
    Stream<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_IDS[2]));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testDeletionRemove() throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_IDS[3]);
    // new a deletion
    int rebootTimes = 0;
    int deletionCount = 100;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    List<PipeSchemaRegionWritePlanEvent> deletionEvents = new ArrayList<>();
    for (int i = 0; i < deletionCount; i++) {
      DeleteDataNode deleteDataNode =
          new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      PipeSchemaRegionWritePlanEvent deletionEvent =
          new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
      deletionEvents.add(deletionEvent);
      deletionResourceManager.registerDeletionResource(deletionEvent);
    }
    deletionEvents.forEach(deletionEvent -> deletionEvent.increaseReferenceCount("test"));
    // Sleep to wait deletion being persisted
    Thread.sleep(1000);
    List<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_IDS[3]))
            .collect(Collectors.toList());
    Assert.assertTrue(paths.stream().anyMatch(Files::isRegularFile));
    int beforeFileCount = paths.size();
    if (beforeFileCount < 2) {
      return;
    }
    // Remove deletion
    deletionEvents.forEach(deletionEvent -> deletionEvent.decreaseReferenceCount("test", false));
    // Sleep to wait deletion being removed
    Thread.sleep(1000);
    List<Path> newPaths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_IDS[3]))
            .collect(Collectors.toList());
    int afterCount = newPaths.size();
    Assert.assertTrue(afterCount < beforeFileCount);
  }

  @Test
  public void testWaitForResult() throws Exception {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_IDS[4]);
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    DeletionResource deletionResource =
        PipeInsertionDataNodeListener.getInstance()
            .listenToDeleteData(deleteDataNode, FAKE_DATE_REGION_IDS[4]);
    Assert.assertSame(deletionResource.waitForResult(), Status.SUCCESS);
  }
}
