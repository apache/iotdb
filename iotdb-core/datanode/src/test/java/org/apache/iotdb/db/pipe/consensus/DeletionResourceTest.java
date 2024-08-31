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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
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
import java.util.Collections;
import java.util.stream.Stream;

public class DeletionResourceTest {
  private static final String FAKE_DATE_REGION_ID = "1";
  private static final String DELETION_BASE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getPipeConsensusDeletionFileDir();
  private static final String BASE_PATH = DELETION_BASE_DIR + File.separator + FAKE_DATE_REGION_ID;
  private static final int THIS_DATANODE_ID =
      IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private DeletionResourceManager deletionResourceManager;

  @Before
  public void setUp() throws Exception {
    DeletionResourceManager.buildForTest();
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATE_REGION_ID);
  }

  @After
  public void tearDown() throws Exception {
    deletionResourceManager.close();
    try (Stream<Path> pathStream = Files.walk(Paths.get(BASE_PATH), 1)) {
      for (Path path : pathStream.toArray(Path[]::new)) {
        FileUtils.deleteFileOrDirectory(path.toFile());
      }
    }
  }

  @Test
  public void testCreateBaseDir() {
    File baseDir = new File(DELETION_BASE_DIR);
    File dataRegionDir = new File(baseDir + File.separator + FAKE_DATE_REGION_ID);
    Assert.assertTrue(baseDir.exists());
    Assert.assertTrue(dataRegionDir.exists());
  }

  @Test
  public void testAddBatchDeletionResource()
      throws IllegalPathException, InterruptedException, IOException {
    int deletionCount = 10;
    int rebootTimes = 0;
    PartialPath path = new PartialPath("root.vehicle.d2.s0");
    for (int i = 0; i < deletionCount; i++) {
      DeleteDataNode deleteDataNode =
          new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      PipeSchemaRegionWritePlanEvent deletionEvent =
          new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
      deletionResourceManager.registerDeletionResource(deletionEvent);
    }
    // Sleep to wait deletion being persisted
    Thread.sleep(1000);
    Stream<Path> paths = Files.list(Paths.get(BASE_PATH));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testAddDeletionResourceTimeout()
      throws IllegalPathException, InterruptedException, IOException {
    int rebootTimes = 0;
    PartialPath path = new PartialPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    PipeSchemaRegionWritePlanEvent deletionEvent =
        new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
    // Only register one deletionResource
    deletionResourceManager.registerDeletionResource(deletionEvent);
    // Sleep to wait deletion being persisted
    Thread.sleep(1000);
    Stream<Path> paths = Files.list(Paths.get(BASE_PATH));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testDeletionRemove() throws IllegalPathException, InterruptedException, IOException {
    // new a deletion
    int rebootTimes = 0;
    PartialPath path = new PartialPath("root.vehicle.d2.s0");
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    PipeSchemaRegionWritePlanEvent deletionEvent =
        new PipeSchemaRegionWritePlanEvent(deleteDataNode, true);
    // Only register one deletionResource
    deletionResourceManager.registerDeletionResource(deletionEvent);
    deletionEvent.increaseReferenceCount("test");
    // Sleep to wait deletion being persisted
    Thread.sleep(1000);
    Stream<Path> paths = Files.list(Paths.get(BASE_PATH));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
    // Remove deletion
    deletionEvent.decreaseReferenceCount("test", false);
    // Sleep to wait deletion being removed
    Thread.sleep(1000);
    Stream<Path> newPaths = Files.list(Paths.get(BASE_PATH));
    Assert.assertFalse(newPaths.anyMatch(Files::isRegularFile));
  }
}
