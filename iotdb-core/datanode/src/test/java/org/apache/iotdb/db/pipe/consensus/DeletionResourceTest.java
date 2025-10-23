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
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource.Status;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHybridSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.read.common.TimeRange;
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
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeletionResourceTest {
  private static final String[] FAKE_DATA_REGION_IDS = {"2", "3", "4", "5", "6"};
  private static final String DELETION_BASE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
  private static final int THIS_DATANODE_ID = 0;
  private static final int TEST_DAL_FILE_SIZE = 1024;
  private DeletionResourceManager deletionResourceManager;
  private int previousDataNodeId;

  @Before
  public void setUp() throws Exception {
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(THIS_DATANODE_ID);
    DeletionResourceManager.buildForTest();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
    for (String FAKE_DATA_REGION_ID : FAKE_DATA_REGION_IDS) {
      File baseDir = new File(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_ID);
      if (baseDir.exists()) {
        FileUtils.deleteFileOrDirectory(baseDir);
      }
    }
    deletionResourceManager.close();
  }

  @Test
  public void testCreateBaseDir() {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[0]);
    File baseDir = new File(DELETION_BASE_DIR);
    File dataRegionDir = new File(baseDir + File.separator + FAKE_DATA_REGION_IDS[0]);
    Assert.assertTrue(baseDir.exists());
    Assert.assertTrue(dataRegionDir.exists());
  }

  @Test
  public void testAddBatchDeletionResource()
      throws IllegalPathException, IOException, InterruptedException {
    addBatchDeletionResource(true, 0);
    addBatchDeletionResource(false, 10);
  }

  public void addBatchDeletionResource(final boolean isRelational, final int initialIndex)
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[1]);
    int deletionCount = 10;
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    for (int i = initialIndex; i < initialIndex + deletionCount; i++) {
      AbstractDeleteDataNode deleteDataNode;
      if (isRelational) {
        deleteDataNode =
            new RelationalDeleteDataNode(
                new PlanNodeId("testPlan"),
                Collections.singletonList(
                    new TableDeletionEntry(
                        new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10))),
                null);
      } else {
        deleteDataNode =
            new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      }
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      deletionResourceManager.registerDeletionResource(deleteDataNode);
    }

    Stream<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[1]));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testAddDeletionResourceTimeout()
      throws IllegalPathException, IOException, InterruptedException {
    addDeletionResourceTimeout(true);
    addDeletionResourceTimeout(false);
  }

  public void addDeletionResourceTimeout(boolean isRelational)
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[2]);
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    AbstractDeleteDataNode deleteDataNode;
    if (isRelational) {
      deleteDataNode =
          new RelationalDeleteDataNode(
              new PlanNodeId("testPlan"),
              Collections.singletonList(
                  new TableDeletionEntry(
                      new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10))),
              null);
    } else {
      deleteDataNode =
          new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    }
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    // Only register one deletionResource
    DeletionResource deletionResource =
        deletionResourceManager.registerDeletionResource(deleteDataNode);
    if (deletionResource.waitForResult() != Status.SUCCESS) {
      Assert.fail();
    }
    Stream<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[2]));
    Assert.assertTrue(paths.anyMatch(Files::isRegularFile));
  }

  @Test
  public void testDeletionRemove() throws IllegalPathException, InterruptedException, IOException {
    PageCacheDeletionBuffer.setDalBufferSize(TEST_DAL_FILE_SIZE);
    deletionRemove(true, 0);
    deletionRemove(false, 20);
  }

  public void deletionRemove(final boolean isRelational, final int initialIndex)
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[3]);
    // new a deletion
    final int rebootTimes = 0;
    final int deletionCount = 20;
    final MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    final List<PipeDeleteDataNodeEvent> deletionEvents = new ArrayList<>();
    for (int i = initialIndex; i < initialIndex + deletionCount; i++) {
      final AbstractDeleteDataNode deleteDataNode;
      if (isRelational) {
        deleteDataNode =
            new RelationalDeleteDataNode(
                new PlanNodeId("testPlan"),
                Collections.singletonList(
                    new TableDeletionEntry(
                        new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10))),
                null);
      } else {
        deleteDataNode =
            new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
      }
      deleteDataNode.setProgressIndex(
          new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, i)));
      final PipeDeleteDataNodeEvent deletionEvent =
          new PipeDeleteDataNodeEvent(
              deleteDataNode, "Test", 10, null, null, null, null, null, null, true, true);
      deletionEvent.setCommitterKeyAndCommitId(
          new CommitterKey("Test", 10, Integer.parseInt(FAKE_DATA_REGION_IDS[3]), 0), i + 1);
      deletionEvents.add(deletionEvent);

      final DeletionResource deletionResource =
          deletionResourceManager.registerDeletionResource(deleteDataNode);
      deletionResource.setPipeTaskReferenceCount(1);
      deletionEvent.setDeletionResource(
          deletionResourceManager.getDeletionResource(deleteDataNode));
      if (deletionResource.waitForResult() != Status.SUCCESS) {
        Assert.fail();
      }
    }

    // for event commit to invoke onCommit() to removeDAL
    if (initialIndex == 0) {
      PipeEventCommitManager.getInstance()
          .register("Test", 10, Integer.parseInt(FAKE_DATA_REGION_IDS[3]), "Test");
    }
    deletionEvents.forEach(deletionEvent -> deletionEvent.increaseReferenceCount("test"));
    final List<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[3]))
            .collect(Collectors.toList());
    Assert.assertTrue(paths.stream().anyMatch(Files::isRegularFile));
    final int beforeFileCount = paths.size();
    if (beforeFileCount < 2) {
      // not generate enough DAL file
      return;
    }
    // Remove deletion
    deletionEvents.forEach(deletionEvent -> deletionEvent.decreaseReferenceCount("test", false));
    // Sleep to wait deletion being removed
    Thread.sleep(1000);
    final List<Path> newPaths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[3]))
            .collect(Collectors.toList());
    final int afterCount = newPaths.size();
    // assume all DAL are deleted except for the last one.
    Assert.assertTrue(afterCount < beforeFileCount && afterCount == 1);
  }

  @Test
  public void testWaitForResult() throws Exception {
    // prepare pipe component
    final PipeRealtimeDataRegionSource extractor = new PipeRealtimeDataRegionHybridSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "data");
              }
            });
    final PipeTaskRuntimeConfiguration configuration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskSourceRuntimeEnvironment(
                "1", 1, Integer.parseInt(FAKE_DATA_REGION_IDS[4]), null));
    extractor.customize(parameters, configuration);
    Assert.assertTrue(extractor.shouldExtractDeletion());

    PipeInsertionDataNodeListener.getInstance()
        .startListenAndAssign(FAKE_DATA_REGION_IDS[4], extractor);
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[4]);
    final int rebootTimes = 0;
    final MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    final AbstractDeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    final DeletionResource deletionResource =
        PipeInsertionDataNodeListener.getInstance()
            .listenToDeleteData(FAKE_DATA_REGION_IDS[4], deleteDataNode);
    Assert.assertSame(Status.SUCCESS, deletionResource.waitForResult());
    // close pipe resource
    PipeInsertionDataNodeListener.getInstance()
        .stopListenAndAssign(FAKE_DATA_REGION_IDS[4], extractor);
  }
}
