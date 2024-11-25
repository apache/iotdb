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
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource.Status;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener.PipeInsertionDataNodeListener;
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
  private static final int THIS_DATANODE_ID =
      IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private DeletionResourceManager deletionResourceManager;

  @Before
  public void setUp() throws Exception {
    DeletionResourceManager.buildForTest();
  }

  @After
  public void tearDown() throws Exception {
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
    addBatchDeletionResource(true);
    addBatchDeletionResource(false);
  }

  public void addBatchDeletionResource(boolean isRelational)
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[1]);
    int deletionCount = 10;
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
                      new DeletionPredicate("table1", new NOP()), new TimeRange(0, 10))));
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
    deletionRemove(true);
    deletionRemove(false);
  }

  public void deletionRemove(boolean isRelational)
      throws IllegalPathException, InterruptedException, IOException {
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[3]);
    // new a deletion
    int rebootTimes = 0;
    int deletionCount = 20;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    List<PipeDeleteDataNodeEvent> deletionEvents = new ArrayList<>();
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
      PipeDeleteDataNodeEvent deletionEvent = new PipeDeleteDataNodeEvent(deleteDataNode, true);
      deletionEvents.add(deletionEvent);
      DeletionResource deletionResource =
          deletionResourceManager.registerDeletionResource(deleteDataNode);
      deletionEvent.setDeletionResource(
          deletionResourceManager.getDeletionResource(deleteDataNode));
      if (deletionResource.waitForResult() != Status.SUCCESS) {
        Assert.fail();
      }
    }
    deletionEvents.forEach(deletionEvent -> deletionEvent.increaseReferenceCount("test"));
    List<Path> paths =
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[3]))
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
        Files.list(Paths.get(DELETION_BASE_DIR + File.separator + FAKE_DATA_REGION_IDS[3]))
            .collect(Collectors.toList());
    int afterCount = newPaths.size();
    Assert.assertTrue(afterCount < beforeFileCount);
  }

  @Test
  public void testWaitForResult() throws Exception {
    // prepare pipe component
    PipeRealtimeDataRegionExtractor extractor = new PipeRealtimeDataRegionHybridExtractor();
    PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY, "data");
              }
            });
    PipeTaskRuntimeConfiguration configuration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskExtractorRuntimeEnvironment(
                "1", 1, Integer.parseInt(FAKE_DATA_REGION_IDS[4]), null));
    extractor.customize(parameters, configuration);
    Assert.assertTrue(extractor.shouldExtractDeletion());

    PipeInsertionDataNodeListener.getInstance()
        .startListenAndAssign(FAKE_DATA_REGION_IDS[4], extractor);
    deletionResourceManager = DeletionResourceManager.getInstance(FAKE_DATA_REGION_IDS[4]);
    int rebootTimes = 0;
    MeasurementPath path = new MeasurementPath("root.vehicle.d2.s0");
    AbstractDeleteDataNode deleteDataNode =
        new DeleteDataNode(new PlanNodeId("1"), Collections.singletonList(path), 50, 150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(THIS_DATANODE_ID, new SimpleProgressIndex(rebootTimes, 1)));
    DeletionResource deletionResource =
        PipeInsertionDataNodeListener.getInstance()
            .listenToDeleteData(FAKE_DATA_REGION_IDS[4], deleteDataNode);
    Assert.assertSame(Status.SUCCESS, deletionResource.waitForResult());
    // close pipe resource
    PipeInsertionDataNodeListener.getInstance()
        .stopListenAndAssign(FAKE_DATA_REGION_IDS[4], extractor);
  }
}
