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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInAgent;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHybridSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionLogSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionTsFileSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PipeRealtimeExtractTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRealtimeExtractTest.class);
  private static final String TEST_PIPE_NAME = "test_degraded_status_pipe";
  private static final long TEST_PIPE_CREATION_TIME = 1L;
  private static final String TEST_REFERENCE_HOLDER = PipeRealtimeExtractTest.class.getName();

  private final int dataRegion1 = 1;
  private final int dataRegion2 = 2;
  private final String pattern1 = "root.sg.d";
  private final String pattern2 = "root.sg.d.a";
  private final String[] device = new String[] {"root", "sg", "d"};
  private final AtomicBoolean alive = new AtomicBoolean();
  private File tmpDir;
  private File tsFileDir;

  private ExecutorService writeService;
  private ExecutorService listenerService;
  private int dataNodeId;
  private double pipeTotalFloatingMemoryProportion;

  @Before
  public void setUp() throws Exception {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    pipeTotalFloatingMemoryProportion =
        CommonDescriptor.getInstance().getConfig().getPipeTotalFloatingMemoryProportion();
    removeTestPipeMeta();
    writeService = Executors.newFixedThreadPool(2);
    listenerService = Executors.newFixedThreadPool(4);
    tmpDir = new File(Files.createTempDirectory("pipeRealtimeExtractor").toString());
    tsFileDir =
        new File(
            tmpDir.getPath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + "root.sg");
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    CommonDescriptor.getInstance()
        .getConfig()
        .setPipeTotalFloatingMemoryProportion(pipeTotalFloatingMemoryProportion);
    writeService.shutdownNow();
    listenerService.shutdownNow();
    FileUtils.deleteFileOrDirectory(tmpDir);
    removeTestPipeMeta();
  }

  @Test
  public void testRealtimeExtractProcess() {
    // set up realtime extractor

    try (final PipeRealtimeDataRegionLogSource extractor0 = new PipeRealtimeDataRegionLogSource();
        final PipeRealtimeDataRegionHybridSource extractor1 =
            new PipeRealtimeDataRegionHybridSource();
        final PipeRealtimeDataRegionTsFileSource extractor2 =
            new PipeRealtimeDataRegionTsFileSource();
        final PipeRealtimeDataRegionHybridSource extractor3 =
            new PipeRealtimeDataRegionHybridSource()) {

      final PipeParameters parameters0 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeParameters parameters1 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              });
      final PipeParameters parameters2 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeParameters parameters3 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              });

      final PipeTaskRuntimeConfiguration configuration0 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1", 1, dataRegion1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration1 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1", 1, dataRegion1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration2 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1", 1, dataRegion2, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration3 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1", 1, dataRegion2, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));

      // Some parameters of extractor are validated and initialized during the validation process.
      extractor0.validate(new PipeParameterValidator(parameters0));
      extractor0.customize(parameters0, configuration0);
      extractor1.validate(new PipeParameterValidator(parameters1));
      extractor1.customize(parameters1, configuration1);
      extractor2.validate(new PipeParameterValidator(parameters2));
      extractor2.customize(parameters2, configuration2);
      extractor3.validate(new PipeParameterValidator(parameters3));
      extractor3.customize(parameters3, configuration3);

      final PipeRealtimeDataRegionSource[] extractors =
          new PipeRealtimeDataRegionSource[] {extractor0, extractor1, extractor2, extractor3};

      // start extractor 0, 1
      extractors[0].start();
      extractors[1].start();

      // test result of extractor 0, 1
      final int writeNum = 10;
      List<Future<?>> writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, 0),
              write2DataRegion(writeNum, dataRegion2, 0));

      alive.set(true);
      List<Future<?>> listenFutures =
          Arrays.asList(
              listen(
                  extractors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[1], event -> 1, writeNum));

      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
      } catch (final TimeoutException e) {
        LOGGER.warn("Time out when listening extractor", e);
        alive.set(false);
        Assert.fail();
      }
      writeFutures.forEach(
          future -> {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });

      // start extractor 2, 3
      extractors[2].start();
      extractors[3].start();

      // test result of extractor 0 - 3
      writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, writeNum),
              write2DataRegion(writeNum, dataRegion2, writeNum));

      alive.set(true);
      listenFutures =
          Arrays.asList(
              listen(
                  extractors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[1], event -> 1, writeNum),
              listen(
                  extractors[2],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[3], event -> 1, writeNum));
      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
        listenFutures.get(2).get(10, TimeUnit.MINUTES);
        listenFutures.get(3).get(10, TimeUnit.MINUTES);
      } catch (final TimeoutException e) {
        LOGGER.warn("Time out when listening extractor", e);
        alive.set(false);
        Assert.fail();
      }
      writeFutures.forEach(
          future -> {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testListenToTsFileSkipsAssignerWithoutTsFileSource() throws Exception {
    try (final NoTsFileRealtimeDataRegionSource extractor =
        new NoTsFileRealtimeDataRegionSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1", 1, dataRegion1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);
      extractor.start();

      final File dataRegionDir =
          new File(tsFileDir.getPath() + File.separator + dataRegion1 + File.separator + "0");
      final boolean ignored = dataRegionDir.mkdirs();
      final File tsFile = new File(dataRegionDir, "0-0-0-0.tsfile");
      Assert.assertTrue(tsFile.createNewFile());

      final TsFileResource resource = new TsFileResource(tsFile);
      resource.updateStartTime(
          IDeviceID.Factory.DEFAULT_FACTORY.create(
              String.join(TsFileConstant.PATH_SEPARATOR, device)),
          0);
      resource.close();

      PipeInsertionDataNodeListener.getInstance()
          .listenToTsFile(dataRegion1, Integer.toString(dataRegion1), resource, false);

      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
      while (System.currentTimeMillis() < deadline
          && extractor.getObservedTsFileEventCount() == 0) {
        TimeUnit.MILLISECONDS.sleep(10);
      }

      Assert.assertEquals(0, extractor.getObservedTsFileEventCount());
    }
  }

  @Test
  public void testHybridSourceReportsTsFileEpochDegradedStatus() throws Exception {
    registerTestPipeMeta();

    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME,
                  TEST_PIPE_CREATION_TIME,
                  dataRegion1,
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource resource = createTsFileResource(dataRegion1, "100-100-0-0.tsfile");
      final PipeRealtimeEvent tabletEvent =
          PipeRealtimeEventFactory.createRealtimeEvent(
              false, "root.sg", createInsertRowNode("degraded-tablet", "a"), resource);

      Assert.assertTrue(tabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tabletEvent);
      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());

      tabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);
      Assert.assertNull(extractor.supply());
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final PipeRealtimeEvent tsFileEvent =
          PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", resource, false);

      Assert.assertTrue(tsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tsFileEvent);
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final Event suppliedEvent = extractor.supply();
      Assert.assertTrue(suppliedEvent instanceof TsFileInsertionEvent);
      releaseSuppliedEvent(suppliedEvent);
      Assert.assertNull(getGlobalTsFileEpochDegraded());
    }

    Assert.assertNull(getGlobalTsFileEpochDegraded());
  }

  @Test
  public void testHybridSourceRegionLevelDowngradingIsPipeSpecific() throws Exception {
    try (final PipeRealtimeDataRegionHybridSource disabledExtractor =
            new PipeRealtimeDataRegionHybridSource();
        final PipeRealtimeDataRegionHybridSource enabledExtractor =
            new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters disabledParameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeParameters enabledParameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.EXTRACTOR_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });

      final PipeTaskRuntimeConfiguration disabledConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "region-level-downgrading-disabled",
                  TEST_PIPE_CREATION_TIME,
                  dataRegion1,
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration enabledConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "region-level-downgrading-enabled",
                  TEST_PIPE_CREATION_TIME,
                  dataRegion1,
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));

      disabledExtractor.validate(new PipeParameterValidator(disabledParameters));
      disabledExtractor.customize(disabledParameters, disabledConfiguration);
      enabledExtractor.validate(new PipeParameterValidator(enabledParameters));
      enabledExtractor.customize(enabledParameters, enabledConfiguration);

      Assert.assertFalse(isRegionLevelDowngradingEnabled(disabledExtractor));
      Assert.assertTrue(isRegionLevelDowngradingEnabled(enabledExtractor));
    }
  }

  @Test
  public void testHybridSourceRegionLevelDowngradingWaitsForTsFileCommit() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource firstResource = createTsFileResource(dataRegion1, "101-101-0-0.tsfile");
      final PipeRealtimeEvent firstTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("first-degraded-tablet", "a"),
                  firstResource),
              extractor,
              pipeTaskMeta);

      Assert.assertTrue(firstTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTabletEvent);
      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());

      firstTabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);

      // Queue a tablet from another epoch before the first epoch triggers region-level
      // downgrading. It should be buffered while the degraded TsFile is being sent.
      final TsFileResource secondResource = createTsFileResource(dataRegion1, "102-102-0-0.tsfile");
      final PipeRealtimeEvent secondTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("second-degraded-tablet", "a"),
                  secondResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTabletEvent);
      Assert.assertEquals(
          TsFileEpoch.State.USING_TABLET, secondTabletEvent.getTsFileEpoch().getState(extractor));

      Assert.assertNull(extractor.supply());
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());
      Assert.assertEquals(
          TsFileEpoch.State.USING_TABLET, secondTabletEvent.getTsFileEpoch().getState(extractor));
      Assert.assertFalse(secondTabletEvent.getEvent().isReleased());

      // Simulate that the buffered tablet is evicted before the previous degraded TsFile is
      // committed. The second epoch should then continue region-level downgrading with its TsFile.
      secondTabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);

      final PipeRealtimeEvent firstTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", firstResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(firstTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTsFileEvent);
      Assert.assertEquals(
          TsFileEpoch.State.USING_TSFILE, firstTsFileEvent.getTsFileEpoch().getState(extractor));

      final Event firstSuppliedTsFile = extractor.supply();
      Assert.assertTrue(firstSuppliedTsFile instanceof TsFileInsertionEvent);
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final PipeRealtimeEvent secondTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", secondResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTsFileEvent);

      // The second TsFile stays in the source until the first TsFile is committed downstream.
      Assert.assertNull(extractor.supply());
      final PipeTsFileInsertionEvent suppliedFirstTsFile =
          (PipeTsFileInsertionEvent) firstSuppliedTsFile;
      suppliedFirstTsFile.registerGeneratedTabletInsertionEvent();
      suppliedFirstTsFile.registerGeneratedTabletInsertionEvent();
      suppliedFirstTsFile.markGeneratedTabletInsertionEventsParsingCompleted();
      final PipeRawTabletInsertionEvent firstGeneratedTabletEvent =
          createGeneratedTabletEvent(suppliedFirstTsFile, pipeTaskMeta, "first-generated");
      final PipeRawTabletInsertionEvent secondGeneratedTabletEvent =
          createGeneratedTabletEvent(suppliedFirstTsFile, pipeTaskMeta, "second-generated");
      Assert.assertTrue(firstGeneratedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      Assert.assertTrue(secondGeneratedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      Assert.assertTrue(suppliedFirstTsFile.decreaseReferenceCount(TEST_REFERENCE_HOLDER, false));
      commitSuppliedEvent(firstGeneratedTabletEvent, commitManager);
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());
      Assert.assertEquals(2, getActiveTsFileEpochCount(extractor));
      Assert.assertEquals(1, getInFlightTsFileCount(extractor));
      commitSuppliedEvent(secondGeneratedTabletEvent, commitManager);
      Assert.assertEquals(1, getActiveTsFileEpochCount(extractor));
      Assert.assertEquals(0, getInFlightTsFileCount(extractor));
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final Event secondSuppliedTsFile = extractor.supply();
      Assert.assertTrue(secondSuppliedTsFile instanceof TsFileInsertionEvent);
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      commitSuppliedEvent(secondSuppliedTsFile, commitManager);
      Assert.assertEquals(0, getActiveTsFileEpochCount(extractor));
      Assert.assertEquals(0, getInFlightTsFileCount(extractor));
      Assert.assertNull(getGlobalTsFileEpochDegraded());
      Assert.assertNull(extractor.supply());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  @Test
  public void testGeneratedTabletTransferWaitsForAllTabletCommits() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    final String dedupScopeId = "generated-tablet-transfer-test";
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try {
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final TsFileResource resource = createTsFileResource(dataRegion1, "110-110-0-0.tsfile");
      final PipeTsFileInsertionEvent tsFileEvent =
          new PipeTsFileInsertionEvent(false, "root.sg", resource, false)
              .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                  TEST_PIPE_NAME,
                  TEST_PIPE_CREATION_TIME,
                  pipeTaskMeta,
                  null,
                  null,
                  null,
                  null,
                  null,
                  true,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE);
      tsFileEvent.bindTsFileDedupScopeID(dedupScopeId);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .registerProgressIndex(dataRegion1, dedupScopeId, resource);

      final AtomicBoolean transferred = new AtomicBoolean(false);
      tsFileEvent.addOnTransferredHook(() -> transferred.set(true));
      tsFileEvent.registerGeneratedTabletInsertionEvent();
      tsFileEvent.registerGeneratedTabletInsertionEvent();

      // The default PipeProcessor path iterates toTabletInsertionEvents() directly and does not
      // report parser completion. The source TsFile commit is still the boundary before generated
      // tablet commits.
      tsFileEvent.skipReportOnCommit();
      tsFileEvent.getOnCommittedHooks().forEach(Runnable::run);

      final PipeRawTabletInsertionEvent firstGeneratedTabletEvent =
          createGeneratedTabletEvent(tsFileEvent, pipeTaskMeta, "first", false);
      final PipeRawTabletInsertionEvent secondGeneratedTabletEvent =
          createGeneratedTabletEvent(tsFileEvent, pipeTaskMeta, "second", true);
      Assert.assertTrue(firstGeneratedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      Assert.assertTrue(secondGeneratedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));

      commitSuppliedEvent(firstGeneratedTabletEvent, commitManager);
      Assert.assertFalse(transferred.get());
      Assert.assertTrue(
          PipeTsFileEpochProgressIndexKeeper.getInstance()
              .containsTsFile(dataRegion1, dedupScopeId, resource.getTsFilePath()));

      commitSuppliedEvent(secondGeneratedTabletEvent, commitManager);
      Assert.assertTrue(transferred.get());
      Assert.assertFalse(
          PipeTsFileEpochProgressIndexKeeper.getInstance()
              .containsTsFile(dataRegion1, dedupScopeId, resource.getTsFilePath()));
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .clearProgressIndex(dataRegion1, dedupScopeId);
    }
  }

  @Test
  public void testGeneratedTabletTransferWaitsForDeferredGeneration() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    final String dedupScopeId = "deferred-generated-tablet-transfer-test";
    try {
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final TsFileResource resource = createTsFileResource(dataRegion1, "111-111-0-0.tsfile");
      final PipeTsFileInsertionEvent tsFileEvent =
          new PipeTsFileInsertionEvent(false, "root.sg", resource, false)
              .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                  TEST_PIPE_NAME,
                  TEST_PIPE_CREATION_TIME,
                  pipeTaskMeta,
                  null,
                  null,
                  null,
                  null,
                  null,
                  true,
                  Long.MIN_VALUE,
                  Long.MAX_VALUE);
      tsFileEvent.bindTsFileDedupScopeID(dedupScopeId);

      final AtomicBoolean transferred = new AtomicBoolean(false);
      tsFileEvent.addOnTransferredHook(() -> transferred.set(true));
      tsFileEvent.markGeneratedTabletInsertionEventsParsingStarted();
      tsFileEvent.skipReportOnCommit();
      tsFileEvent.getOnCommittedHooks().forEach(Runnable::run);
      Assert.assertFalse(transferred.get());

      tsFileEvent.registerGeneratedTabletInsertionEvent();
      tsFileEvent.markGeneratedTabletInsertionEventsParsingCompleted();
      final PipeRawTabletInsertionEvent generatedTabletEvent =
          createGeneratedTabletEvent(tsFileEvent, pipeTaskMeta, "deferred");
      Assert.assertTrue(generatedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      commitSuppliedEvent(generatedTabletEvent, commitManager);
      Assert.assertTrue(transferred.get());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .clearProgressIndex(dataRegion1, dedupScopeId);
    }
  }

  @Test
  public void testHybridSourceClearsInFlightTsFileWhenSuppliedEventIsDiscarded() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource resource = createTsFileResource(dataRegion1, "112-112-0-0.tsfile");
      final PipeRealtimeEvent tabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false, "root.sg", createInsertRowNode("discarded-tsfile-tablet", "a"), resource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(tabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tabletEvent);
      tabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);
      Assert.assertNull(extractor.supply());
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final PipeRealtimeEvent tsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", resource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(tsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tsFileEvent);

      final Event suppliedTsFile = extractor.supply();
      Assert.assertTrue(suppliedTsFile instanceof TsFileInsertionEvent);
      Assert.assertEquals(1, getInFlightTsFileCount(extractor));

      ((EnrichedEvent) suppliedTsFile).clearReferenceCount(TEST_REFERENCE_HOLDER);
      Assert.assertEquals(0, getInFlightTsFileCount(extractor));
      Assert.assertNull(getGlobalTsFileEpochDegraded());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  @Test
  public void testHybridSourceCompensatesForDiscardedGeneratedTabletEvents() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource resource = createTsFileResource(dataRegion1, "113-113-0-0.tsfile");
      final PipeRealtimeEvent tabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("discarded-generated-tablet", "a"),
                  resource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(tabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tabletEvent);
      tabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);
      Assert.assertNull(extractor.supply());

      final PipeRealtimeEvent tsFileRealtimeEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", resource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(tsFileRealtimeEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(tsFileRealtimeEvent);
      final PipeTsFileInsertionEvent suppliedTsFile = (PipeTsFileInsertionEvent) extractor.supply();
      Assert.assertNotNull(suppliedTsFile);
      Assert.assertEquals(1, getInFlightTsFileCount(extractor));

      suppliedTsFile.registerGeneratedTabletInsertionEvent();
      suppliedTsFile.registerGeneratedTabletInsertionEvent();
      suppliedTsFile.markGeneratedTabletInsertionEventsParsingCompleted();
      final PipeRawTabletInsertionEvent firstGeneratedTablet =
          createGeneratedTabletEvent(suppliedTsFile, pipeTaskMeta, "discarded-first");
      final PipeRawTabletInsertionEvent secondGeneratedTablet =
          createGeneratedTabletEvent(suppliedTsFile, pipeTaskMeta, "discarded-second");
      firstGeneratedTablet.markAsGeneratedEventRegisteredWithSource();
      secondGeneratedTablet.markAsGeneratedEventRegisteredWithSource();
      Assert.assertTrue(firstGeneratedTablet.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      Assert.assertTrue(secondGeneratedTablet.increaseReferenceCount(TEST_REFERENCE_HOLDER));

      commitManager.enrichWithCommitterKeyAndCommitId(
          suppliedTsFile, TEST_PIPE_CREATION_TIME, dataRegion1);
      Assert.assertTrue(suppliedTsFile.decreaseReferenceCount(TEST_REFERENCE_HOLDER, false));
      firstGeneratedTablet.clearReferenceCount(TEST_REFERENCE_HOLDER);
      Assert.assertEquals(1, getInFlightTsFileCount(extractor));
      secondGeneratedTablet.clearReferenceCount(TEST_REFERENCE_HOLDER);

      Assert.assertEquals(0, getInFlightTsFileCount(extractor));
      Assert.assertNull(getGlobalTsFileEpochDegraded());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  @Test
  public void testHybridSourceRegionLevelDowngradingResumesCompleteBufferedTablets()
      throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource firstResource = createTsFileResource(dataRegion1, "103-103-0-0.tsfile");
      final PipeRealtimeEvent firstTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("first-degraded-tablet", "a"),
                  firstResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(firstTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTabletEvent);
      firstTabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);

      final TsFileResource secondResource = createTsFileResource(dataRegion1, "104-104-0-0.tsfile");
      final PipeRealtimeEvent secondTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("fully-buffered-tablet", "a"),
                  secondResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTabletEvent);

      Assert.assertNull(extractor.supply());
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());
      Assert.assertEquals(
          TsFileEpoch.State.USING_TABLET, secondTabletEvent.getTsFileEpoch().getState(extractor));
      Assert.assertFalse(secondTabletEvent.getEvent().isReleased());

      final PipeRealtimeEvent firstTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", firstResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(firstTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTsFileEvent);
      final Event firstSuppliedTsFile = extractor.supply();
      Assert.assertTrue(firstSuppliedTsFile instanceof TsFileInsertionEvent);

      commitLastGeneratedTabletEvent(
          (PipeTsFileInsertionEvent) firstSuppliedTsFile, commitManager, pipeTaskMeta);
      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());

      // The latest TsFile is still open. Since all of its requests survived in memory at the
      // commit boundary above, later writes of the same TsFile should immediately continue on the
      // realtime path instead of waiting for another flush.
      final PipeRealtimeEvent newRealtimeTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("new-realtime-tablet", "a"),
                  secondResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(newRealtimeTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(newRealtimeTabletEvent);

      final Event resumedTabletEvent = extractor.supply();
      Assert.assertTrue(resumedTabletEvent instanceof TabletInsertionEvent);
      Assert.assertSame(secondTabletEvent.getEvent(), resumedTabletEvent);
      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());
      commitSuppliedEvent(resumedTabletEvent, commitManager);

      final Event newSuppliedTabletEvent = extractor.supply();
      Assert.assertTrue(newSuppliedTabletEvent instanceof TabletInsertionEvent);
      Assert.assertSame(newRealtimeTabletEvent.getEvent(), newSuppliedTabletEvent);
      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());
      commitSuppliedEvent(newSuppliedTabletEvent, commitManager);

      final PipeRealtimeEvent secondTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", secondResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTsFileEvent);

      // The second TsFile is no longer needed because all of its tablets survived buffering.
      Assert.assertNull(extractor.supply());
      Assert.assertNull(getGlobalTsFileEpochDegraded());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  @Test
  public void testHybridSourceRegionLevelDowngradingOnlyCachesLatestTsFile() throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource firstResource = createTsFileResource(dataRegion1, "107-107-0-0.tsfile");
      final PipeRealtimeEvent firstTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("first-degraded-tablet", "a"),
                  firstResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(firstTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTabletEvent);
      firstTabletEvent.clearReferenceCount(TEST_REFERENCE_HOLDER);

      Assert.assertNull(extractor.supply());
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final TsFileResource secondResource = createTsFileResource(dataRegion1, "108-108-0-0.tsfile");
      final PipeRealtimeEvent secondTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("second-buffered-tablet", "a"),
                  secondResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTabletEvent);

      final PipeRealtimeEvent secondTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", secondResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(secondTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(secondTsFileEvent);

      final TsFileResource thirdResource = createTsFileResource(dataRegion1, "109-109-0-0.tsfile");
      final PipeRealtimeEvent thirdTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("latest-buffered-tablet", "a"),
                  thirdResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(thirdTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(thirdTabletEvent);

      // Once a newer epoch appears, the former tail is downgraded even if all its tablets are
      // still available. This bounds the region-level cache to the latest TsFile.
      Assert.assertEquals(
          TsFileEpoch.State.USING_TSFILE, secondTabletEvent.getTsFileEpoch().getState(extractor));
      Assert.assertTrue(secondTabletEvent.getEvent().isReleased());
      Assert.assertEquals(
          TsFileEpoch.State.USING_TABLET, thirdTabletEvent.getTsFileEpoch().getState(extractor));
      Assert.assertFalse(thirdTabletEvent.getEvent().isReleased());

      // Extract the first TsFile after the second one to verify that downgrade order, rather than
      // flush completion order, decides which file can pass downstream.
      final PipeRealtimeEvent firstTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", firstResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(firstTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(firstTsFileEvent);

      final Event firstSuppliedTsFile = extractor.supply();
      Assert.assertTrue(firstSuppliedTsFile instanceof TsFileInsertionEvent);
      Assert.assertSame(firstTsFileEvent.getEvent(), firstSuppliedTsFile);
      commitSuppliedEvent(firstSuppliedTsFile, commitManager);

      final Event secondSuppliedTsFile = extractor.supply();
      Assert.assertTrue(secondSuppliedTsFile instanceof TsFileInsertionEvent);
      Assert.assertSame(secondTsFileEvent.getEvent(), secondSuppliedTsFile);
      commitSuppliedEvent(secondSuppliedTsFile, commitManager);

      Assert.assertEquals(Boolean.FALSE, getGlobalTsFileEpochDegraded());
      final Event resumedLatestTablet = extractor.supply();
      Assert.assertTrue(resumedLatestTablet instanceof TabletInsertionEvent);
      Assert.assertSame(thirdTabletEvent.getEvent(), resumedLatestTablet);
      commitSuppliedEvent(resumedLatestTablet, commitManager);

      final PipeRealtimeEvent thirdTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", thirdResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(thirdTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(thirdTsFileEvent);

      Assert.assertNull(extractor.supply());
      Assert.assertNull(getGlobalTsFileEpochDegraded());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  @Test
  public void testHybridSourceRegionLevelDowngradingPreservesPreviouslyQueuedEvents()
      throws Exception {
    registerTestPipeMeta();

    final PipeEventCommitManager commitManager = PipeEventCommitManager.getInstance();
    commitManager.register(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, "test");
    try (final PipeRealtimeDataRegionHybridSource extractor =
        new PipeRealtimeDataRegionHybridSource()) {
      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                  put(
                      PipeSourceConstant.SOURCE_REALTIME_REGION_LEVEL_DOWNGRADING_KEY,
                      Boolean.TRUE.toString());
                }
              });
      final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
      final PipeTaskRuntimeConfiguration configuration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1, pipeTaskMeta));

      extractor.validate(new PipeParameterValidator(parameters));
      extractor.customize(parameters, configuration);

      final TsFileResource olderResource = createTsFileResource(dataRegion1, "105-105-0-0.tsfile");
      final PipeRealtimeEvent olderTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("queued-before-downgrading", "a"),
                  olderResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(olderTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(olderTabletEvent);

      // Seal the older epoch while leaving its tablet queued in the source.
      final PipeRealtimeEvent olderTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(false, "root.sg", olderResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(olderTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(olderTsFileEvent);

      final TsFileResource degradedResource =
          createTsFileResource(dataRegion1, "106-106-0-0.tsfile");
      final PipeRealtimeEvent degradedTabletEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false,
                  "root.sg",
                  createInsertRowNode("trigger-region-downgrading", "a"),
                  degradedResource),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(degradedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));

      CommonDescriptor.getInstance().getConfig().setPipeTotalFloatingMemoryProportion(0);
      try {
        extractor.extract(degradedTabletEvent);
      } finally {
        CommonDescriptor.getInstance()
            .getConfig()
            .setPipeTotalFloatingMemoryProportion(pipeTotalFloatingMemoryProportion);
      }
      Assert.assertEquals(
          TsFileEpoch.State.USING_TSFILE, degradedTabletEvent.getTsFileEpoch().getState(extractor));
      Assert.assertEquals(Boolean.TRUE, getGlobalTsFileEpochDegraded());

      final PipeRealtimeEvent degradedTsFileEvent =
          bindToTestPipe(
              PipeRealtimeEventFactory.createRealtimeEvent(
                  false, "root.sg", degradedResource, false),
              extractor,
              pipeTaskMeta);
      Assert.assertTrue(degradedTsFileEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
      extractor.extract(degradedTsFileEvent);

      // The tablet that was already queued before downgrading must not be overtaken by the later
      // degraded TsFile.
      final Event firstSuppliedEvent = extractor.supply();
      Assert.assertTrue(firstSuppliedEvent instanceof TabletInsertionEvent);
      Assert.assertSame(olderTabletEvent.getEvent(), firstSuppliedEvent);
      commitSuppliedEvent(firstSuppliedEvent, commitManager);

      final Event secondSuppliedEvent = extractor.supply();
      Assert.assertTrue(secondSuppliedEvent instanceof TsFileInsertionEvent);
      Assert.assertSame(degradedTsFileEvent.getEvent(), secondSuppliedEvent);
      commitSuppliedEvent(secondSuppliedEvent, commitManager);

      Assert.assertNull(getGlobalTsFileEpochDegraded());
      Assert.assertNull(extractor.supply());
    } finally {
      commitManager.deregister(TEST_PIPE_NAME, TEST_PIPE_CREATION_TIME, dataRegion1);
    }
  }

  private Future<?> write2DataRegion(
      final int writeNum, final int dataRegionId, final int startNum) {
    final File dataRegionDir =
        new File(tsFileDir.getPath() + File.separator + dataRegionId + File.separator + "0");
    final boolean ignored = dataRegionDir.mkdirs();
    return writeService.submit(
        () -> {
          for (int i = startNum; i < startNum + writeNum; ++i) {
            final File tsFile = new File(dataRegionDir, String.format("%s-%s-0-0.tsfile", i, i));
            try {
              final boolean ignored1 = tsFile.createNewFile();
            } catch (final IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            final TsFileResource resource = new TsFileResource(tsFile);
            resource.updateStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    String.join(TsFileConstant.PATH_SEPARATOR, device)),
                0);

            try {
              resource.close();
            } catch (final IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    Integer.toString(dataRegionId),
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"a"},
                        new TSDataType[] {TSDataType.INT32},
                        0,
                        new Integer[] {1},
                        false),
                    resource);
            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    Integer.toString(dataRegionId),
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"b"},
                        new TSDataType[] {TSDataType.INT32},
                        0,
                        new Integer[] {1},
                        false),
                    resource);
            PipeInsertionDataNodeListener.getInstance()
                .listenToTsFile(dataRegionId, Integer.toString(dataRegionId), resource, false);
          }
        });
  }

  private Future<?> listen(
      final PipeRealtimeDataRegionSource extractor,
      final Function<Event, Integer> weight,
      final int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive.get() && eventNum < expectNum) {
              Event event;
              try {
                event = extractor.supply();
              } catch (final Exception e) {
                throw new RuntimeException(e);
              }
              if (event != null) {
                eventNum += weight.apply(event);
              }
            }
          } finally {
            Assert.assertEquals(expectNum, eventNum);
          }
        });
  }

  private TsFileResource createTsFileResource(final int dataRegionId, final String fileName)
      throws IOException {
    final File dataRegionDir =
        new File(tsFileDir.getPath() + File.separator + dataRegionId + File.separator + "0");
    Assert.assertTrue(dataRegionDir.mkdirs() || dataRegionDir.isDirectory());

    final File tsFile = new File(dataRegionDir, fileName);
    Assert.assertTrue(tsFile.createNewFile());

    final TsFileResource resource = new TsFileResource(tsFile);
    resource.updateStartTime(
        IDeviceID.Factory.DEFAULT_FACTORY.create(
            String.join(TsFileConstant.PATH_SEPARATOR, device)),
        0);
    resource.close();
    return resource;
  }

  private InsertRowNode createInsertRowNode(final String planNodeId, final String measurement)
      throws Exception {
    return new InsertRowNode(
        new PlanNodeId(planNodeId),
        new PartialPath(device),
        false,
        new String[] {measurement},
        new TSDataType[] {TSDataType.INT32},
        0,
        new Integer[] {1},
        false);
  }

  private void registerTestPipeMeta() throws Exception {
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();
    pipeMetaKeeper.acquireWriteLock();
    try {
      pipeMetaKeeper.removePipeMeta(TEST_PIPE_NAME);
      pipeMetaKeeper.addPipeMeta(
          new PipeMeta(
                  new PipeStaticMeta(
                      TEST_PIPE_NAME,
                      TEST_PIPE_CREATION_TIME,
                      new HashMap<>(),
                      new HashMap<>(),
                      new HashMap<>()),
                  new PipeRuntimeMeta())
              .deepCopy4TaskAgent());
    } finally {
      pipeMetaKeeper.releaseWriteLock();
    }
  }

  @Test
  public void testProgressReportExtractionReleasesDroppedEvents() {
    final TestRealtimeDataRegionSource source = new TestRealtimeDataRegionSource();

    final PipeRealtimeEvent heartbeatEvent =
        PipeRealtimeEventFactory.createRealtimeEvent(dataRegion1, false);
    heartbeatEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER);
    source.extractHeartbeatForTest(heartbeatEvent);
    Assert.assertEquals(1, heartbeatEvent.getEvent().getReferenceCount());

    final PipeRealtimeEvent progressEvent = createProgressReportRealtimeEvent();
    progressEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER);
    source.extractProgressReportEventForTest(progressEvent);

    Assert.assertEquals(0, heartbeatEvent.getEvent().getReferenceCount());
    Assert.assertEquals(1, progressEvent.getEvent().getReferenceCount());
    Assert.assertEquals(1, source.getEventCount());

    final PipeRealtimeEvent mergedProgressEvent = createProgressReportRealtimeEvent();
    mergedProgressEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER);
    source.extractProgressReportEventForTest(mergedProgressEvent);

    Assert.assertEquals(0, mergedProgressEvent.getEvent().getReferenceCount());
    Assert.assertEquals(1, progressEvent.getEvent().getReferenceCount());
    Assert.assertEquals(1, source.getEventCount());

    final Event queuedEvent = source.pollForTest();
    Assert.assertSame(progressEvent, queuedEvent);
    ((EnrichedEvent) queuedEvent).clearReferenceCount(TEST_REFERENCE_HOLDER);
  }

  private void removeTestPipeMeta() throws Exception {
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();
    pipeMetaKeeper.acquireWriteLock();
    try {
      pipeMetaKeeper.removePipeMeta(TEST_PIPE_NAME);
    } finally {
      pipeMetaKeeper.releaseWriteLock();
    }
  }

  private Boolean getGlobalTsFileEpochDegraded() throws Exception {
    final PipeMeta pipeMeta = getPipeMetaKeeper().getPipeMeta(TEST_PIPE_NAME);
    Assert.assertNotNull(pipeMeta);
    return ((PipeTemporaryMetaInAgent) pipeMeta.getTemporaryMeta()).getGlobalTsFileEpochDegraded();
  }

  private PipeMetaKeeper getPipeMetaKeeper() throws Exception {
    final Field pipeMetaKeeperField = PipeTaskAgent.class.getDeclaredField("pipeMetaKeeper");
    pipeMetaKeeperField.setAccessible(true);
    return (PipeMetaKeeper) pipeMetaKeeperField.get(PipeDataNodeAgent.task());
  }

  private void releaseSuppliedEvent(final Event event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).clearReferenceCount(TEST_REFERENCE_HOLDER);
    }
  }

  private PipeRealtimeEvent bindToTestPipe(
      final PipeRealtimeEvent event,
      final PipeRealtimeDataRegionSource extractor,
      final PipeTaskMeta pipeTaskMeta) {
    return event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
        TEST_PIPE_NAME,
        TEST_PIPE_CREATION_TIME,
        pipeTaskMeta,
        extractor.getTreePattern(),
        extractor.getTablePattern(),
        String.valueOf(extractor.getUserId()),
        extractor.getUserName(),
        extractor.getCliHostname(),
        extractor.isSkipIfNoPrivileges(),
        extractor.getRealtimeDataExtractionStartTime(),
        extractor.getRealtimeDataExtractionEndTime());
  }

  private void commitSuppliedEvent(final Event event, final PipeEventCommitManager commitManager) {
    final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
    commitManager.enrichWithCommitterKeyAndCommitId(
        enrichedEvent, TEST_PIPE_CREATION_TIME, dataRegion1);
    Assert.assertTrue(enrichedEvent.decreaseReferenceCount(TEST_REFERENCE_HOLDER, true));
  }

  private void commitLastGeneratedTabletEvent(
      final PipeTsFileInsertionEvent tsFileEvent,
      final PipeEventCommitManager commitManager,
      final PipeTaskMeta pipeTaskMeta) {
    tsFileEvent.registerGeneratedTabletInsertionEvent();
    tsFileEvent.markGeneratedTabletInsertionEventsParsingCompleted();
    final PipeRawTabletInsertionEvent generatedTabletEvent =
        createGeneratedTabletEvent(tsFileEvent, pipeTaskMeta, "generated");

    Assert.assertTrue(generatedTabletEvent.increaseReferenceCount(TEST_REFERENCE_HOLDER));
    Assert.assertTrue(tsFileEvent.decreaseReferenceCount(TEST_REFERENCE_HOLDER, false));
    commitSuppliedEvent(generatedTabletEvent, commitManager);
  }

  private PipeRawTabletInsertionEvent createGeneratedTabletEvent(
      final PipeTsFileInsertionEvent tsFileEvent,
      final PipeTaskMeta pipeTaskMeta,
      final String deviceId) {
    return createGeneratedTabletEvent(tsFileEvent, pipeTaskMeta, deviceId, true);
  }

  private PipeRawTabletInsertionEvent createGeneratedTabletEvent(
      final PipeTsFileInsertionEvent tsFileEvent,
      final PipeTaskMeta pipeTaskMeta,
      final String deviceId,
      final boolean needToReport) {
    final Tablet tablet =
        new Tablet(
            "root.sg.d." + deviceId,
            Collections.singletonList(new MeasurementSchema("s", TSDataType.INT32)),
            1);
    return new PipeRawTabletInsertionEvent(
        false,
        "root.sg",
        null,
        null,
        tablet,
        false,
        TEST_PIPE_NAME,
        TEST_PIPE_CREATION_TIME,
        pipeTaskMeta,
        tsFileEvent,
        needToReport);
  }

  private int getActiveTsFileEpochCount(final PipeRealtimeDataRegionHybridSource extractor)
      throws Exception {
    final Field activeTsFileEpochsField =
        PipeRealtimeDataRegionHybridSource.class.getDeclaredField("activeTsFileEpochs");
    activeTsFileEpochsField.setAccessible(true);
    return ((Set<?>) activeTsFileEpochsField.get(extractor)).size();
  }

  private int getInFlightTsFileCount(final PipeRealtimeDataRegionHybridSource extractor)
      throws Exception {
    final Field inFlightTsFileCountField =
        PipeRealtimeDataRegionHybridSource.class.getDeclaredField("inFlightTsFileCount");
    inFlightTsFileCountField.setAccessible(true);
    return inFlightTsFileCountField.getInt(extractor);
  }

  private boolean isRegionLevelDowngradingEnabled(
      final PipeRealtimeDataRegionHybridSource extractor) throws Exception {
    final Field isRegionLevelDowngradingEnabledField =
        PipeRealtimeDataRegionHybridSource.class.getDeclaredField(
            "isRegionLevelDowngradingEnabled");
    isRegionLevelDowngradingEnabledField.setAccessible(true);
    return isRegionLevelDowngradingEnabledField.getBoolean(extractor);
  }

  private PipeRealtimeEvent createProgressReportRealtimeEvent() {
    final ProgressReportEvent progressReportEvent = new ProgressReportEvent(null, 0, null);
    progressReportEvent.bindProgressIndex(MinimumProgressIndex.INSTANCE);
    return PipeRealtimeEventFactory.createRealtimeEvent(progressReportEvent);
  }

  private static class TestRealtimeDataRegionSource extends PipeRealtimeDataRegionSource {

    private void extractHeartbeatForTest(final PipeRealtimeEvent event) {
      extractHeartbeat(event);
    }

    private void extractProgressReportEventForTest(final PipeRealtimeEvent event) {
      extractProgressReportEvent(event);
    }

    private Event pollForTest() {
      return pendingQueue.directPoll();
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      // Not needed in this reference-counting unit test.
    }

    @Override
    public Event supply() {
      return pendingQueue.directPoll();
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return false;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return false;
    }
  }

  private static class NoTsFileRealtimeDataRegionSource extends PipeRealtimeDataRegionSource {

    private final AtomicInteger observedTsFileEventCount = new AtomicInteger(0);

    @Override
    public Event supply() {
      return null;
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      if (event.getEvent() instanceof TsFileInsertionEvent) {
        observedTsFileEventCount.incrementAndGet();
      }
      event.decreaseReferenceCount(NoTsFileRealtimeDataRegionSource.class.getName(), false);
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return false;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return false;
    }

    private int getObservedTsFileEventCount() {
      return observedTsFileEventCount.get();
    }
  }
}
