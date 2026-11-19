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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.PageCacheDeletionBuffer;
import org.apache.iotdb.db.pipe.receiver.protocol.iotconsensusv2.IoTConsensusV2ReceiverAgent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeletionResourceManagerCleanupTest {
  private static final int REGION_ID = 91821;
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private String previousDirectory;
  private int previousBufferSize;
  private Logger bufferLogger;
  private Logger managerLogger;
  private Appender<ILoggingEvent> appender;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    previousDirectory =
        IoTDBDescriptor.getInstance().getConfig().getIotConsensusV2DeletionFileDir();
    previousBufferSize = PageCacheDeletionBuffer.DAL_BUFFER_SIZE;
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setIotConsensusV2DeletionFileDir(temporaryFolder.getRoot().getAbsolutePath());
    PageCacheDeletionBuffer.setDalBufferSize(1024);
    DeletionResourceManager.buildForTest();
    DeletionResourceManager.onRegionCreated(REGION_ID);
    bufferLogger = (Logger) LoggerFactory.getLogger(PageCacheDeletionBuffer.class);
    appender = mock(Appender.class);
    bufferLogger.addAppender(appender);
    managerLogger = (Logger) LoggerFactory.getLogger(DeletionResourceManager.class);
  }

  @After
  public void tearDown() {
    try {
      DeletionResourceManager.closeAndRemoveDAL(REGION_ID);
    } finally {
      bufferLogger.detachAppender(appender);
      managerLogger.detachAppender(appender);
      DeletionResourceManager.onRegionCreated(REGION_ID);
      IoTDBDescriptor.getInstance().getConfig().setIotConsensusV2DeletionFileDir(previousDirectory);
      PageCacheDeletionBuffer.setDalBufferSize(previousBufferSize);
    }
  }

  @Test
  public void testCleanupWithoutManagerDoesNotStartBuffer() {
    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(regionDirectory().exists());
    verify(appender, never()).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void testCleanupRemovesOrphanedDALWithoutStartingBuffer() throws Exception {
    final File directory = regionDirectory();
    Assert.assertTrue(directory.mkdir());
    Files.write(new File(directory, "_0-0.deletion").toPath(), new byte[] {1, 2, 3});

    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(directory.exists());
    verify(appender, never()).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void testCleanupBlocksRecreationUntilNewRegionLifecycle() {
    final DeletionResourceManager manager = DeletionResourceManager.getInstance(REGION_ID);
    Assert.assertTrue(regionDirectory().exists());

    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);

    Assert.assertFalse(regionDirectory().exists());
    Assert.assertNull(DeletionResourceManager.getInstance(REGION_ID));
    Assert.assertNull(DeletionResourceManager.getInstanceIfPresent(REGION_ID));
    DeletionResourceManager.onRegionCreated(REGION_ID);
    Assert.assertNotSame(manager, DeletionResourceManager.getInstance(REGION_ID));
    Assert.assertTrue(regionDirectory().exists());
  }

  @Test
  public void testLookupDoesNotCreateManager() {
    Assert.assertNull(DeletionResourceManager.getInstanceIfPresent(REGION_ID));
    Assert.assertFalse(regionDirectory().exists());
    verify(appender, never()).doAppend(any(ILoggingEvent.class));
  }

  @Test
  public void testDeletionBarrierRejectsRetainedManagerRegistration() {
    final DeletionResourceManager manager = DeletionResourceManager.getInstance(REGION_ID);
    DeletionResourceManager.markRegionDeleted(REGION_ID);
    Assert.assertNull(DeletionResourceManager.getInstance(REGION_ID));
    final DeletionResource resource =
        manager.registerDeletionResource(mock(AbstractDeleteDataNode.class));
    Assert.assertSame(DeletionResource.Status.FAILURE, resource.waitForResult());
  }

  @Test
  public void testOldManagerCannotWriteOrRemoveNewLifecycleDAL() throws Exception {
    final DeletionResourceManager old = DeletionResourceManager.getInstance(REGION_ID);
    DeletionResourceManager.closeAndRemoveDAL(REGION_ID);
    DeletionResourceManager.onRegionCreated(REGION_ID);
    DeletionResourceManager.getInstance(REGION_ID);
    final File marker = new File(regionDirectory(), "_0-1.deletion");
    Files.write(marker.toPath(), new byte[] {1});
    final DeletionResource rejected =
        old.registerDeletionResource(mock(AbstractDeleteDataNode.class));
    Assert.assertSame(DeletionResource.Status.FAILURE, rejected.waitForResult());
    rejected.removeSelf();
    Assert.assertTrue(marker.exists());
  }

  @Test
  public void testReferenceReleaseDoesNotHoldPersistCallbackMonitor() {
    final AtomicBoolean released = new AtomicBoolean();
    final DeletionResource resource =
        new DeletionResource(
            mock(AbstractDeleteDataNode.class),
            deletion -> {
              Assert.assertFalse(Thread.holdsLock(deletion));
              released.set(true);
            },
            REGION_ID);
    resource.setPipeTaskReferenceCount(1);
    resource.decreaseReference();
    Assert.assertTrue(released.get());
  }

  @Test(timeout = 30000)
  public void testCleanupWaitsForInFlightInitialization() throws Exception {
    final CountDownLatch initializing = new CountDownLatch(1);
    final CountDownLatch finishInitialization = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              final ILoggingEvent event = invocation.getArgument(0);
              if (DataNodePipeMessages.DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING.equals(
                  event.getMessage())) {
                initializing.countDown();
                Assert.assertTrue(finishInitialization.await(10, TimeUnit.SECONDS));
              }
              return null;
            })
        .when(appender)
        .doAppend(any(ILoggingEvent.class));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<DeletionResourceManager> creation =
          executor.submit(() -> DeletionResourceManager.getInstance(REGION_ID));
      Assert.assertTrue(initializing.await(10, TimeUnit.SECONDS));
      final CountDownLatch cleanupStarted = new CountDownLatch(1);
      final Future<?> cleanup =
          executor.submit(
              () -> {
                cleanupStarted.countDown();
                DeletionResourceManager.closeAndRemoveDAL(REGION_ID);
              });
      Assert.assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));
      assertWaiting(cleanup);
      Assert.assertTrue(regionDirectory().exists());
      finishInitialization.countDown();
      creation.get(10, TimeUnit.SECONDS);
      cleanup.get(10, TimeUnit.SECONDS);
      Assert.assertFalse(regionDirectory().exists());
      Assert.assertNull(DeletionResourceManager.getInstance(REGION_ID));
    } finally {
      finishInitialization.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 30000)
  public void testCreationCannotEnterBetweenCloseAndDirectoryRemoval() throws Exception {
    DeletionResourceManager.getInstance(REGION_ID);
    final CountDownLatch managerClosed = new CountDownLatch(1);
    final CountDownLatch removeDirectory = new CountDownLatch(1);
    managerLogger.addAppender(appender);
    doAnswer(
            invocation -> {
              final ILoggingEvent event = invocation.getArgument(0);
              if (DataNodePipeMessages.DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY.equals(
                  event.getMessage())) {
                managerClosed.countDown();
                Assert.assertTrue(removeDirectory.await(10, TimeUnit.SECONDS));
              }
              return null;
            })
        .when(appender)
        .doAppend(any(ILoggingEvent.class));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<?> cleanup =
          executor.submit(() -> DeletionResourceManager.closeAndRemoveDAL(REGION_ID));
      Assert.assertTrue(managerClosed.await(10, TimeUnit.SECONDS));
      final CountDownLatch creationStarted = new CountDownLatch(1);
      final Future<DeletionResourceManager> creation =
          executor.submit(
              () -> {
                creationStarted.countDown();
                return DeletionResourceManager.getInstance(REGION_ID);
              });
      Assert.assertTrue(creationStarted.await(10, TimeUnit.SECONDS));
      assertWaiting(creation);
      removeDirectory.countDown();
      cleanup.get(10, TimeUnit.SECONDS);
      Assert.assertNull(creation.get(10, TimeUnit.SECONDS));
      Assert.assertFalse(regionDirectory().exists());
    } finally {
      removeDirectory.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStorageDeletionStopsReceiverAndAssignerBeforeDALRemoval() throws Exception {
    final DataRegionId regionId = new DataRegionId(REGION_ID);
    final StorageEngine engine = StorageEngine.getInstance();
    final Field regionsField = StorageEngine.class.getDeclaredField("dataRegionMap");
    regionsField.setAccessible(true);
    final Map<DataRegionId, DataRegion> regions =
        (Map<DataRegionId, DataRegion>) regionsField.get(engine);
    final DataRegion region = mock(DataRegion.class);
    when(region.getDatabaseName()).thenReturn("root.dal_cleanup");
    when(region.getDataRegionIdString()).thenReturn(Integer.toString(REGION_ID));
    final PipeInsertionDataNodeListener listener = PipeInsertionDataNodeListener.getInstance();
    final Field assignerField =
        PipeInsertionDataNodeListener.class.getDeclaredField("dataRegionId2Assigner");
    assignerField.setAccessible(true);
    final Map<Integer, PipeDataRegionAssigner> assigners =
        (Map<Integer, PipeDataRegionAssigner>) assignerField.get(listener);
    final PipeDataRegionAssigner assigner = mock(PipeDataRegionAssigner.class);
    final IoTConsensusV2ReceiverAgent receiver = PipeDataNodeAgent.receiver().iotConsensusV2();
    final Field staleField = IoTConsensusV2ReceiverAgent.class.getDeclaredField("staleRegions");
    staleField.setAccessible(true);
    final Set<Integer> stale = (Set<Integer>) staleField.get(receiver);
    DeletionResourceManager.getInstance(REGION_ID);
    doAnswer(
            invocation -> {
              verify(assigner).close();
              Assert.assertFalse(assigners.containsKey(REGION_ID));
              Assert.assertTrue(stale.contains(REGION_ID));
              Assert.assertNull(DeletionResourceManager.getInstance(REGION_ID));
              Assert.assertTrue(regionDirectory().exists());
              DeletionResourceManager.closeAndRemoveDAL(REGION_ID);
              return null;
            })
        .when(region)
        .deleteDALFolderAndClose();
    regions.put(regionId, region);
    assigners.put(REGION_ID, assigner);
    try {
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), engine.deleteDataRegion(regionId).getCode());
      verify(region).deleteDALFolderAndClose();
      Assert.assertFalse(regionDirectory().exists());
    } finally {
      regions.remove(regionId, region);
      assigners.remove(REGION_ID, assigner);
      listener.onRegionCreated(REGION_ID);
      stale.remove(REGION_ID);
    }
  }

  private void assertWaiting(Future<?> operation) throws Exception {
    try {
      operation.get(200, TimeUnit.MILLISECONDS);
      Assert.fail("Operation must wait for the concurrent lifecycle transition");
    } catch (TimeoutException expected) {
      // The other operation is deliberately paused while holding the per-region lock.
    }
  }

  private File regionDirectory() {
    return new File(temporaryFolder.getRoot(), Integer.toString(REGION_ID));
  }
}
