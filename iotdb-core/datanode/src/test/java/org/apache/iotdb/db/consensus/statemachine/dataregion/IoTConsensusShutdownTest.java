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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.NOP;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALBuffer;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.CheckpointRecoverUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRecoverManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.file.UnsealedTsFileRecoverPerformer;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.WALRecoverListener;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class IoTConsensusShutdownTest {

  private static final String DATABASE = "root.shutdown";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String MEASUREMENT = "s1";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private WALMode previousWalMode;
  private int previousDataNodeId;
  private String previousConsensusProtocol;
  private NodeStatus previousNodeStatus;
  private String previousStatusReason;
  private boolean previousStopping;
  private TestDataRegion dataRegion;
  private IoTConsensusDataRegionStateMachine stateMachine;

  @Before
  public void setUp() throws Exception {
    previousWalMode = config.getWalMode();
    previousDataNodeId = config.getDataNodeId();
    previousConsensusProtocol = config.getDataRegionConsensusProtocolClass();
    previousNodeStatus = commonConfig.getNodeStatus();
    previousStatusReason = commonConfig.getStatusReason();
    previousStopping = commonConfig.isStopping();
    config.setWalMode(WALMode.ASYNC);
    config.setDataNodeId(0);
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    commonConfig.setNodeStatus(NodeStatus.Running);
    commonConfig.setStopping(false);
    EnvironmentUtils.envSetUp();
    dataRegion = new TestDataRegion();
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
    stateMachine = new IoTConsensusDataRegionStateMachine(dataRegion);
  }

  @After
  public void tearDown() throws Exception {
    try {
      commonConfig.setStopping(false);
      commonConfig.setNodeStatus(NodeStatus.Running);
      if (dataRegion != null) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      }
      CompactionTaskManager.getInstance().stop();
      EnvironmentUtils.cleanEnv();
    } finally {
      config.setWalMode(previousWalMode);
      config.setDataNodeId(previousDataNodeId);
      config.setDataRegionConsensusProtocolClass(previousConsensusProtocol);
      commonConfig.setNodeStatus(NodeStatus.Running);
      commonConfig.setNodeStatusWithReason(previousNodeStatus, previousStatusReason);
      commonConfig.setStopping(previousStopping);
    }
  }

  @Test
  public void testQueuedSyncLogDoesNotReopenStorageAfterShutdown() throws Exception {
    writeAndAssertSuccess(1);
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final IoTConsensusServerImpl server = newServer(stateMachine, backgroundTaskService, 2);
    final DeserializedBatchIndexedConsensusRequest request =
        new DeserializedBatchIndexedConsensusRequest(1, 2, 2, 1, 2);
    request.add(insertRow(2));
    request.add(insertRow(3));
    final AtomicReference<TSStatus> status = new AtomicReference<>();
    final Thread syncLogThread = new Thread(() -> status.set(server.syncLog(1, request)));
    try {
      assertFalse(server.isReadOnly());
      syncLogThread.start();
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> syncLogThread.getState() == Thread.State.TIMED_WAITING);

      // Follow the production order: read-only, final flush, storage stop, then WAL stop.
      commonConfig.setStopping(true);
      commonConfig.setNodeStatus(NodeStatus.ReadOnly);
      StorageEngine.getInstance().stopIoTConsensusWrites();
      WALManager.getInstance().waitAllWALFlushed();
      StorageEngine.getInstance().syncCloseAllProcessor();
      final File flushedFile = dataRegion.getSequenceFileList().get(0).getTsFile();
      StorageEngine.getInstance().stop();
      WALManager.getInstance().stop();
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
      assertTrue(syncLogThread.isAlive());

      // An empty preceding batch wakes the ordered request without another storage write.
      server.syncLog(1, new DeserializedBatchIndexedConsensusRequest(0, 0, 0, 1, 0));
      syncLogThread.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(syncLogThread.isAlive());
      assertEquals(2, status.get().getSubStatusSize());
      assertEquals(
          status.get()
              + ", WAL nodes: "
              + WALManager.getInstance().getWALNodesNum()
              + ", working TsFile processors: "
              + dataRegion.getWorkSequenceTsFileProcessors().size(),
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          status.get().getSubStatus().get(0).getCode());
      assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          status.get().getSubStatus().get(1).getCode());
      assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
      assertTrue(dataRegion.getWorkUnsequenceTsFileProcessors().isEmpty());
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
      assertEquals(0, server.getEffectiveSafePhysicalTime(1));
      assertSingleRow(flushedFile, 1);
    } finally {
      syncLogThread.interrupt();
      syncLogThread.join(TimeUnit.SECONDS.toMillis(10));
      backgroundTaskService.shutdownNow();
    }
  }

  @Test
  public void testSyncLogBecomesReadOnlyAfterPreApplyCheck() throws Exception {
    writeAndAssertSuccess(1);
    final CountDownLatch preApplyCheckPassed = new CountDownLatch(1);
    final CountDownLatch resumeApply = new CountDownLatch(1);
    final IoTConsensusDataRegionStateMachine delayedStateMachine = spy(stateMachine);
    doAnswer(
            invocation -> {
              preApplyCheckPassed.countDown();
              assertTrue(resumeApply.await(30, TimeUnit.SECONDS));
              return invocation.callRealMethod();
            })
        .when(delayedStateMachine)
        .write(any(IConsensusRequest.class));
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final ExecutorService syncLogExecutor = Executors.newSingleThreadExecutor();
    final IoTConsensusServerImpl server = newServer(delayedStateMachine, backgroundTaskService, 1);
    final DeserializedBatchIndexedConsensusRequest request =
        new DeserializedBatchIndexedConsensusRequest(1, 2, 2, 1, 2);
    request.add(insertRow(2));
    request.add(insertRow(3));
    final Future<TSStatus> syncLog = syncLogExecutor.submit(() -> server.syncLog(1, request));
    try {
      // The consensus-layer read-only check has returned false, but apply has not started.
      assertTrue(preApplyCheckPassed.await(10, TimeUnit.SECONDS));
      commonConfig.setStopping(true);
      commonConfig.setNodeStatus(NodeStatus.ReadOnly);
      StorageEngine.getInstance().stopIoTConsensusWrites();
      WALManager.getInstance().waitAllWALFlushed();
      StorageEngine.getInstance().syncCloseAllProcessor();
      final File flushedFile = dataRegion.getSequenceFileList().get(0).getTsFile();
      StorageEngine.getInstance().stop();
      WALManager.getInstance().stop();
      assertEquals(0, WALManager.getInstance().getWALNodesNum());

      resumeApply.countDown();
      final TSStatus status = syncLog.get(10, TimeUnit.SECONDS);
      assertEquals(2, status.getSubStatusSize());
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), status.getSubStatus().get(0).getCode());
      assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          status.getSubStatus().get(1).getCode());
      assertEquals(0, server.getEffectiveSafePhysicalTime(1));
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
      assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
      assertTrue(dataRegion.getWorkUnsequenceTsFileProcessors().isEmpty());
      assertSingleRow(flushedFile, 1);
    } finally {
      resumeApply.countDown();
      syncLogExecutor.shutdownNow();
      assertTrue(syncLogExecutor.awaitTermination(10, TimeUnit.SECONDS));
      backgroundTaskService.shutdownNow();
    }
  }

  @Test
  public void testShutdownDrainsAdmittedApplyBeforeClosingStorage() throws Exception {
    checkShutdownDrainsAdmittedApply(false);
  }

  @Test
  public void testShutdownDrainsWalSeparatorBeforeClosingStorage() throws Exception {
    checkShutdownDrainsAdmittedApply(true);
  }

  private void checkShutdownDrainsAdmittedApply(final boolean delaySeparator) throws Exception {
    writeAndAssertSuccess(1);
    final CountDownLatch admittedApply = new CountDownLatch(1);
    final CountDownLatch resumeApply = new CountDownLatch(1);
    final InsertRowNode pendingRow = spy(insertRow(2));
    final InsertRowNode delayedRow =
        doAnswer(
                invocation -> {
                  admittedApply.countDown();
                  assertTrue(resumeApply.await(30, TimeUnit.SECONDS));
                  return invocation.callRealMethod();
                })
            .when(pendingRow);
    if (delaySeparator) {
      delayedRow.getRoutingEpoch();
    } else {
      delayedRow.setLastFragment(true);
    }
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CountDownLatch shutdownStarted = new CountDownLatch(1);
    final CountDownLatch admissionClosed = new CountDownLatch(1);
    final AtomicReference<Thread> shutdownThread = new AtomicReference<>();
    final Future<TSStatus> write =
        executor.submit(() -> stateMachine.write((IConsensusRequest) pendingRow));
    try {
      // This request has passed the storage admission check and holds insertLock.
      assertTrue(admittedApply.await(10, TimeUnit.SECONDS));
      commonConfig.setStopping(true);
      commonConfig.setNodeStatus(NodeStatus.ReadOnly);
      final Future<?> shutdown =
          executor.submit(
              () -> {
                shutdownThread.set(Thread.currentThread());
                shutdownStarted.countDown();
                StorageEngine.getInstance().stopIoTConsensusWrites();
                admissionClosed.countDown();
                WALManager.getInstance().waitAllWALFlushed();
                StorageEngine.getInstance().syncCloseAllProcessor();
                StorageEngine.getInstance().stop();
                WALManager.getInstance().stop();
              });
      assertTrue(shutdownStarted.await(10, TimeUnit.SECONDS));
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () ->
                  admissionClosed.getCount() == 0
                      || shutdownThread.get().getState() == Thread.State.WAITING);
      assertEquals(1, admissionClosed.getCount());
      assertFalse(shutdown.isDone());
      assertFalse(dataRegion.getSequenceFileList().get(0).isClosed());

      resumeApply.countDown();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), write.get(10, TimeUnit.SECONDS).getCode());
      shutdown.get(10, TimeUnit.SECONDS);
      assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1, 2);
      assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
      assertTrue(dataRegion.getWorkUnsequenceTsFileProcessors().isEmpty());
      assertEquals(0, WALManager.getInstance().getWALNodesNum());

      // Stopping storage closes admission permanently, even if the node status changes again.
      commonConfig.setStopping(false);
      commonConfig.setNodeStatus(NodeStatus.Running);
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          stateMachine.write((IConsensusRequest) insertRow(3)).getCode());
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
    } finally {
      resumeApply.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testShutdownDuringWriteRetryDoesNotHoldAdmissionLock() throws Exception {
    writeAndAssertSuccess(1);
    final CountDownLatch retryWaiting = new CountDownLatch(1);
    final CountDownLatch resumeRetry = new CountDownLatch(1);
    stateMachine =
        new IoTConsensusDataRegionStateMachine(dataRegion) {
          @Override
          protected void waitBeforeNextWriteRetry() throws InterruptedException {
            retryWaiting.countDown();
            assertTrue(resumeRetry.await(30, TimeUnit.SECONDS));
          }
        };
    final AtomicInteger applyAttempts = new AtomicInteger();
    final InsertRowNode pendingRow = spy(insertRow(2));
    doAnswer(
            invocation ->
                applyAttempts.incrementAndGet() == 1
                    ? new TSStatus(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode())
                    : invocation.callRealMethod())
        .when(pendingRow)
        .accept(any(IPlanVisitor.class), any(DataRegion.class));
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<TSStatus> write =
        executor.submit(() -> stateMachine.write((IConsensusRequest) pendingRow));
    try {
      assertTrue(retryWaiting.await(10, TimeUnit.SECONDS));
      commonConfig.setStopping(true);
      commonConfig.setNodeStatus(NodeStatus.ReadOnly);
      StorageEngine.getInstance().stopIoTConsensusWrites();
      WALManager.getInstance().waitAllWALFlushed();
      StorageEngine.getInstance().syncCloseAllProcessor();
      final File flushedFile = dataRegion.getSequenceFileList().get(0).getTsFile();
      StorageEngine.getInstance().stop();
      WALManager.getInstance().stop();
      assertFalse(write.isDone());

      resumeRetry.countDown();
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), write.get(10, TimeUnit.SECONDS).getCode());
      // The retry enters the visitor, but its storage admission check rejects it before any WAL
      // or processor can be created.
      assertEquals(2, applyAttempts.get());
      assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
      assertTrue(dataRegion.getWorkUnsequenceTsFileProcessors().isEmpty());
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
      assertSingleRow(flushedFile, 1);
    } finally {
      resumeRetry.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testReadOnlyCanBecomeWritableBeforeShutdown() throws Exception {
    writeAndAssertSuccess(1);
    commonConfig.setNodeStatus(NodeStatus.ReadOnly);
    assertEquals(
        TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
        stateMachine.write((IConsensusRequest) insertRow(2)).getCode());
    commonConfig.setNodeStatus(NodeStatus.Running);
    writeAndAssertSuccess(2);
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1, 2);
  }

  @Test
  public void testWriteWaitingForInsertLockIsRejectedAfterShutdown() throws Exception {
    writeAndAssertSuccess(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final AtomicReference<Thread> writerThread = new AtomicReference<>();
    final Future<TSStatus> write;
    dataRegion.writeLock("testShutdownWhileWaitingForInsertLock");
    try {
      write =
          executor.submit(
              () -> {
                writerThread.set(Thread.currentThread());
                return stateMachine.write((IConsensusRequest) insertRow(2));
              });
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () ->
                  writerThread.get() != null
                      && writerThread.get().getState() == Thread.State.WAITING);
      // Close admission while the writer has already entered the visitor and is queued for
      // insertLock. The stopping and read-only flags deliberately remain unset.
      dataRegion.stopIoTConsensusWrites();
    } finally {
      dataRegion.writeUnlock();
      executor.shutdown();
    }
    try {
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), write.get(10, TimeUnit.SECONDS).getCode());
      dataRegion.syncCloseAllWorkingTsFileProcessors();
      assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testMemoryBackpressureDoesNotBlockShutdownOrFlush() throws Exception {
    writeAndAssertSuccess(1);
    final Field rejectedField = SystemInfo.class.getDeclaredField("rejected");
    rejectedField.setAccessible(true);
    final boolean previousRejected = rejectedField.getBoolean(SystemInfo.getInstance());
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final AtomicReference<Thread> writerThread = new AtomicReference<>();
    rejectedField.setBoolean(SystemInfo.getInstance(), true);
    final Future<TSStatus> write =
        executor.submit(
            () -> {
              writerThread.set(Thread.currentThread());
              return stateMachine.write((IConsensusRequest) insertRow(2));
            });
    try {
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () ->
                  writerThread.get() != null
                      && writerThread.get().getState() == Thread.State.TIMED_WAITING);
      executor.submit(() -> dataRegion.stopIoTConsensusWrites()).get(10, TimeUnit.SECONDS);
      assertFalse(write.isDone());
      // The same insertLock remains available to flush the memory that the writer is waiting for.
      executor
          .submit(() -> dataRegion.syncCloseAllWorkingTsFileProcessors())
          .get(10, TimeUnit.SECONDS);
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), write.get(10, TimeUnit.SECONDS).getCode());
      assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
      assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1);
    } finally {
      rejectedField.setBoolean(SystemInfo.getInstance(), previousRejected);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testAllWritePathsRejectAfterStorageStop() throws Exception {
    writeAndAssertSuccess(1);
    final PlanNodeId id = new PlanNodeId("shutdown");
    final List<PlanNode> writes =
        Arrays.asList(
            insertRow(2),
            insertTablet(2),
            new InsertRowsNode(
                id, Collections.singletonList(0), Collections.singletonList(insertRow(2))),
            new InsertRowsOfOneDeviceNode(
                id, Collections.singletonList(0), Collections.singletonList(insertRow(2))),
            new InsertMultiTabletsNode(
                id, Collections.singletonList(0), Collections.singletonList(insertTablet(2))),
            StatementTestUtils.genInsertRowNode(2),
            StatementTestUtils.genInsertTabletNode(1, 2),
            new DeleteDataNode(
                id, Collections.singletonList(new MeasurementPath(DEVICE + ".s1")), 0, 10),
            new DeleteDataNode(
                id, Collections.singletonList(new MeasurementPath(DATABASE + ".**")), 0, 10),
            tableDeletion());
    StorageEngine.getInstance().stop();
    WALManager.getInstance().stop();
    assertFalse(commonConfig.isReadOnly());
    assertFalse(commonConfig.isStopping());
    for (final PlanNode node : writes) {
      assertEquals(
          node.getClass().getSimpleName(),
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          stateMachine.write((IConsensusRequest) node).getCode());
    }
    dataRegion.insertSeparatorToWAL();
    assertEquals(0, WALManager.getInstance().getWALNodesNum());
    assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
    assertTrue(dataRegion.getWorkUnsequenceTsFileProcessors().isEmpty());
    assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1);
  }

  @Test
  public void testOtherConsensusAppliesRemainWritable() throws Exception {
    final DataRegionStateMachine otherStateMachine = new DataRegionStateMachine(dataRegion);
    commonConfig.setNodeStatus(NodeStatus.ReadOnly);
    commonConfig.setStopping(true);
    try {
      long time = 0;
      for (final String protocol :
          Arrays.asList(
              ConsensusFactory.RATIS_CONSENSUS,
              ConsensusFactory.SIMPLE_CONSENSUS,
              ConsensusFactory.IOT_CONSENSUS_V2)) {
        config.setDataRegionConsensusProtocolClass(protocol);
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            otherStateMachine.write((IConsensusRequest) insertRow(++time)).getCode());
      }
    } finally {
      config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
      commonConfig.setNodeStatus(NodeStatus.Running);
      commonConfig.setStopping(false);
    }
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    assertRows(dataRegion.getSequenceFileList().get(0).getTsFile(), 1, 2, 3);
  }

  @Test
  public void testShutdownDrainsTableDeletionOutsideInsertLock() throws Exception {
    checkShutdownDrainsTableDeletion(false);
  }

  @Test
  public void testFailedTableDeletionDoesNotBlockShutdown() throws Exception {
    checkShutdownDrainsTableDeletion(true);
  }

  private void checkShutdownDrainsTableDeletion(final boolean failDeletion) throws Exception {
    DataNodeTableCache.getInstance()
        .preUpdateTable(DATABASE, StatementTestUtils.genTsTable(), null);
    DataNodeTableCache.getInstance()
        .commitUpdateTable(DATABASE, StatementTestUtils.tableName(), null, null);
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        stateMachine
            .write((IConsensusRequest) StatementTestUtils.genInsertTabletNode(2, 0))
            .getCode());
    dataRegion.syncCloseAllWorkingTsFileProcessors();
    final TsFileResource resource = dataRegion.getSequenceFileList().get(0);
    assertTrue(resource.transformStatus(TsFileResourceStatus.COMPACTION_CANDIDATE));
    assertTrue(resource.transformStatus(TsFileResourceStatus.COMPACTING));
    final ModificationFile modFile = spy(resource.getModFileForWrite());
    resource.setExclusiveModFile(modFile);
    final CountDownLatch modFileClosing = new CountDownLatch(1);
    final CountDownLatch resumeDeletion = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              modFileClosing.countDown();
              assertTrue(resumeDeletion.await(30, TimeUnit.SECONDS));
              if (failDeletion) {
                throw new IOException();
              }
              return invocation.callRealMethod();
            })
        .when(modFile)
        .close();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final AtomicReference<Thread> shutdownThread = new AtomicReference<>();
    final Future<TSStatus> deletion =
        executor.submit(() -> stateMachine.write((IConsensusRequest) tableDeletion()));
    try {
      assertTrue(modFileClosing.await(10, TimeUnit.SECONDS));
      // Mod-file I/O still runs without insertLock, so an ordinary flush can proceed.
      executor.submit(() -> dataRegion.timedFlushSeqMemTable()).get(10, TimeUnit.SECONDS);
      final Future<?> shutdown =
          executor.submit(
              () -> {
                shutdownThread.set(Thread.currentThread());
                commonConfig.setStopping(true);
                StorageEngine.getInstance().stop();
                WALManager.getInstance().stop();
              });
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () ->
                  shutdownThread.get() != null
                      && shutdownThread.get().getState() == Thread.State.WAITING);
      assertFalse(shutdown.isDone());
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          stateMachine.write((IConsensusRequest) insertRow(3)).getCode());
      resumeDeletion.countDown();
      assertEquals(
          (failDeletion ? TSStatusCode.WRITE_PROCESS_ERROR : TSStatusCode.SUCCESS_STATUS)
              .getStatusCode(),
          deletion.get(10, TimeUnit.SECONDS).getCode());
      shutdown.get(10, TimeUnit.SECONDS);
      assertEquals(0, WALManager.getInstance().getWALNodesNum());
    } finally {
      resumeDeletion.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      doAnswer(invocation -> invocation.callRealMethod()).when(modFile).close();
      modFile.close();
    }
  }

  private static RelationalDeleteDataNode tableDeletion() {
    return new RelationalDeleteDataNode(
        new PlanNodeId("shutdown"),
        new TableDeletionEntry(
            new DeletionPredicate(StatementTestUtils.tableName(), new NOP()), new TimeRange(0, 10)),
        DATABASE);
  }

  private IoTConsensusServerImpl newServer(
      IoTConsensusDataRegionStateMachine targetStateMachine,
      ScheduledExecutorService backgroundTaskService,
      int maxPendingBatchesNum)
      throws Exception {
    final Peer thisNode = new Peer(new DataRegionId(0), 0, new TEndPoint("127.0.0.1", 6667));
    return new IoTConsensusServerImpl(
        temporaryFolder.getRoot().getAbsolutePath(),
        null,
        DirectoryStrategyType.SEQUENCE_STRATEGY,
        thisNode,
        Collections.singletonList(thisNode),
        targetStateMachine,
        backgroundTaskService,
        null,
        null,
        IoTConsensusConfig.newBuilder()
            .setReplication(
                IoTConsensusConfig.Replication.newBuilder()
                    .setMaxPendingBatchesNum(maxPendingBatchesNum)
                    .setMaxWaitingTimeForWaitBatchInMs(TimeUnit.MINUTES.toMillis(5))
                    .build())
            .build());
  }

  @Test
  public void testUncheckedWriteAfterWalManagerStopLosesCheckpoint() throws Exception {
    writeAndAssertSuccess(1);
    commonConfig.setNodeStatus(NodeStatus.ReadOnly);
    WALManager.getInstance().waitAllWALFlushed();
    StorageEngine.getInstance().syncCloseAllProcessor();
    final File flushedFile = dataRegion.getSequenceFileList().get(0).getTsFile();
    final WALNode originalWalNode = (WALNode) dataRegion.getWALNode().get();
    StorageEngine.getInstance().stop();
    WALManager.getInstance().stop();

    // Bypass lifecycle admission with the real visitor to reproduce the old late-apply behavior.
    // Unlike a write before WAL stop, this recreates the node in a directory containing closed WAL.
    writeUncheckedAndAssertSuccess(2);
    final WALNode lateWalNode = (WALNode) dataRegion.getWALNode().get();
    assertNotSame(originalWalNode, lateWalNode);
    assertEquals(originalWalNode.getLogDirectory(), lateWalNode.getLogDirectory());
    final File lateFile =
        dataRegion
            .getWorkSequenceTsFileProcessors()
            .iterator()
            .next()
            .getTsFileResource()
            .getTsFile();
    WALManager.getInstance().waitAllWALFlushed();
    WALManager.getInstance().stop();
    // The new CheckpointManager appends another header to the existing _0.checkpoint file. Even
    // with a drained and normally closed WAL, recovery cannot find the late memtable in that file.
    assertTrue(
        CheckpointRecoverUtils.recoverMemTableInfo(lateWalNode.getLogDirectory())
            .getMemTableId2Info()
            .isEmpty());
    dataRegion.forceCloseAllWorkingTsFileProcessors();
    dataRegion.getWorkSequenceTsFileProcessors().clear();
    dataRegion.getWorkUnsequenceTsFileProcessors().clear();

    final WALRecoverManager recoverManager = WALRecoverManager.getInstance();
    recoverManager.clear();
    final UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(
            new TsFileResource(lateFile), true, recovered -> assertFalse(recovered.canWrite()));
    recoverManager.addRecoverPerformer(performer);
    recoverManager.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
    recoverManager.recover();
    assertEquals(WALRecoverListener.Status.SUCCESS, performer.getRecoverListener().waitForResult());
    assertSingleRow(flushedFile, 1);
    assertTrue(performer.getTsFileResource().isEmpty());
  }

  @Test
  public void testAsyncWriteAfterTsFileFlushIsRecoveredWhenWalCloses() throws Exception {
    writeAndAssertSuccess(1);
    commonConfig.setNodeStatus(NodeStatus.ReadOnly);
    WALManager.getInstance().waitAllWALFlushed();
    StorageEngine.getInstance().syncCloseAllProcessor();
    assertTrue(dataRegion.getWorkSequenceTsFileProcessors().isEmpty());
    final File flushedFile = dataRegion.getSequenceFileList().get(0).getTsFile();
    final WALBuffer buffer = ((WALNode) dataRegion.getWALNode().get()).getWALBuffer();

    // Hold the real WAL sync executor so the async ACK cannot imply that the WAL is durable.
    final Field syncBufferThreadField = WALBuffer.class.getDeclaredField("syncBufferThread");
    syncBufferThreadField.setAccessible(true);
    final ExecutorService syncExecutor = (ExecutorService) syncBufferThreadField.get(buffer);
    final CountDownLatch syncBlocked = new CountDownLatch(1);
    final CountDownLatch resumeSync = new CountDownLatch(1);
    final Future<?> blocker =
        syncExecutor.submit(
            () -> {
              syncBlocked.countDown();
              assertTrue(resumeSync.await(30, TimeUnit.SECONDS));
              return null;
            });
    final File lateFile;
    try {
      assertTrue(syncBlocked.await(10, TimeUnit.SECONDS));
      // Call the real visitor directly to model the old unchecked SyncLog apply.
      writeUncheckedAndAssertSuccess(2);
      assertFalse(buffer.isAllWALEntriesConsumed());
      assertEquals(1, dataRegion.getWorkSequenceTsFileProcessors().size());
      final TsFileProcessor processor =
          dataRegion.getWorkSequenceTsFileProcessors().iterator().next();
      assertEquals(1, processor.getWorkMemTable().getTotalPointsNum());
      lateFile = processor.getTsFileResource().getTsFile();
    } finally {
      resumeSync.countDown();
      blocker.get(10, TimeUnit.SECONDS);
    }

    // Normal WAL close drains the late write, even though there is no further TsFile flush.
    WALManager.getInstance().stop();
    dataRegion.forceCloseAllWorkingTsFileProcessors();
    dataRegion.getWorkSequenceTsFileProcessors().clear();
    dataRegion.getWorkUnsequenceTsFileProcessors().clear();
    assertFalse(new File(lateFile.getPath() + TsFileResource.RESOURCE_SUFFIX).exists());

    // Discard the memtable and recover through the actual checkpoint/WAL recovery path.
    final WALRecoverManager recoverManager = WALRecoverManager.getInstance();
    recoverManager.clear();
    final UnsealedTsFileRecoverPerformer performer =
        new UnsealedTsFileRecoverPerformer(
            new TsFileResource(lateFile), true, recovered -> assertFalse(recovered.canWrite()));
    recoverManager.addRecoverPerformer(performer);
    recoverManager.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
    recoverManager.recover();
    assertEquals(WALRecoverListener.Status.SUCCESS, performer.getRecoverListener().waitForResult());
    assertSingleRow(flushedFile, 1);
    assertSingleRow(lateFile, 2);
  }

  private void writeAndAssertSuccess(long time) throws Exception {
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        stateMachine.write((IConsensusRequest) insertRow(time)).getCode());
  }

  private void writeUncheckedAndAssertSuccess(long time) throws Exception {
    dataRegion.bypassWriteChecks = true;
    try {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          insertRow(time).accept(new DataExecutionVisitor(), dataRegion).getCode());
    } finally {
      dataRegion.bypassWriteChecks = false;
    }
  }

  private static class TestDataRegion extends DataRegion {
    // Only the two fault-injection tests bypass admission to reproduce the old late-write bug.
    private boolean bypassWriteChecks;

    private TestDataRegion() throws DataRegionException {
      super(
          TestConstant.OUTPUT_DATA_DIR + "shutdown-info",
          "0",
          new TsFileFlushPolicy.DirectFlushPolicy(),
          DATABASE);
    }

    @Override
    protected void checkIoTConsensusWriteAllowed() throws WriteProcessRejectException {
      if (!bypassWriteChecks) {
        super.checkIoTConsensusWriteAllowed();
      }
    }
  }

  private static InsertTabletNode insertTablet(final long time) throws Exception {
    return new InsertTabletNode(
        new PlanNodeId("shutdown"),
        new PartialPath(DEVICE),
        false,
        new String[] {MEASUREMENT},
        new TSDataType[] {TSDataType.INT32},
        new MeasurementSchema[] {new MeasurementSchema(MEASUREMENT, TSDataType.INT32)},
        new long[] {time},
        null,
        new Object[] {new int[] {(int) time * 10}},
        1);
  }

  private static InsertRowNode insertRow(long time) throws Exception {
    final InsertRowNode node =
        new InsertRowNode(
            new PlanNodeId("shutdown"),
            new PartialPath(DEVICE),
            false,
            new String[] {MEASUREMENT},
            new TSDataType[] {TSDataType.INT32},
            time,
            new Object[] {(int) time * 10},
            false);
    node.setMeasurementSchemas(
        new MeasurementSchema[] {new MeasurementSchema(MEASUREMENT, TSDataType.INT32)});
    return node;
  }

  private static void assertSingleRow(File file, long time) throws Exception {
    assertRows(file, time);
  }

  private static void assertRows(File file, long... times) throws Exception {
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getPath());
        TsFileReader reader = new TsFileReader(sequenceReader)) {
      final QueryDataSet dataSet =
          reader.query(
              QueryExpression.create(
                  Collections.singletonList(new Path(DEVICE, MEASUREMENT, false)), null));
      for (long time : times) {
        assertTrue(dataSet.hasNext());
        final RowRecord row = dataSet.next();
        assertEquals(time, row.getTimestamp());
        assertEquals((int) time * 10, row.getFields().get(0).getIntV());
      }
      assertFalse(dataSet.hasNext());
    }
  }
}
