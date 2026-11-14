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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.JVMCommonUtils;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The research backends' snapshot path at a graceful shutdown: StorageEngine.stop() has started to
 * close the region's TsFile when the consensus layer takes its snapshot. The portable snapshot must
 * hold the TsFile, so that the restart, which replaces the region's files with the snapshot and
 * replays the log only after it, still has these rows.
 */
public class ResearchSnapshotConcurrentCloseTest {
  // Snapshot fixtures write only a few KiB; host free-space ratios must not change their behavior.
  @Rule
  public final ExternalResource snapshotDiskPolicy =
      new ExternalResource() {
        @Override
        protected void before() {
          JVMCommonUtils.setDiskSpaceWarningThreshold(0);
        }

        @Override
        protected void after() {
          JVMCommonUtils.setDiskSpaceWarningThreshold(
              CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold());
        }
      };

  private static final String DATABASE = "root.researchsnapshot";
  private static final String DEVICE = DATABASE + ".d0";
  private static final long ROWS = 10;

  /** Bounds waits that end promptly unless the code under test hangs; no ordering depends on it. */
  private static final long HANG_GUARD_SECONDS = 60;

  private final String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private DataRegion dataRegion;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    dataRegion = new DataRegionTest.DummyDataRegion(systemDir, DATABASE);
    dataRegion.setAllowCompaction(false);
    StorageEngine.getInstance().setDataRegion(new DataRegionId(0), dataRegion);
  }

  @After
  public void tearDown() throws Exception {
    if (dataRegion != null) {
      dataRegion.syncDeleteDataFiles();
      // the region that the snapshot load installed, if any
      StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
    }
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void restartFromSnapshotKeepsTsFileThatStorageEngineWasClosing() throws Exception {
    for (long time = 1; time <= ROWS; time++) {
      TSRecord record = new TSRecord(DEVICE, time);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "s0", String.valueOf(time)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }
    TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    CountDownLatch flushHeld = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    resource
        .getProcessor()
        .addFlushListeners(
            Collections.singletonList(new HoldingFlushListener(flushHeld, releaseFlush)));
    // StorageEngine.stop() moves the working processor into the closing set first.
    List<Future<?>> storageEngineClose = dataRegion.asyncCloseAllWorkingTsFileProcessors();
    assertTrue(flushHeld.await(HANG_GUARD_SECONDS, TimeUnit.SECONDS));
    assertFalse(resource.isClosed());

    ResearchRaftDataRegionStateMachine machine = new ResearchRaftDataRegionStateMachine(dataRegion);
    File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "research-snapshot");
    FutureTask<Boolean> snapshot = new FutureTask<>(() -> machine.takeSnapshot(snapshotDir));
    Thread taker = new Thread(snapshot, "research-snapshot-taker");
    taker.start();
    try {
      awaitFinishedOrWaitingForTheClose(taker);
    } finally {
      releaseFlush.countDown();
    }
    assertTrue(snapshot.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS));
    for (Future<?> close : storageEngineClose) {
      close.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
    }

    // The restart: the snapshot replaces the region's files.
    machine.loadSnapshot(snapshotDir);
    DataRegion restored = StorageEngine.getInstance().getDataRegion(new DataRegionId(0));
    assertNotSame(dataRegion, restored);
    List<TsFileResource> restoredFiles = restored.getTsFileManager().getTsFileList(true);
    assertEquals(1, restoredFiles.size());
    assertEquals(resource.getTsFile().getName(), restoredFiles.get(0).getTsFile().getName());
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE);
    assertEquals(Optional.of(1L), restoredFiles.get(0).getStartTime(device));
    assertEquals(Optional.of(ROWS), restoredFiles.get(0).getEndTime(device));
  }

  /**
   * Returns once {@code taker} has finished or is parked on a close in DataRegion's wait for the
   * closes in flight; both are final while the flush is held.
   */
  private static void awaitFinishedOrWaitingForTheClose(Thread taker) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANG_GUARD_SECONDS);
    while (taker.isAlive() && !waitsForTheClose(taker)) {
      if (System.nanoTime() > deadline) {
        fail(
            "The snapshot neither finished nor waited for the closing TsFileProcessor: "
                + Arrays.toString(taker.getStackTrace()));
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  /** Parked in Future#get called by DataRegion#syncCloseAllTsFileProcessors (not in its lock). */
  private static boolean waitsForTheClose(Thread thread) {
    Thread.State state = thread.getState();
    if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
      return false;
    }
    StackTraceElement[] frames = thread.getStackTrace();
    for (int i = 1; i < frames.length; i++) {
      if (DataRegion.class.getName().equals(frames[i].getClassName())
          && "syncCloseAllTsFileProcessors".equals(frames[i].getMethodName())) {
        return "get".equals(frames[i - 1].getMethodName());
      }
    }
    return false;
  }

  /** Blocks the flush thread between writing the memtable and sealing the TsFile. */
  private static final class HoldingFlushListener implements FlushListener {
    private final CountDownLatch held;
    private final CountDownLatch release;

    private HoldingFlushListener(CountDownLatch held, CountDownLatch release) {
      this.held = held;
      this.release = release;
    }

    @Override
    public void onMemTableFlushStarted(IMemTable memTable) {
      // the hold is placed after the memtable reached the file
    }

    @Override
    public void onMemTableFlushed(IMemTable memTable) {
      held.countDown();
      try {
        // Bounded so that a failing test does not leave a flush thread blocked behind it.
        release.await(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
