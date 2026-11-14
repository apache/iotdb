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

package org.apache.iotdb.db.storageengine.dataregion.snapshot;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionTest;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushListener;
import org.apache.iotdb.db.storageengine.dataregion.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.memory.AbstractPoolManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A snapshot that flushes first must also cover the TsFiles that another thread started to close,
 * and must fail rather than leave out or wait forever for a TsFile that a close did not seal.
 *
 * <p>At a graceful DataNode shutdown, StorageEngine.stop() closes every working TsFileProcessor
 * while the consensus layer takes a DataRegion snapshot on its own thread. When the close moves a
 * processor into the closing set first, the snapshot's own flush has nothing left to close, and a
 * snapshot that does not wait for that close skips the still unsealed TsFile. A restart that loads
 * the snapshot then replaces the region's files with ones that lack these rows.
 */
public class SnapshotTakerConcurrentCloseTest {

  private static final String DATABASE = "root.snapshotrace";
  private static final String DEVICE = DATABASE + ".d0";
  private static final String MEASUREMENT = "s0";
  private static final long ROWS = 10;

  /** Bounds waits that end promptly unless the code under test hangs; no ordering depends on it. */
  private static final long HANG_GUARD_SECONDS = 60;

  /**
   * Shorter than {@link SnapshotTaker#DEFAULT_SEAL_WAIT_MILLIS}: a snapshot that returns within it
   * did not just run out of time.
   */
  private static final long PROMPT_SECONDS = 30;

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
      // A processor whose TsFile could not be sealed stays closing, and deleting the region would
      // wait for it forever.
      closingProcessors(dataRegion, true).clear();
      closingProcessors(dataRegion, false).clear();
      dataRegion.syncDeleteDataFiles();
      StorageEngine.getInstance().deleteDataRegion(new DataRegionId(0));
    }
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void snapshotIncludesTsFileThatAnotherThreadIsClosing() throws Exception {
    insertRows();
    TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);

    // Hold the flush after the memtable is written and before the TsFile is sealed: until the
    // latch opens, the processor stays in the closing set and its resource stays unclosed.
    CountDownLatch flushHeld = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    resource
        .getProcessor()
        .addFlushListeners(
            Collections.singletonList(new HoldingFlushListener(flushHeld, releaseFlush)));

    // The concurrent close: StorageEngine.stop() moves the working processor into the closing set.
    List<Future<?>> concurrentClose = dataRegion.asyncCloseAllWorkingTsFileProcessors();
    assertTrue(flushHeld.await(HANG_GUARD_SECONDS, TimeUnit.SECONDS));
    assertFalse(resource.isClosed());

    File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot-concurrent-close");
    FutureTask<Boolean> snapshot =
        new FutureTask<>(
            () ->
                new SnapshotTaker(dataRegion)
                    .takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
    Thread taker = new Thread(snapshot, "snapshot-taker");
    taker.start();
    try {
      // The snapshot has listed its files (the unclosed resource among them) before it can do
      // either: finish without the TsFile, or park until the closing processor is done.
      awaitFinishedOrWaitingForClosingProcessors(taker);
    } finally {
      releaseFlush.countDown();
    }
    assertTrue(snapshot.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS));
    for (Future<?> close : concurrentClose) {
      close.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
    }
    assertTrue(resource.isClosed());
    assertSnapshotHolds(snapshotDir, resource);

    // Loading a snapshot replaces the region's files, as a restart of a research backend does.
    DataRegion restored =
        new SnapshotLoader(snapshotDir.getAbsolutePath(), DATABASE, "0")
            .loadSnapshotForStateMachine();
    assertNotNull(restored);
    List<TsFileResource> restoredFiles = restored.getTsFileManager().getTsFileList(true);
    assertEquals(1, restoredFiles.size());
    assertEquals(resource.getTsFile().getName(), restoredFiles.get(0).getTsFile().getName());
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE);
    assertEquals(Optional.of(1L), restoredFiles.get(0).getStartTime(device));
    assertEquals(Optional.of(ROWS), restoredFiles.get(0).getEndTime(device));
  }

  @Test
  public void snapshotFailsWhenAnotherThreadsCloseCannotSealTheTsFile() throws Exception {
    insertRows();
    TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    TsFileProcessor processor = resource.getProcessor();
    failSealOf(processor);
    for (Future<?> close : dataRegion.asyncCloseAllWorkingTsFileProcessors()) {
      close.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
    }
    // The close is over, but its TsFile stays unsealed and its processor stays in the closing set.
    assertFalse(resource.isClosed());
    assertTrue(closingProcessors(dataRegion, true).contains(processor));

    assertFalse(takeSnapshotPromptly("snapshot-other-close-fails"));
    assertFalse(resource.isClosed());
    assertRegionUnlocked();
  }

  @Test
  public void snapshotFailsWhenItsOwnCloseCannotSealTheTsFile() throws Exception {
    insertRows();
    TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    failSealOf(resource.getProcessor());

    assertFalse(takeSnapshotPromptly("snapshot-own-close-fails"));
    assertFalse(resource.isClosed());
    assertRegionUnlocked();
  }

  @Test
  public void snapshotFailsWhenACloseOutlastsTheWaitAndSucceedsOnceItIsOver() throws Exception {
    insertRows();
    TsFileResource resource = dataRegion.getTsFileManager().getTsFileList(true).get(0);
    CountDownLatch flushHeld = new CountDownLatch(1);
    CountDownLatch releaseFlush = new CountDownLatch(1);
    resource
        .getProcessor()
        .addFlushListeners(
            Collections.singletonList(new HoldingFlushListener(flushHeld, releaseFlush)));
    List<Future<?>> concurrentClose = dataRegion.asyncCloseAllWorkingTsFileProcessors();
    long sealWaitMillis = 300;
    try {
      assertTrue(flushHeld.await(HANG_GUARD_SECONDS, TimeUnit.SECONDS));
      long start = System.nanoTime();
      assertFalse(
          new SnapshotTaker(dataRegion, sealWaitMillis)
              .takeFullSnapshot(
                  new File(TestConstant.OUTPUT_DATA_DIR, "snapshot-close-outlasts")
                      .getAbsolutePath(),
                  true));
      assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(sealWaitMillis));
      // The failed snapshot let go of the region although the close is still running.
      assertFalse(resource.isClosed());
      assertRegionUnlocked();
    } finally {
      releaseFlush.countDown();
    }
    for (Future<?> close : concurrentClose) {
      close.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
    }
    assertTrue(resource.isClosed());

    File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, "snapshot-after-close");
    assertTrue(new SnapshotTaker(dataRegion).takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
    assertSnapshotHolds(snapshotDir, resource);
  }

  @Test
  public void snapshotFailsAfterAFailedFlushDroppedRowsOfTheRegion() throws Exception {
    insertRows();
    try (AutoCloseable ignored = failFlushes()) {
      for (Future<?> close : dataRegion.asyncCloseAllWorkingTsFileProcessors()) {
        close.get(HANG_GUARD_SECONDS, TimeUnit.SECONDS);
      }
    }
    // The failed flush removed the rows and their TsFile, so the snapshot cannot even list them.
    assertTrue(dataRegion.getTsFileManager().getTsFileList(true).isEmpty());

    assertFalse(takeSnapshotPromptly("snapshot-after-dropped-rows"));
  }

  @Test
  public void snapshotFailsWhenItsOwnFlushDropsRows() throws Exception {
    insertRows();
    try (AutoCloseable ignored = failFlushes()) {
      assertFalse(takeSnapshotPromptly("snapshot-own-flush-drops-rows"));
    }
    assertTrue(dataRegion.getTsFileManager().getTsFileList(true).isEmpty());
  }

  private void insertRows() throws Exception {
    for (long time = 1; time <= ROWS; time++) {
      TSRecord record = new TSRecord(DEVICE, time);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT64, MEASUREMENT, String.valueOf(time)));
      dataRegion.insert(DataRegionTest.buildInsertRowNodeByTSRecord(record));
    }
  }

  /**
   * Takes a flush-first snapshot on another thread and returns its result, failing the test if it
   * neither succeeds nor fails before the default seal wait could have run out.
   */
  private boolean takeSnapshotPromptly(String name) throws Exception {
    File snapshotDir = new File(TestConstant.OUTPUT_DATA_DIR, name);
    FutureTask<Boolean> snapshot =
        new FutureTask<>(
            () ->
                new SnapshotTaker(dataRegion)
                    .takeFullSnapshot(snapshotDir.getAbsolutePath(), true));
    Thread taker = new Thread(snapshot, "snapshot-taker");
    taker.start();
    try {
      return snapshot.get(PROMPT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      String stack = Arrays.toString(taker.getStackTrace());
      taker.interrupt();
      taker.join(TimeUnit.SECONDS.toMillis(HANG_GUARD_SECONDS));
      fail("The snapshot still waits for a close that is over: " + stack);
      return false;
    }
  }

  private void assertRegionUnlocked() {
    assertTrue(dataRegion.tryReadLock(TimeUnit.SECONDS.toMillis(1)));
    dataRegion.readUnlock();
  }

  private static void assertSnapshotHolds(File snapshotDir, TsFileResource resource)
      throws Exception {
    String tsFileName = resource.getTsFile().getName();
    List<String> snapshotFiles = new ArrayList<>();
    for (File file :
        new SnapshotLoader(snapshotDir.getAbsolutePath(), DATABASE, "0").getSnapshotFileInfo()) {
      snapshotFiles.add(file.getName());
    }
    assertTrue(
        "The snapshot left out " + tsFileName + ", which was closing: " + snapshotFiles,
        snapshotFiles.contains(tsFileName));
    assertTrue(snapshotFiles.contains(tsFileName + TsFileResource.RESOURCE_SUFFIX));
  }

  /**
   * An I/O failure on the TsFile after the memtable reached it and before the file is sealed: the
   * close ends, but the TsFile stays unsealed and the processor stays in its closing set.
   */
  private static void failSealOf(TsFileProcessor processor) throws Exception {
    Field writerField = TsFileProcessor.class.getDeclaredField("writer");
    writerField.setAccessible(true);
    RestorableTsFileIOWriter writer = (RestorableTsFileIOWriter) writerField.get(processor);
    processor.addFlushListeners(
        Collections.singletonList(
            new FlushListener() {
              @Override
              public void onMemTableFlushStarted(IMemTable memTable) {
                // the failure comes after the memtable reached the file
              }

              @Override
              public void onMemTableFlushed(IMemTable memTable) {
                try {
                  writer.getTsFileOutput().close();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              }
            }));
  }

  /**
   * Until closed, a flush fails before it writes its memtable. TsFileProcessor#flushOneMemTable
   * then truncates the TsFile and releases the memtable, so the region loses these rows; a flush
   * that FlushManager.stop() interrupts at shutdown ends in the same error path.
   */
  private static AutoCloseable failFlushes() throws Exception {
    Field poolField = AbstractPoolManager.class.getDeclaredField("pool");
    poolField.setAccessible(true);
    FlushSubTaskPoolManager manager = FlushSubTaskPoolManager.getInstance();
    ExecutorService rejecting = Executors.newSingleThreadExecutor();
    rejecting.shutdown();
    Object original;
    // AbstractPoolManager#submit and #stop synchronize on the manager.
    synchronized (manager) {
      original = poolField.get(manager);
      poolField.set(manager, rejecting);
    }
    return () -> {
      synchronized (manager) {
        poolField.set(manager, original);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static Set<TsFileProcessor> closingProcessors(DataRegion region, boolean sequence)
      throws Exception {
    Field field =
        DataRegion.class.getDeclaredField(
            sequence ? "closingSequenceTsFileProcessor" : "closingUnSequenceTsFileProcessor");
    field.setAccessible(true);
    return (Set<TsFileProcessor>) field.get(region);
  }

  /**
   * Returns once {@code taker} has finished or is parked on a close in DataRegion's wait for the
   * closes in flight. Both are final while the flush is held, so the outcome does not depend on how
   * long either thread runs.
   */
  private static void awaitFinishedOrWaitingForClosingProcessors(Thread taker) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANG_GUARD_SECONDS);
    while (taker.isAlive() && !waitsForClosingProcessors(taker)) {
      if (System.nanoTime() > deadline) {
        fail(
            "The snapshot neither finished nor waited for the closing TsFileProcessor: "
                + Arrays.toString(taker.getStackTrace()));
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  /** Parked in Future#get called by DataRegion#syncCloseAllTsFileProcessors (not in its lock). */
  private static boolean waitsForClosingProcessors(Thread thread) {
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
