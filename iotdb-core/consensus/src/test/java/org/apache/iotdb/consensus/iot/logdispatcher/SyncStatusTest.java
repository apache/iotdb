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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SyncStatusTest {

  private static final File storageDir = new File("target" + java.io.File.separator + "test");

  private static final Peer peer =
      new Peer(new DataRegionId(1), 2, new TEndPoint("127.0.0.1", 6667));
  private static final IoTConsensusConfig config = new IoTConsensusConfig.Builder().build();
  private static final long CHECK_POINT_GAP = 500;

  @Before
  public void setUp() throws IOException {
    FileUtils.createDirectories(storageDir);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteFully(storageDir);
  }

  /** Confirm success from front to back. */
  @Test
  public void sequenceTest() throws InterruptedException {
    IndexController controller =
        new IndexController(storageDir.getAbsolutePath(), peer, 0, CHECK_POINT_GAP);
    Assert.assertEquals(0, controller.getCurrentIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<Batch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatchesNum(); i++) {
      TLogEntry logEntry = new TLogEntry();
      logEntry.setSearchIndex(i);
      Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
      batch.addTLogEntry(logEntry);
      batch.buildIndex();
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatchesNum(); i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum() - 1 - i,
          status.getPendingBatches().size());
      Assert.assertEquals(i, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
    }
  }

  /** Confirm success from back to front. */
  @Test
  public void reverseTest() throws InterruptedException {
    IndexController controller =
        new IndexController(storageDir.getAbsolutePath(), peer, 0, CHECK_POINT_GAP);
    Assert.assertEquals(0, controller.getCurrentIndex());
    Assert.assertEquals(0, controller.getLastFlushedIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<Batch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatchesNum(); i++) {
      TLogEntry logEntry = new TLogEntry();
      logEntry.setSearchIndex(i);
      Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
      batch.addTLogEntry(logEntry);
      batch.buildIndex();
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatchesNum() - 1; i++) {
      status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatchesNum() - 1 - i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getPendingBatches().size());
      Assert.assertEquals(0, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
    }

    status.removeBatch(batchList.get(0));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum() - 1, controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
  }

  /** Confirm success first from front to back, then back to front. */
  @Test
  public void mixedTest() throws InterruptedException {
    IndexController controller =
        new IndexController(storageDir.getAbsolutePath(), peer, 0, CHECK_POINT_GAP);
    Assert.assertEquals(0, controller.getCurrentIndex());
    Assert.assertEquals(0, controller.getLastFlushedIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<Batch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatchesNum(); i++) {
      TLogEntry logEntry = new TLogEntry();
      logEntry.setSearchIndex(i);
      Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
      batch.addTLogEntry(logEntry);
      batch.buildIndex();
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatchesNum() / 2; i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum() - 1 - i,
          status.getPendingBatches().size());
      Assert.assertEquals(i, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
    }

    for (int i = config.getReplication().getMaxPendingBatchesNum() / 2 + 1;
        i < config.getReplication().getMaxPendingBatchesNum();
        i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum()
              - config.getReplication().getMaxPendingBatchesNum() / 2,
          status.getPendingBatches().size());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
    }

    status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatchesNum() / 2));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum() - 1, controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
  }

  /** Test Blocking while addNextBatch. */
  @Test
  public void waitTest() throws InterruptedException, ExecutionException {
    IndexController controller =
        new IndexController(storageDir.getAbsolutePath(), peer, 0, CHECK_POINT_GAP);
    Assert.assertEquals(0, controller.getCurrentIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<Batch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatchesNum(); i++) {
      TLogEntry logEntry = new TLogEntry();
      logEntry.setSearchIndex(i);
      Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
      batch.addTLogEntry(logEntry);
      batch.buildIndex();
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatchesNum() - 1; i++) {
      status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatchesNum() - 1 - i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getPendingBatches().size());
      Assert.assertEquals(0, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatchesNum(), status.getNextSendingIndex());
    }

    CompletableFuture<Boolean> future =
        CompletableFuture.supplyAsync(
            () -> {
              TLogEntry logEntry = new TLogEntry();
              logEntry.setSearchIndex(config.getReplication().getMaxPendingBatchesNum());
              Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
              batch.addTLogEntry(logEntry);
              batch.buildIndex();
              batchList.add(batch);
              try {
                status.addNextBatch(batch);
              } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
              }
              return true;
            });

    Thread.sleep(1000);
    Assert.assertFalse(future.isDone());

    status.removeBatch(batchList.get(0));
    Assert.assertTrue(future.get());
    Assert.assertEquals(1, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum() - 1, controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum() + 1, status.getNextSendingIndex());

    status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatchesNum()));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum(), controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatchesNum() + 1, status.getNextSendingIndex());
  }

  @Test
  public void testFirstBatchRetriesMemoryReservation()
      throws InterruptedException, ExecutionException, TimeoutException {
    IndexController controller =
        new IndexController(storageDir.getAbsolutePath(), peer, 0, CHECK_POINT_GAP);
    IoTConsensusConfig retryConfig =
        IoTConsensusConfig.newBuilder()
            .setReplication(
                IoTConsensusConfig.Replication.newBuilder().setBasicRetryWaitTimeMs(10).build())
            .build();
    SyncStatus status = new SyncStatus(controller, retryConfig);
    TLogEntry logEntry = new TLogEntry().setSearchIndex(1).setMemorySize(1);
    Batch batch = new Batch(retryConfig);
    batch.addTLogEntry(logEntry);
    batch.buildIndex();

    IoTConsensusMemoryManager memoryManager = IoTConsensusMemoryManager.getInstance();
    long previousMaxMemory = memoryManager.getMaxMemorySizeInByte();
    long previousMaxQueueMemory = memoryManager.getMaxMemorySizeForQueueInByte();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch taskStarted = new CountDownLatch(1);
    memoryManager.init(0, 0);
    try {
      Future<?> future =
          executor.submit(
              () -> {
                taskStarted.countDown();
                status.addNextBatch(batch);
                return null;
              });

      Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
      // The zero limit makes the first reservation fail before the retry limit is raised.
      Thread.sleep(100);
      Assert.assertFalse(future.isDone());
      memoryManager.init(batch.getMemorySize() + 1, batch.getMemorySize() + 1);
      future.get(5, TimeUnit.SECONDS);

      Assert.assertEquals(1, status.getPendingBatches().size());
      status.removeBatch(batch);
      Assert.assertEquals(0, status.getPendingBatches().size());
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      status.free();
      memoryManager.init(previousMaxMemory, previousMaxQueueMemory);
    }
  }
}
