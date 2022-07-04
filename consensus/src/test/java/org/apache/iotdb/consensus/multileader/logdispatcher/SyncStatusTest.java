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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.consensus.config.MultiLeaderConfig;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SyncStatusTest {

  private static final File storageDir = new File("target" + java.io.File.separator + "test");
  private static final String prefix = "version";
  private static final MultiLeaderConfig config = new MultiLeaderConfig.Builder().build();

  @Before
  public void setUp() throws IOException {
    FileUtils.createDirectories(storageDir);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteFully(storageDir);
  }

  /** Confirm success from front to back */
  @Test
  public void sequenceTest() throws InterruptedException {
    IndexController controller = new IndexController(storageDir.getAbsolutePath(), prefix);
    Assert.assertEquals(0, controller.getCurrentIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<PendingBatch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatch(); i++) {
      PendingBatch batch = new PendingBatch(i, i, Collections.emptyList());
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatch(); i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch() - 1 - i, status.getPendingBatches().size());
      Assert.assertEquals(i, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
    }
  }

  /** Confirm success from back to front */
  @Test
  public void reverseTest() throws InterruptedException {
    IndexController controller = new IndexController(storageDir.getAbsolutePath(), prefix);
    Assert.assertEquals(0, controller.getCurrentIndex());
    Assert.assertEquals(0, controller.getLastFlushedIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<PendingBatch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatch(); i++) {
      PendingBatch batch = new PendingBatch(i, i, Collections.emptyList());
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatch() - 1; i++) {
      status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatch() - 1 - i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getPendingBatches().size());
      Assert.assertEquals(0, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
    }

    status.removeBatch(batchList.get(0));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatch() - 1, controller.getCurrentIndex());
    Assert.assertEquals(config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
  }

  /** Confirm success first from front to back, then back to front */
  @Test
  public void mixedTest() throws InterruptedException {
    IndexController controller = new IndexController(storageDir.getAbsolutePath(), prefix);
    Assert.assertEquals(0, controller.getCurrentIndex());
    Assert.assertEquals(0, controller.getLastFlushedIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<PendingBatch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatch(); i++) {
      PendingBatch batch = new PendingBatch(i, i, Collections.emptyList());
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatch() / 2; i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch() - 1 - i, status.getPendingBatches().size());
      Assert.assertEquals(i, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
    }

    for (int i = config.getReplication().getMaxPendingBatch() / 2 + 1;
        i < config.getReplication().getMaxPendingBatch();
        i++) {
      status.removeBatch(batchList.get(i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch() / 2, status.getPendingBatches().size());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
    }

    status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatch() / 2));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatch() - 1, controller.getCurrentIndex());
    Assert.assertEquals(config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
  }

  /** Test Blocking while addNextBatch */
  @Test
  public void waitTest() throws InterruptedException, ExecutionException {
    IndexController controller = new IndexController(storageDir.getAbsolutePath(), prefix);
    Assert.assertEquals(0, controller.getCurrentIndex());

    SyncStatus status = new SyncStatus(controller, config);
    List<PendingBatch> batchList = new ArrayList<>();

    for (long i = 0; i < config.getReplication().getMaxPendingBatch(); i++) {
      PendingBatch batch = new PendingBatch(i, i, Collections.emptyList());
      batchList.add(batch);
      status.addNextBatch(batch);
    }

    for (int i = 0; i < config.getReplication().getMaxPendingBatch() - 1; i++) {
      status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatch() - 1 - i));
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getPendingBatches().size());
      Assert.assertEquals(0, controller.getCurrentIndex());
      Assert.assertEquals(
          config.getReplication().getMaxPendingBatch(), status.getNextSendingIndex());
    }

    CompletableFuture<Boolean> future =
        CompletableFuture.supplyAsync(
            () -> {
              PendingBatch batch =
                  new PendingBatch(
                      config.getReplication().getMaxPendingBatch(),
                      config.getReplication().getMaxPendingBatch(),
                      Collections.emptyList());
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
        config.getReplication().getMaxPendingBatch() - 1, controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatch() + 1, status.getNextSendingIndex());

    status.removeBatch(batchList.get(config.getReplication().getMaxPendingBatch()));
    Assert.assertEquals(0, status.getPendingBatches().size());
    Assert.assertEquals(config.getReplication().getMaxPendingBatch(), controller.getCurrentIndex());
    Assert.assertEquals(
        config.getReplication().getMaxPendingBatch() + 1, status.getNextSendingIndex());
  }
}
