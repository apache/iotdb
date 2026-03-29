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

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTsFileDeletionBarrierTest {

  private static final int TEST_REGION_ID = 1;
  private static final String TEST_TSFILE_PATH = "/tmp/test.tsfile";

  @After
  public void tearDown() {
    PipeTsFileDeletionBarrier.getInstance().clear();
  }

  @Test
  public void testSnapshotWaitsForEarlierDeletionResolution() throws Exception {
    final PipeTsFileDeletionBarrier barrier = PipeTsFileDeletionBarrier.getInstance();
    final long deleteSeq = barrier.beginDeletion(TEST_REGION_ID);

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    final Thread thread =
        new Thread(
            () -> {
              try {
                started.countDown();
                barrier.awaitDeletionResolutionUpTo(TEST_REGION_ID, deleteSeq);
              } catch (final Throwable throwable) {
                failure.set(throwable);
              } finally {
                finished.countDown();
              }
            },
            "test-wait-delete-resolution");
    thread.start();

    Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
    Assert.assertFalse(finished.await(200, TimeUnit.MILLISECONDS));

    barrier.resolveDeletionTargets(TEST_REGION_ID, deleteSeq, Collections.emptySet());

    Assert.assertTrue(finished.await(5, TimeUnit.SECONDS));
    if (failure.get() != null) {
      throw new AssertionError("Snapshot wait should finish successfully", failure.get());
    }
    thread.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  public void testSnapshotWaitsForPendingDeletionUpToUpperBound() throws Exception {
    final PipeTsFileDeletionBarrier barrier = PipeTsFileDeletionBarrier.getInstance();
    final long deleteSeq = barrier.beginDeletion(TEST_REGION_ID);
    barrier.resolveDeletionTargets(
        TEST_REGION_ID, deleteSeq, Collections.singleton(TEST_TSFILE_PATH));

    final long snapshotUpperBound = barrier.beginSnapshot(TEST_REGION_ID, TEST_TSFILE_PATH);
    Assert.assertEquals(deleteSeq, snapshotUpperBound);

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    final Thread thread =
        new Thread(
            () -> {
              try {
                started.countDown();
                barrier.awaitPendingDeletionsUpTo(TEST_TSFILE_PATH, snapshotUpperBound);
              } catch (final Throwable throwable) {
                failure.set(throwable);
              } finally {
                finished.countDown();
              }
            },
            "test-wait-pending-delete");
    thread.start();

    Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
    Assert.assertFalse(finished.await(200, TimeUnit.MILLISECONDS));

    barrier.finishDeletion(deleteSeq, Collections.singleton(TEST_TSFILE_PATH));

    Assert.assertTrue(finished.await(5, TimeUnit.SECONDS));
    if (failure.get() != null) {
      throw new AssertionError("Pending delete wait should finish successfully", failure.get());
    }

    barrier.finishSnapshot(TEST_TSFILE_PATH, snapshotUpperBound);
    thread.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  public void testLaterDeletionWaitsForEarlierSnapshot() throws Exception {
    final PipeTsFileDeletionBarrier barrier = PipeTsFileDeletionBarrier.getInstance();
    final long snapshotUpperBound = barrier.beginSnapshot(TEST_REGION_ID, TEST_TSFILE_PATH);
    Assert.assertEquals(0L, snapshotUpperBound);

    final long deleteSeq = barrier.beginDeletion(TEST_REGION_ID);
    barrier.resolveDeletionTargets(
        TEST_REGION_ID, deleteSeq, Collections.singleton(TEST_TSFILE_PATH));

    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    final Thread thread =
        new Thread(
            () -> {
              try {
                started.countDown();
                barrier.awaitSnapshotsBeforeMaterialization(TEST_TSFILE_PATH, deleteSeq);
              } catch (final Throwable throwable) {
                failure.set(throwable);
              } finally {
                finished.countDown();
              }
            },
            "test-wait-earlier-snapshot");
    thread.start();

    Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
    Assert.assertFalse(finished.await(200, TimeUnit.MILLISECONDS));

    barrier.finishSnapshot(TEST_TSFILE_PATH, snapshotUpperBound);

    Assert.assertTrue(finished.await(5, TimeUnit.SECONDS));
    if (failure.get() != null) {
      throw new AssertionError("Later deletion wait should finish successfully", failure.get());
    }

    barrier.finishDeletion(deleteSeq, Collections.singleton(TEST_TSFILE_PATH));
    thread.join(TimeUnit.SECONDS.toMillis(5));
  }
}
