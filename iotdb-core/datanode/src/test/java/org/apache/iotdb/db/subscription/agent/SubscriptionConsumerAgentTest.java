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

package org.apache.iotdb.db.subscription.agent;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SubscriptionConsumerAgentTest {

  private static final long TEST_TIMEOUT_SECONDS = 5L;

  @Test
  public void testTopicDiffSetsUpBeforeTeardownAndMetaPublication() {
    final List<String> operations = new ArrayList<>();

    SubscriptionConsumerAgent.applyTopicDiff(
        () -> operations.add("setup"),
        () -> operations.add("teardown"),
        () -> operations.add("publish"));

    assertEquals(Arrays.asList("setup", "teardown", "publish"), operations);
  }

  @Test
  public void testTopicDiffSetupFailurePreservesOldQueuesAndMeta() {
    final List<String> operations = new ArrayList<>();

    try {
      SubscriptionConsumerAgent.applyTopicDiff(
          () -> {
            operations.add("setup");
            throw new IllegalStateException("setup failed");
          },
          () -> operations.add("teardown"),
          () -> operations.add("publish"));
      fail();
    } catch (final IllegalStateException ignored) {
      // expected
    }

    assertEquals(Collections.singletonList("setup"), operations);
  }

  @Test
  public void testSameConsumerGroupMetaChangesAreSerialized() throws Exception {
    final SubscriptionConsumerAgent agent = new SubscriptionConsumerAgent();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CountDownLatch firstEntered = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CountDownLatch secondEntered = new CountDownLatch(1);
    try {
      final Future<?> first =
          executor.submit(
              () ->
                  agent.executeWithConsumerGroupLock(
                      "group",
                      () -> {
                        firstEntered.countDown();
                        await(releaseFirst);
                      }));
      assertTrue(firstEntered.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

      final Future<?> second =
          executor.submit(
              () -> agent.executeWithConsumerGroupLock("group", secondEntered::countDown));
      assertFalse(secondEntered.await(200L, TimeUnit.MILLISECONDS));

      releaseFirst.countDown();
      first.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      second.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertEquals(0L, secondEntered.getCount());
    } finally {
      releaseFirst.countDown();
      executor.shutdownNow();
      executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testDifferentConsumerGroupMetaChangesRunIndependently() throws Exception {
    final SubscriptionConsumerAgent agent = new SubscriptionConsumerAgent();
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CountDownLatch firstEntered = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CountDownLatch secondEntered = new CountDownLatch(1);
    try {
      final Future<?> first =
          executor.submit(
              () ->
                  agent.executeWithConsumerGroupLock(
                      "group-1",
                      () -> {
                        firstEntered.countDown();
                        await(releaseFirst);
                      }));
      assertTrue(firstEntered.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

      final Future<?> second =
          executor.submit(
              () -> agent.executeWithConsumerGroupLock("group-2", secondEntered::countDown));
      assertTrue(secondEntered.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

      releaseFirst.countDown();
      first.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      second.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } finally {
      releaseFirst.countDown();
      executor.shutdownNow();
      executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testConsumerMetaReadsRemainAvailableDuringGroupOperation() throws Exception {
    final SubscriptionConsumerAgent agent = new SubscriptionConsumerAgent();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch operationEntered = new CountDownLatch(1);
    final CountDownLatch releaseOperation = new CountDownLatch(1);
    try {
      final Future<?> operation =
          executor.submit(
              () ->
                  agent.executeWithConsumerGroupLock(
                      "group",
                      () -> {
                        operationEntered.countDown();
                        await(releaseOperation);
                      }));
      assertTrue(operationEntered.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertFalse(agent.isConsumerExisted("group", "consumer"));

      releaseOperation.countDown();
      operation.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } finally {
      releaseOperation.countDown();
      executor.shutdownNow();
      executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
