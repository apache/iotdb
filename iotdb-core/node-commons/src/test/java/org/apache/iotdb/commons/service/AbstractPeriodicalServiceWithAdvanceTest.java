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

package org.apache.iotdb.commons.service;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AbstractPeriodicalServiceWithAdvanceTest {

  // far longer than the test may take, so only a wake-up can end the interval wait
  private static final long INTERVAL_MS = TimeUnit.MINUTES.toMillis(10);
  private static final long STOP_TIMEOUT_MS = 5_000;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void stopWakesARunningIteration() throws Exception {
    final CountDownLatch firstRun = new CountDownLatch(1);
    final TestService service = new TestService(executor, firstRun);

    service.startService();
    Assert.assertTrue(firstRun.await(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    service.stopService();
    executor.shutdown();

    Assert.assertTrue(executor.awaitTermination(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, service.runs.get());
  }

  @Test
  public void stopBeforeTheQueuedIterationBegins() throws Exception {
    // hold the executor's only thread, so that the iteration submitted by startService() stays
    // queued until after stopService()
    final CountDownLatch release = new CountDownLatch(1);
    executor.submit(
        () -> {
          release.await();
          return null;
        });
    final TestService service = new TestService(executor, new CountDownLatch(1));

    service.startService();
    service.stopService();
    executor.shutdown();
    release.countDown();

    Assert.assertTrue(executor.awaitTermination(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Assert.assertEquals(1, service.runs.get());
  }

  private static class TestService extends AbstractPeriodicalServiceWithAdvance {
    private final AtomicInteger runs = new AtomicInteger();
    private final CountDownLatch firstRun;

    TestService(final ExecutorService executor, final CountDownLatch firstRun) {
      super(executor, INTERVAL_MS);
      this.firstRun = firstRun;
    }

    @Override
    protected void executeTask() {
      runs.incrementAndGet();
      firstRun.countDown();
    }
  }
}
