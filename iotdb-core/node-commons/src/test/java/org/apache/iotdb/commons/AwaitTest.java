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

package org.apache.iotdb.commons;

import org.apache.iotdb.commons.concurrent.Await;
import org.apache.iotdb.commons.concurrent.AwaitTimeoutException;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AwaitTest {

  @Test
  public void testConditionAlreadyTrue() {
    Await.await().atMost(1, TimeUnit.SECONDS).until(() -> true);
  }

  @Test
  public void testConditionBecomesTrue() {
    AtomicBoolean flag = new AtomicBoolean(false);
    new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              flag.set(true);
            })
        .start();

    Await.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(flag::get);

    assertTrue(flag.get());
  }

  @Test(expected = AwaitTimeoutException.class)
  public void testTimeout() {
    Await.await()
        .atMost(300, TimeUnit.MILLISECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> false);
  }

  @Test
  public void testPollDelay() {
    long start = System.currentTimeMillis();

    Await.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollDelay(200, TimeUnit.MILLISECONDS)
        .until(() -> true);

    long elapsed = System.currentTimeMillis() - start;
    assertTrue("Expected at least 200ms delay, got " + elapsed, elapsed >= 180);
  }

  @Test
  public void testIgnoreAllExceptions() {
    AtomicInteger counter = new AtomicInteger(0);

    Await.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              int val = counter.incrementAndGet();
              if (val < 3) {
                throw new RuntimeException("not ready yet");
              }
              return true;
            });

    assertTrue(counter.get() >= 3);
  }

  @Test
  public void testIgnoreSpecificException() {
    AtomicInteger counter = new AtomicInteger(0);

    Await.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .ignoreException(IllegalStateException.class)
        .until(
            () -> {
              int val = counter.incrementAndGet();
              if (val < 3) {
                throw new IllegalStateException("not ready");
              }
              return true;
            });

    assertTrue(counter.get() >= 3);
  }

  @Test
  public void testNonIgnoredExceptionPropagates() {
    try {
      Await.await()
          .atMost(5, TimeUnit.SECONDS)
          .ignoreException(IllegalStateException.class)
          .until(
              () -> {
                throw new IllegalArgumentException("unexpected");
              });
      fail("Should have thrown");
    } catch (AwaitTimeoutException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testUntilAsserted() {
    AtomicInteger value = new AtomicInteger(0);
    new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              value.set(42);
            })
        .start();

    Await.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertEquals(42, value.get()));
  }

  @Test
  public void testForever() {
    AtomicInteger counter = new AtomicInteger(0);

    Await.await()
        .forever()
        .pollInterval(10, TimeUnit.MILLISECONDS)
        .until(() -> counter.incrementAndGet() >= 5);

    assertTrue(counter.get() >= 5);
  }

  @Test
  public void testTimeoutMessageIncludesLastException() {
    try {
      Await.await()
          .atMost(200, TimeUnit.MILLISECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .ignoreExceptions()
          .until(
              () -> {
                throw new RuntimeException("still failing");
              });
      fail("Should have thrown");
    } catch (AwaitTimeoutException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals("still failing", e.getCause().getMessage());
    }
  }
}
