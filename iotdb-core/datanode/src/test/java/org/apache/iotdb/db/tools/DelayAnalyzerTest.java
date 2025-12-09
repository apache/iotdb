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
package org.apache.iotdb.db.tools;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Unit tests for DelayAnalyzer */
public class DelayAnalyzerTest {

  /**
   * Test 1: Basic functionality verification. Verifies simple data ingestion and P99 calculation to
   * ensure CDF logic is correct.
   */
  @Test
  public void testBasicQuantileCalculation() {
    // Window size of 100
    DelayAnalyzer analyzer = new DelayAnalyzer(100, 1);

    long now = System.currentTimeMillis();
    // Ingest delay data ranging from 0ms to 100ms
    for (int i = 0; i <= 100; i++) {
      // arrival = now, generation = now - delay
      analyzer.update(now - i, now);
    }

    // Verification: Since data is uniformly distributed from 0-99
    // P50 should be around 49 or 50
    // P99 should be 99
    long p50 = analyzer.getDelayQuantile(0.50);
    long p99 = analyzer.getDelayQuantile(0.99);
    long max = analyzer.getDelayQuantile(1.00);

    System.out.println("Basic Test -> P50: " + p50 + ", P99: " + p99);

    // Allow 1ms calculation error (depending on rounding logic)
    Assert.assertTrue("P50 should be around 49", Math.abs(p50 - 49) <= 1);
    Assert.assertEquals("P99 should be 99", 99, p99);
    Assert.assertEquals("Max should be 100", 100, max);
  }

  /**
   * Test 2: Circular buffer eviction mechanism (sliding window). Corresponds to "dynamic delay
   * distribution" mentioned in the paper. Scenario: Ingest low-latency data first, then
   * high-latency data, verifying that old data is correctly evicted.
   */
  @Test
  public void testCircularBufferEviction() {
    // Extremely small window size: 5
    DelayAnalyzer analyzer = new DelayAnalyzer(5, 1);
    long now = System.currentTimeMillis();

    // Phase 1: Fill the window with small delays (10ms)
    for (int i = 0; i < 5; i++) {
      analyzer.update(now - 10, now);
    }
    Assert.assertEquals("Initial max delay should be 10", 10, analyzer.getDelayQuantile(1.0));

    // Phase 2: Ingest new high-latency data (100ms)
    // Writing 5 new points should evict all previous 10ms points
    for (int i = 0; i < 5; i++) {
      analyzer.update(now - 100, now);
    }

    // Verification: The minimum delay in the window should now be 100, old 10ms data should be
    // evicted
    // If the circular logic is wrong, old data might still be read
    long minInWindow = analyzer.getDelayQuantile(0.01); // Approximate P0
    Assert.assertEquals("Old data (10ms) should be evicted, min should be 100", 100, minInWindow);
  }

  /**
   * Test 3: Negative delay handling (clock skew). The paper mentions clock skew can cause
   * anomalies; the code should clamp negative values to 0.
   */
  @Test
  public void testNegativeDelayHandling() {
    DelayAnalyzer analyzer = new DelayAnalyzer(1000);
    long now = 10000;

    // Generation time is later than arrival time (future time), simulating clock rollback or
    // desynchronization
    analyzer.update(now + 5000, now);

    long maxDelay = analyzer.getDelayQuantile(1.0);
    Assert.assertEquals("Negative delay should be clamped to 0", 0, maxDelay);
  }

  /**
   * Test 4: Watermark calculation logic. Verifies if getSafeWatermark correctly uses the quantile.
   */
  @Test
  public void testSafeWatermarkLogic() {
    DelayAnalyzer analyzer = new DelayAnalyzer(100, 1);

    // Inject a fixed delay of 500ms
    analyzer.update(1000, 1500);

    long currentSystemTime = 2000;
    // P99 should be 500ms
    // Watermark = Current(2000) - P99(500) = 1500
    long watermark = analyzer.getSafeWatermark(currentSystemTime, 0.99);

    Assert.assertEquals(1500, watermark);
  }

  /** Test 5: Empty buffer and boundary handling */
  @Test
  public void testEmptyAndBoundaries() {
    DelayAnalyzer analyzer = new DelayAnalyzer(1000);

    // 1. When no data exists
    Assert.assertEquals("Empty buffer should return 0", 0, analyzer.getDelayQuantile(0.99));

    // 2. Only 1 data point
    analyzer.update(100, 150); // delay 50
    Assert.assertEquals(50, analyzer.getDelayQuantile(0.01));
    Assert.assertEquals(50, analyzer.getDelayQuantile(0.99));

    // 3. Illegal arguments
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          analyzer.getDelayQuantile(1.5);
        });

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          analyzer.getDelayQuantile(-0.1);
        });
  }

  /**
   * Test 6: Concurrency safety test. Simulates high-concurrency write scenarios to verify if
   * ReadWriteLock works effectively, ensuring no exceptions or deadlocks occur.
   */
  @Test
  public void testConcurrency() throws InterruptedException {
    int windowSize = 2000;
    final DelayAnalyzer analyzer = new DelayAnalyzer(windowSize);
    int threadCount = 10;
    final int writesPerThread = 5000;

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    final Random random = new Random();

    // Start 10 threads writing aggressively
    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              for (int j = 0; j < writesPerThread; j++) {
                long now = System.currentTimeMillis();
                // Random delay 0-100ms
                analyzer.update(now - random.nextInt(100), now);
              }
            } finally {
              latch.countDown();
            }
          });
    }

    // Main thread attempts to read simultaneously
    for (int i = 0; i < 10; i++) {
      analyzer.getDelayQuantile(0.99);
      Thread.sleep(10);
    }

    latch.await(5, TimeUnit.SECONDS); // Wait for all write threads to finish
    executor.shutdown();

    // Verification: Internal state should remain consistent after concurrent writes
    // Simple check: Should be able to read data normally without IndexOutOfBounds or
    // ConcurrentModificationException
    long p99 = analyzer.getDelayQuantile(0.99);

    Assert.assertTrue("P99 should be >= 0", p99 >= 0);
    Assert.assertTrue("P99 should be <= 100", p99 <= 100);

    System.out.println("Concurrency Test Passed. P99: " + p99);
  }

  /** Test 7: Default constructor and default confidence level */
  @Test
  public void testDefaultConstructor() {
    DelayAnalyzer analyzer = new DelayAnalyzer();
    Assert.assertEquals(
        "Default window size should be " + DelayAnalyzer.DEFAULT_WINDOW_SIZE,
        DelayAnalyzer.DEFAULT_WINDOW_SIZE,
        analyzer.getWindowSize());

    long now = System.currentTimeMillis();
    analyzer.update(now - 100, now);

    // Test default confidence level watermark calculation
    long watermark1 = analyzer.getSafeWatermark(now);
    long watermark2 = analyzer.getSafeWatermark(now, DelayAnalyzer.DEFAULT_CONFIDENCE_LEVEL);
    Assert.assertEquals("Default confidence level should be consistent", watermark1, watermark2);
  }

  /** Test 8: Window size validation */
  @Test
  public void testWindowSizeValidation() {
    // Test minimum window size
    DelayAnalyzer analyzer1 = new DelayAnalyzer(DelayAnalyzer.MIN_WINDOW_SIZE);
    Assert.assertEquals(DelayAnalyzer.MIN_WINDOW_SIZE, analyzer1.getWindowSize());

    // Test maximum window size
    DelayAnalyzer analyzer2 = new DelayAnalyzer(DelayAnalyzer.MAX_WINDOW_SIZE);
    Assert.assertEquals(DelayAnalyzer.MAX_WINDOW_SIZE, analyzer2.getWindowSize());

    // Test window size that is too small
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          new DelayAnalyzer(DelayAnalyzer.MIN_WINDOW_SIZE - 1);
        });

    // Test window size that is too large
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          new DelayAnalyzer(DelayAnalyzer.MAX_WINDOW_SIZE + 1);
        });
  }

  /** Test 9: Statistics retrieval */
  @Test
  public void testStatistics() {
    DelayAnalyzer analyzer = new DelayAnalyzer(100, 1);
    long now = System.currentTimeMillis();

    // Add some data
    for (int i = 0; i < 50; i++) {
      analyzer.update(now - i, now);
    }

    String stats = analyzer.getStatistics();
    Assert.assertNotNull("Statistics should not be null", stats);
    Assert.assertTrue("Statistics should contain P50", stats.contains("P50"));
    Assert.assertTrue("Statistics should contain P99", stats.contains("P99"));

    // Test empty statistics
    DelayAnalyzer emptyAnalyzer = new DelayAnalyzer(1000);
    String emptyStats = emptyAnalyzer.getStatistics();
    Assert.assertTrue("Empty statistics should indicate no data", emptyStats.contains("No data"));
  }

  /** Test 10: Reset functionality */
  @Test
  public void testReset() {
    DelayAnalyzer analyzer = new DelayAnalyzer(100, 1);
    long now = System.currentTimeMillis();

    // Add data
    for (int i = 0; i < 50; i++) {
      analyzer.update(now - i, now);
    }

    Assert.assertTrue("Should have data", analyzer.getCurrentSampleCount() > 0);
    Assert.assertTrue("Total samples should be > 0", analyzer.getTotalSamples() > 0);

    // Reset
    analyzer.reset();

    Assert.assertEquals(
        "After reset, current sample count should be 0", 0, analyzer.getCurrentSampleCount());
    Assert.assertEquals("After reset, total samples should be 0", 0, analyzer.getTotalSamples());
    Assert.assertEquals(
        "After reset, quantile should return 0", 0, analyzer.getDelayQuantile(0.99));
  }

  /** Test 11: Sample count functionality */
  @Test
  public void testSampleCount() {
    DelayAnalyzer analyzer = new DelayAnalyzer(100, 1);
    long now = System.currentTimeMillis();

    // Initial state
    Assert.assertEquals("Initial sample count should be 0", 0, analyzer.getCurrentSampleCount());
    Assert.assertEquals("Initial total samples should be 0", 0, analyzer.getTotalSamples());

    // Add 50 samples
    for (int i = 0; i < 50; i++) {
      analyzer.update(now - i, now);
    }

    Assert.assertEquals("Current sample count should be 50", 50, analyzer.getCurrentSampleCount());
    Assert.assertEquals("Total samples should be 50", 50, analyzer.getTotalSamples());

    // Fill the window
    for (int i = 50; i < 150; i++) {
      analyzer.update(now - i, now);
    }

    Assert.assertEquals(
        "After window is full, current sample count should be 100",
        100,
        analyzer.getCurrentSampleCount());
    Assert.assertEquals("Total samples should be 150", 150, analyzer.getTotalSamples());
  }

  /** Test 12: Accuracy of different quantiles */
  @Test
  public void testDifferentQuantiles() {
    DelayAnalyzer analyzer = new DelayAnalyzer(1000);
    long now = System.currentTimeMillis();

    // Create uniformly distributed delays: 0-999ms
    for (int i = 0; i < 1000; i++) {
      analyzer.update(now - i, now);
    }

    // Verify different quantiles
    long p10 = analyzer.getDelayQuantile(0.10);
    long p25 = analyzer.getDelayQuantile(0.25);
    long p50 = analyzer.getDelayQuantile(0.50);
    long p75 = analyzer.getDelayQuantile(0.75);
    long p90 = analyzer.getDelayQuantile(0.90);
    long p99 = analyzer.getDelayQuantile(0.99);

    // Allow 2ms error
    Assert.assertTrue("P10 should be around 100", Math.abs(p10 - 100) <= 2);
    Assert.assertTrue("P25 should be around 250", Math.abs(p25 - 250) <= 2);
    Assert.assertTrue("P50 should be around 500", Math.abs(p50 - 500) <= 2);
    Assert.assertTrue("P75 should be around 750", Math.abs(p75 - 750) <= 2);
    Assert.assertTrue("P90 should be around 900", Math.abs(p90 - 900) <= 2);
    Assert.assertTrue("P99 should be around 990", Math.abs(p99 - 990) <= 2);

    // Verify monotonicity: quantiles should be increasing
    Assert.assertTrue("P10 <= P25", p10 <= p25);
    Assert.assertTrue("P25 <= P50", p25 <= p50);
    Assert.assertTrue("P50 <= P75", p50 <= p75);
    Assert.assertTrue("P75 <= P90", p75 <= p90);
    Assert.assertTrue("P90 <= P99", p90 <= p99);
  }
}
