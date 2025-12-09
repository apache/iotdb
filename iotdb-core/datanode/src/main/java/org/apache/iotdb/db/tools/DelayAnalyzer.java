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

import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DelayAnalyzer: Calculate watermarks for in-order and out-of-order data based on statistics and
 * models
 *
 * <p>This implementation is based on the paper "Separation or Not: On Handling Out-of-Order
 * Time-Series Data" (ICDE 2022) proposed method, which dynamically calculates safe watermarks by
 * analyzing the Cumulative Distribution Function (CDF) of data arrival delays.
 *
 * <p>Core concepts:
 *
 * <ul>
 *   <li>Record the generation time (Event Time) and arrival time (System Time) of each data point
 *   <li>Calculate delay: delay = arrivalTime - generationTime
 *   <li>Maintain a sliding window to store recent delay samples
 *   <li>Use CDF to calculate delay quantiles (e.g., P99) at specific confidence levels
 *   <li>Safe watermark = Current system time - P99 delay
 * </ul>
 *
 * <p>Key formulas from the paper:
 *
 * <ul>
 *   <li>Delay calculation: p.t_d = p.t_a - p.t_g (Formula 160)
 *   <li>Use F(x) (CDF) to estimate the probability of out-of-order data (Reference 110)
 *   <li>Dynamic delay distribution: Use sliding window to maintain recent delay samples
 * </ul>
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Thread-safe: Uses ReadWriteLock to support high-concurrency read/write operations
 *   <li>Memory efficient: Uses circular buffer with fixed memory footprint
 *   <li>Clock skew handling: Automatically handles negative delays (clock rollback)
 *   <li>Dynamic adaptation: Sliding window automatically adapts to changes in delay distribution
 * </ul>
 *
 * @see <a href="https://ieeexplore.ieee.org/document/9835234">ICDE 2022 Paper</a>
 */
public class DelayAnalyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DelayAnalyzer.class);

  /** Default window size, empirical value: 10000 sample points */
  public static final int DEFAULT_WINDOW_SIZE = 10000;

  /** Recommended window size range */
  public static final int MIN_WINDOW_SIZE = 1000;

  public static final int MAX_WINDOW_SIZE = 100000;

  /** Default confidence level: 99% */
  public static final double DEFAULT_CONFIDENCE_LEVEL = 0.99;

  /**
   * Sliding window size for storing recent delay sample points. A larger window provides more
   * accurate P99 calculations but increases memory and sorting overhead. The paper mentions using
   * dynamic delay distribution to adapt to changes in delay patterns.
   */
  private final int windowSize;

  /** Circular buffer storing delay samples (unit: milliseconds) */
  private final long[] delaySamples;

  /** Current write position (cursor of the circular buffer) */
  private int cursor = 0;

  /** Flag indicating whether the buffer is full */
  private boolean isFull = false;

  /** Total number of samples (for statistics) */
  private long totalSamples = 0;

  /**
   * ReadWriteLock ensures thread safety in high-concurrency scenarios. Read operations (calculating
   * quantiles) use read lock, write operations (updating delays) use write lock.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /** Constructor using default window size */
  public DelayAnalyzer() {
    this(DEFAULT_WINDOW_SIZE);
  }

  /**
   * Constructor
   *
   * @param windowSize Sliding window size, recommended range: 5000 - 10000. Larger windows provide
   *     more accurate statistics but increase memory and computational overhead.
   * @throws IllegalArgumentException if window size is not within valid range
   */
  public DelayAnalyzer(int windowSize) {
    if (windowSize < MIN_WINDOW_SIZE || windowSize > MAX_WINDOW_SIZE) {
      throw new IllegalArgumentException(
          String.format(
              "Window size must be between %d and %d, got %d",
              MIN_WINDOW_SIZE, MAX_WINDOW_SIZE, windowSize));
    }
    this.windowSize = windowSize;
    this.delaySamples = new long[windowSize];
    LOGGER.debug("DelayAnalyzer initialized with window size: {}", windowSize);
  }

  @TestOnly
  public DelayAnalyzer(int windowSize, int placeHolder) {
    this.windowSize = windowSize;
    this.delaySamples = new long[windowSize];
    LOGGER.debug("DelayAnalyzer initialized with window size: {}", windowSize);
  }

  /**
   * Core method: Record a new data point
   *
   * <p>Corresponds to the formula in the paper: p.t_d = p.t_a - p.t_g (Formula 160)
   *
   * <p>Delay calculation logic:
   *
   * <ul>
   *   <li>delay = arrivalTime - generationTime
   *   <li>If delay < 0, it indicates clock skew or out-of-order data, set it to 0 (Paper Reference
   *       26: clock skew correction)
   * </ul>
   *
   * @param generationTime Data generation time (Event Time), unit: milliseconds
   * @param arrivalTime Data arrival time (System Time), unit: milliseconds
   */
  public void update(long generationTime, long arrivalTime) {
    // Calculate delay; delay cannot be negative (clock skew correction, Paper Reference 26)
    long delay = Math.max(0, arrivalTime - generationTime);

    lock.writeLock().lock();
    try {
      delaySamples[cursor] = delay;
      cursor++;
      totalSamples++;

      // Circular buffer: when cursor reaches window size, reset to 0 and mark as full
      if (cursor >= windowSize) {
        cursor = 0;
        isFull = true;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get the delay threshold for a specific quantile
   *
   * <p>Corresponds to the method in the paper using F(x) (CDF) to estimate the probability of
   * out-of-order data (Reference 110).
   *
   * <p>Calculation steps:
   *
   * <ol>
   *   <li>Get a snapshot of the current window (to avoid holding write lock for too long)
   *   <li>Sort the samples
   *   <li>Calculate index position based on quantile
   *   <li>Return the delay value at the corresponding position
   * </ol>
   *
   * @param percentile Quantile, range (0, 1], e.g., 0.99 represents 99% quantile (P99)
   * @return Delay threshold, unit: milliseconds
   * @throws IllegalArgumentException if percentile is not within valid range
   */
  public long getDelayQuantile(double percentile) {
    if (percentile <= 0 || percentile > 1) {
      throw new IllegalArgumentException(
          String.format("Percentile must be between 0 and 1, got %f", percentile));
    }

    long[] snapshot;
    int currentSize;

    // Get snapshot to avoid blocking write lock for too long
    lock.readLock().lock();
    try {
      if (!isFull && cursor == 0) {
        // No data yet
        return 0;
      }
      currentSize = isFull ? windowSize : cursor;
      snapshot = Arrays.copyOf(delaySamples, currentSize);
    } finally {
      lock.readLock().unlock();
    }

    // Sort to calculate quantile (Arrays.sort performance is sufficient for small datasets)
    Arrays.sort(snapshot);

    // Calculate index corresponding to quantile
    int index = (int) Math.ceil(currentSize * percentile) - 1;
    // Boundary correction to prevent index out of bounds
    index = Math.max(0, Math.min(index, currentSize - 1));

    return snapshot[index];
  }

  /**
   * Get the recommended safe watermark
   *
   * <p>Definition: At confidence level confidenceLevel, the probability that all data before this
   * timestamp has arrived.
   *
   * <p>Calculation formula:
   *
   * <pre>
   *   SafeWatermark = CurrentSystemTime - P99_Delay
   * </pre>
   *
   * <p>Meaning: If current system time is T and P99 delay is D, then data before timestamp T-D has
   * a 99% probability of having all arrived.
   *
   * @param currentSystemTime Current system time, unit: milliseconds
   * @param confidenceLevel Confidence level, e.g., 0.99 represents 99% confidence
   * @return Safe watermark timestamp, unit: milliseconds
   */
  public long getSafeWatermark(long currentSystemTime, double confidenceLevel) {
    long pDelay = getDelayQuantile(confidenceLevel);
    // Watermark = Current time - P99 delay
    long watermark = currentSystemTime - pDelay;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Calculated safe watermark: {} (currentTime: {}, P{} delay: {}ms)",
          watermark,
          currentSystemTime,
          (int) (confidenceLevel * 100),
          pDelay);
    }

    return watermark;
  }

  /**
   * Get the recommended safe watermark (using default confidence level)
   *
   * @param currentSystemTime Current system time, unit: milliseconds
   * @return Safe watermark timestamp, unit: milliseconds
   */
  public long getSafeWatermark(long currentSystemTime) {
    return getSafeWatermark(currentSystemTime, DEFAULT_CONFIDENCE_LEVEL);
  }

  /**
   * Get current statistics
   *
   * @return String containing statistics such as P50, P95, P99, maximum value, etc.
   */
  public String getStatistics() {
    if (!isFull && cursor == 0) {
      return "DelayAnalyzer: No data collected yet";
    }

    long p50 = getDelayQuantile(0.50);
    long p95 = getDelayQuantile(0.95);
    long p99 = getDelayQuantile(0.99);
    long max = getDelayQuantile(1.00);
    int currentSize = isFull ? windowSize : cursor;

    return String.format(
        "DelayAnalyzer Statistics -> Samples: %d/%d, P50: %dms, P95: %dms, P99: %dms, Max: %dms",
        currentSize, windowSize, p50, p95, p99, max);
  }

  /** Print current statistics (for debugging) */
  public void printStatistics() {
    LOGGER.info(getStatistics());
  }

  /**
   * Get the number of samples in the current window
   *
   * @return Current valid number of samples
   */
  public int getCurrentSampleCount() {
    lock.readLock().lock();
    try {
      return isFull ? windowSize : cursor;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get total number of samples (including samples that have been overwritten)
   *
   * @return Total number of samples recorded since creation
   */
  public long getTotalSamples() {
    lock.readLock().lock();
    try {
      return totalSamples;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get window size
   *
   * @return Window size
   */
  public int getWindowSize() {
    return windowSize;
  }

  /** Reset the analyzer, clearing all statistical data */
  public void reset() {
    lock.writeLock().lock();
    try {
      Arrays.fill(delaySamples, 0);
      cursor = 0;
      isFull = false;
      totalSamples = 0;
      LOGGER.debug("DelayAnalyzer has been reset");
    } finally {
      lock.writeLock().unlock();
    }
  }
}
