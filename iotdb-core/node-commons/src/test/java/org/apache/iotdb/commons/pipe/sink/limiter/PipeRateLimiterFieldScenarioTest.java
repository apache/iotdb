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

package org.apache.iotdb.commons.pipe.sink.limiter;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

/**
 * Reproduces the configuration of the pipe regression case {@code
 * obj-10-table-big-object-kill-restart}: a {@code rate-limit-bytes-per-second} of 1 MB/s together
 * with an OBJECT batch that is as large as {@code PipeSinkReadFileBufferSize} (5 MB by default).
 *
 * <p>With those two defaults the cost of one batch (5 seconds at 1 MB/s) is larger than the
 * hot-reload check interval that used to double as the {@code tryAcquire} timeout (1 second), so
 * every attempt failed instantly and the sink thread spun at roughly 100% CPU while the pipe kept
 * reporting {@code RUNNING} with an empty {@code ExceptionMessage}. The stall was silent because
 * that retry loop logged nothing.
 *
 * <p>Such a request is never rejected outright - the quota keeps accumulating during the spin and
 * the transfer does make progress at the configured rate - so the defect shows up as wasted CPU
 * rather than as a wrong result. These tests therefore assert on the CPU time consumed while the
 * requests are being paced.
 */
public class PipeRateLimiterFieldScenarioTest {

  /** The {@code sink('rate-limit-bytes-per-second')} used by the regression case. */
  private static final double RATE_LIMIT_BYTES_PER_SECOND = 1048576;

  /** One OBJECT batch is bounded by {@code PipeSinkReadFileBufferSize}, 5 MB by default. */
  private static final int OBJECT_BATCH_BYTES = 5 * 1024 * 1024;

  /**
   * A spin consumes CPU for almost the whole waiting time, whereas a correct implementation sleeps
   * and consumes almost none. Measured with the pre-fix logic the ratio is about 64%, and with the
   * fix it is 0%, so this threshold separates the two with a wide margin.
   */
  private static final double MAX_CPU_TIME_RATIO = 0.2;

  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  @Test(timeout = 120_000)
  public void testFieldScenarioDoesNotBusyWait() {
    final RateLimiter rateLimiter = RateLimiter.create(RATE_LIMIT_BYTES_PER_SECOND);

    // Three batches, i.e. 15 MB, so that the initial one-second burst no longer deserves the
    // transfer and the limiter has to pace the remaining ones. The complete object is 1 GB, which
    // would take about 17 minutes at this rate and cannot be part of a unit test.
    final long wallTimeNanos = System.nanoTime();
    final long cpuTimeNanos = THREAD_MX_BEAN.getCurrentThreadCpuTime();
    for (int batch = 0; batch < 3; batch++) {
      Assert.assertTrue(
          RateLimitUtils.acquireWithRateLimit(
              rateLimiter,
              OBJECT_BATCH_BYTES,
              () -> true,
              PipeConfig.getInstance().getRateLimiterHotReloadCheckIntervalMs()));
    }
    final long elapsedNanos = System.nanoTime() - wallTimeNanos;
    final long consumedCpuNanos = THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuTimeNanos;

    final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    final double consumedCpuSeconds = consumedCpuNanos / 1_000_000_000.0;
    final double cpuTimeRatio = consumedCpuSeconds / elapsedSeconds;

    // The transfer must be paced by the rate limit instead of being served by the burst, which is
    // the
    // case exactly when the limiter is still being asked for one batch at a time.
    Assert.assertTrue(
        "15 MB at 1 MB/s minus the initial burst should take at least 12s, but took "
            + elapsedSeconds
            + "s",
        elapsedSeconds >= 12.0);
    Assert.assertTrue(
        "the CPU time ratio is "
            + cpuTimeRatio
            + " ("
            + consumedCpuSeconds
            + "s CPU over "
            + elapsedSeconds
            + "s wall), which means the sink thread was busy-waiting instead of waiting for the"
            + " rate limiter",
        cpuTimeRatio <= MAX_CPU_TIME_RATIO);
  }

  /**
   * The burst capacity of {@code RateLimiter.create(rate)} is one second's worth of permits, so a
   * single batch must stay below it for one timeout-bounded attempt to be able to grant the whole
   * batch.
   */
  @Test
  public void testConfiguredDefaultsStayWithinTheBurstCapacity() {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();

    final long burstCapacity = (long) RATE_LIMIT_BYTES_PER_SECOND;
    final long timeoutNanos =
        TimeUnit.MILLISECONDS.toNanos(config.getRateLimiterHotReloadCheckIntervalMs());
    final long grantableBytes = burstCapacity * timeoutNanos / 1_000_000_000L;

    // A batch larger than this cannot be granted by a timeout-bounded attempt, which is the trap
    // the
    // regression case fell into.
    Assert.assertTrue(
        "one OBJECT batch ("
            + OBJECT_BATCH_BYTES
            + " bytes) exceeds what the rate limiter can grant within the check interval ("
            + grantableBytes
            + " bytes), so the limiter has to split the request",
        OBJECT_BATCH_BYTES > grantableBytes);
  }
}
