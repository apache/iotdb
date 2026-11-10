/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.disk;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LinuxDiskMetricsManagerTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Path diskStats;
  private Path diskIds;
  private Path processIo;
  private final AtomicLong clock = new AtomicLong(10001);

  @Before
  public void setUp() throws Exception {
    diskStats = temporaryFolder.newFile("diskstats").toPath();
    diskIds = temporaryFolder.newFolder("block").toPath();
    Path queue = Files.createDirectories(diskIds.resolve("vda/queue"));
    Files.write(queue.resolve("hw_sector_size"), "512\n".getBytes(StandardCharsets.UTF_8));
    processIo = temporaryFolder.newFile("io").toPath();
    Files.write(processIo, "syscr: 1\nsyscw: 2\n".getBytes(StandardCharsets.UTF_8));
    writeDiskStats(10, 100, 200);
  }

  @Test
  public void testFirstSampleAndUnchangedOrBackwardClock() throws Exception {
    LinuxDiskMetricsManager manager = newManager(clock::get);
    assertEquals(0.0, manager.getIoUtilsPercentage().get("vda"), 0.0);
    assertEquals(0.0, manager.getQueueSizeForDisk().get("vda"), 0.0);

    clock.addAndGet(20000);
    writeDiskStats(20, 5100, 10200);
    assertEquals(0.25, manager.getIoUtilsPercentage().get("vda"), 0.0);
    assertEquals(0.5, manager.getQueueSizeForDisk().get("vda"), 0.0);
    Map<String, Long> previousCounts = manager.getReadOperationCountForDisk();

    writeDiskStats(30, 15100, 30200);
    // Neither a zero interval nor a backward wall clock replaces the last valid sample.
    assertEquals(0.25, manager.getIoUtilsPercentage().get("vda"), 0.0);
    clock.set(1);
    assertEquals(0.5, manager.getQueueSizeForDisk().get("vda"), 0.0);
    clock.set(50001);
    assertEquals(0.5, manager.getIoUtilsPercentage().get("vda"), 0.0);
    assertEquals(1.0, manager.getQueueSizeForDisk().get("vda"), 0.0);
    assertEquals(Long.valueOf(20), previousCounts.get("vda"));
    assertEquals(Long.valueOf(30), manager.getReadOperationCountForDisk().get("vda"));
  }

  @Test
  public void testRefreshUsesTheTimestampCheckedForEligibility() {
    // A second clock read would return the previous sample time and cause a zero denominator.
    AtomicLong nextTime = new AtomicLong(10001);
    LinuxDiskMetricsManager manager = newManager(() -> nextTime.getAndSet(10001));
    manager.getReadDataSizeForDisk();
    nextTime.set(30001);
    manager.getReadDataSizeForDisk();
    assertEquals(0.0, manager.getIoUtilsPercentage().get("vda"), 0.0);
    assertEquals(0.0, manager.getQueueSizeForDisk().get("vda"), 0.0);
  }

  @Test
  public void testConcurrentReadersWaitForCompleteSample() throws Exception {
    CountDownLatch sampling = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    LinuxDiskMetricsManager manager =
        newManager(
            () -> {
              sampling.countDown();
              try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              return clock.get();
            });
    FutureTask<Map<String, Double>> refresh = new FutureTask<>(manager::getReadDataSizeForDisk);
    FutureTask<Map<String, Double>> read = new FutureTask<>(manager::getIoUtilsPercentage);
    Thread refreshing = new Thread(refresh, "disk-refresh");
    Thread reading = new Thread(read, "disk-read");
    try {
      refreshing.start();
      assertTrue(sampling.await(10, TimeUnit.SECONDS));
      reading.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (reading.getState() != Thread.State.BLOCKED && !read.isDone()) {
        if (System.nanoTime() > deadline) {
          fail("The metric reader did not wait for the active disk sample");
        }
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      }
      assertEquals(Thread.State.BLOCKED, reading.getState());
      release.countDown();
      refresh.get(10, TimeUnit.SECONDS);
      assertEquals(0.0, read.get(10, TimeUnit.SECONDS).get("vda"), 0.0);
    } finally {
      release.countDown();
      refresh.cancel(true);
      read.cancel(true);
      refreshing.join(TimeUnit.SECONDS.toMillis(10));
      reading.join(TimeUnit.SECONDS.toMillis(10));
    }
  }

  private LinuxDiskMetricsManager newManager(LongSupplier timeSupplier) {
    return new LinuxDiskMetricsManager(diskStats, diskIds, processIo, timeSupplier);
  }

  private void writeDiskStats(long reads, long busyTime, long queueTime) throws Exception {
    String stats = "   8 0 vda " + reads + " 0 40 0 0 0 0 0 0 " + busyTime + " " + queueTime + "\n";
    Files.write(diskStats, stats.getBytes(StandardCharsets.UTF_8));
  }
}
