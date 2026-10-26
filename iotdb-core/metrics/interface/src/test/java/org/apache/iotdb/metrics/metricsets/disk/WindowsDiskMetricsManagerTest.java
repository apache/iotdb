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

package org.apache.iotdb.metrics.metricsets.disk;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WindowsDiskMetricsManagerTest {

  @Test
  public void testCollectWindowsDiskMetrics() {
    AtomicInteger processQueryCount = new AtomicInteger();
    WindowsDiskMetricsManager manager =
        new WindowsDiskMetricsManager(
            "123",
            command -> {
              if (command.contains("PhysicalDisk")) {
                return new WindowsDiskMetricsManager.CommandResult(
                    0, Arrays.asList("0 C:\t1\t2\t1024\t4096\t0.001\t0.002\t75\t3"));
              }
              if (command.contains("PerfProc_Process")) {
                processQueryCount.incrementAndGet();
                return new WindowsDiskMetricsManager.CommandResult(
                    0, Arrays.asList("3\t4\t8192\t16384"));
              }
              return new WindowsDiskMetricsManager.CommandResult(1, Arrays.asList("unexpected"));
            });

    Set<String> diskIds = manager.getDiskIds();

    assertTrue(diskIds.contains("0 C:"));
    assertEquals(1, processQueryCount.get());
    assertEquals(0.25, manager.getIoUtilsPercentage().get("0 C:"), 0.0001);
    assertEquals(3.0, manager.getQueueSizeForDisk().get("0 C:"), 0.0001);
    assertEquals(1.0, manager.getAvgReadCostTimeOfEachOpsForDisk().get("0 C:"), 0.0001);
    assertEquals(2.0, manager.getAvgWriteCostTimeOfEachOpsForDisk().get("0 C:"), 0.0001);
    assertEquals(1024.0, manager.getAvgSizeOfEachReadForDisk().get("0 C:"), 0.0001);
    assertEquals(2048.0, manager.getAvgSizeOfEachWriteForDisk().get("0 C:"), 0.0001);
  }

  @Test
  public void testPowerShellFailureSkipsFollowingQueryDuringBackoff() {
    AtomicInteger executeCount = new AtomicInteger();
    WindowsDiskMetricsManager manager =
        new WindowsDiskMetricsManager(
            "123",
            command -> {
              executeCount.incrementAndGet();
              throw new IOException("CreateProcess error=5");
            });

    assertTrue(manager.getDiskIds().isEmpty());
    assertEquals(1, executeCount.get());
  }
}
