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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared logging utilities for region migrators. Provides a unified summary log format and variance
 * computation so that GCR, Greedy, and PGP migrators produce consistent, comparable output.
 */
public class MigratorLogHelper {

  /** Scale factor for disk variance: convert bytes to MB to prevent long overflow. */
  public static final long DISK_SCALE_FACTOR = 1_000_000L;

  private MigratorLogHelper() {
    // utility class
  }

  /**
   * Log a unified summary at a checkpoint.
   *
   * <p>Output format:
   *
   * <pre>
   * [tag] [label] regionCounter={1=8, 2=8}, diskCounter={1=24000MB, 2=24000MB},
   *   Var(region)=0, Var(disk)=0, migrations=0, cost=0MB
   * </pre>
   *
   * @param logger the caller's logger instance
   * @param tag log prefix, e.g. "LoadBalance", "Greedy", "PGPRebalance"
   * @param label checkpoint label, e.g. "Initial", "Phase1", "Final"
   * @param allNodeIds all available DataNode IDs
   * @param regionCounter per-node region count
   * @param diskCounter per-node disk usage in bytes
   * @param migrations number of replica migrations
   * @param costBytes total migration cost in bytes
   */
  public static void logSummary(
      Logger logger,
      String tag,
      String label,
      Set<Integer> allNodeIds,
      Map<Integer, Integer> regionCounter,
      Map<Integer, Long> diskCounter,
      long migrations,
      long costBytes) {

    List<Integer> sortedNodeIds = new ArrayList<>(allNodeIds);
    Collections.sort(sortedNodeIds);

    StringBuilder regionSb = new StringBuilder("{");
    StringBuilder diskSb = new StringBuilder("{");
    boolean first = true;
    for (int nodeId : sortedNodeIds) {
      if (!first) {
        regionSb.append(", ");
        diskSb.append(", ");
      }
      regionSb.append(nodeId).append("=").append(regionCounter.getOrDefault(nodeId, 0));
      diskSb
          .append(nodeId)
          .append("=")
          .append(diskCounter.getOrDefault(nodeId, 0L) / DISK_SCALE_FACTOR)
          .append("MB");
      first = false;
    }
    regionSb.append("}");
    diskSb.append("}");

    long varRegion = computeVariance(regionCounter, allNodeIds, 1L);
    long varDisk = computeVariance(diskCounter, allNodeIds, DISK_SCALE_FACTOR);

    logger.debug(
        "[{}] [{}] regionCounter={}, diskCounter={}, Var(region)={}, Var(disk)={}, migrations={}, cost={}MB",
        tag,
        label,
        regionSb,
        diskSb,
        varRegion,
        varDisk,
        migrations,
        costBytes / DISK_SCALE_FACTOR);
  }

  /**
   * Compute integer-proportional variance: n * Σ(x/scale)² - (Σ(x/scale))².
   *
   * <p>This avoids floating-point precision loss. The result is proportional to the true variance
   * (multiplied by n²), which is sufficient for comparison purposes.
   *
   * @param counter per-node values (Integer or Long)
   * @param nodeIds set of node IDs to include
   * @param scaleFactor divide each value by this before computing (use 1L for region count,
   *     DISK_SCALE_FACTOR for disk bytes)
   * @return n * Σ(x/scale)² - (Σ(x/scale))², or 0 if nodeIds is empty
   */
  public static long computeVariance(
      Map<Integer, ? extends Number> counter, Set<Integer> nodeIds, long scaleFactor) {
    long sumValues = 0L;
    long sumSquares = 0L;
    int count = 0;
    for (int nodeId : nodeIds) {
      Number num = counter.get(nodeId);
      long value = (num != null ? num.longValue() : 0L) / scaleFactor;
      sumValues += value;
      sumSquares += value * value;
      count++;
    }
    if (count == 0) {
      return 0L;
    }
    return count * sumSquares - sumValues * sumValues;
  }
}
