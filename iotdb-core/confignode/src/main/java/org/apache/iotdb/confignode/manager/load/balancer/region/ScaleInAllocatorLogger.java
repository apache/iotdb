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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.migrator.MigratorLogHelper;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Shared logging utility for scale-in allocators ({@link GreedyCopySetRegionGroupAllocator} and
 * {@link GreedyRegionGroupAllocator}).
 *
 * <p>Logs per-node region counts (including removed nodes), disk usage, variance, migrations, cost,
 * and per-database max loads in a single line per phase, matching the format of {@code
 * GreedyCopySetRegionGroupMigrator.logSummary}.
 *
 * <p>Example output:
 *
 * <pre>
 * [GCR] [Initial] regionCounter={1=5, 2=5, 3=5, 4=5, 5=5}, diskCounter={1=500MB, ...},
 *   Var(region)=0, Var(disk)=0, migrations=5, cost=500MB
 * [GCR] [Final] regionCounter={1=6, 2=6, 3=6, 4=6}, diskCounter={1=600MB, ...},
 *   Var(region)=0, Var(disk)=0, migrations=5, cost=500MB
 * </pre>
 */
class ScaleInAllocatorLogger {

  /**
   * Log scale-in summary with per-node region counts, disk usage, and migration cost.
   *
   * <p>For the Initial phase (result == null), removed nodes are inferred from {@code
   * removedNodeMap} and their region/disk counts are included in the output so that the log shows
   * the complete cluster state before migration.
   *
   * @param logger the caller's Logger instance
   * @param tag algorithm name tag (e.g. "GCR" or "Greedy")
   * @param label phase label (e.g. "Initial" or "Final")
   * @param availableDataNodeMap available nodes after removing the to-be-removed nodes
   * @param allocatedRegionGroups current allocated region groups (with removed nodes already
   *     excluded from locations)
   * @param result the migration result; null for "Initial" phase
   * @param regionDatabaseMap region to database mapping
   * @param remainReplicasMap regions that need migration
   * @param regionDiskUsageMap disk usage (in bytes) per RegionGroup; may be null or empty
   * @param removedNodeMap mapping from each region that needs migration to the removed DataNode
   *     that held its replica; used to reconstruct removed-node load in Initial phase
   */
  static void logSummary(
      Logger logger,
      String tag,
      String label,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, TDataNodeConfiguration> result,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap,
      Map<TConsensusGroupId, Long> regionDiskUsageMap,
      Map<TConsensusGroupId, List<Integer>> removedNodeMap) {

    // Collect all node IDs to display: available nodes + removed nodes (for Initial phase)
    Set<Integer> allNodeIds = new TreeSet<>(availableDataNodeMap.keySet());
    if (result == null && removedNodeMap != null) {
      for (List<Integer> nodeIds : removedNodeMap.values()) {
        allNodeIds.addAll(nodeIds);
      }
    }
    List<Integer> sortedNodeIds = new ArrayList<>(allNodeIds);
    Collections.sort(sortedNodeIds);

    // Compute per-node region counts and disk usage from allocatedRegionGroups
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    Map<Integer, Long> diskCounter = new TreeMap<>();
    for (int nodeId : sortedNodeIds) {
      regionCounter.put(nodeId, 0);
      diskCounter.put(nodeId, 0L);
    }
    for (TRegionReplicaSet rs : allocatedRegionGroups) {
      long diskUsage =
          regionDiskUsageMap != null ? regionDiskUsageMap.getOrDefault(rs.getRegionId(), 0L) : 0L;
      for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
        if (regionCounter.containsKey(loc.getDataNodeId())) {
          regionCounter.merge(loc.getDataNodeId(), 1, Integer::sum);
          diskCounter.merge(loc.getDataNodeId(), diskUsage, Long::sum);
        }
      }
    }

    long migrations = 0;
    long costBytes = 0;
    if (result == null) {
      // Initial phase: reconstruct removed-node load from removedNodeMap
      if (removedNodeMap != null) {
        for (Map.Entry<TConsensusGroupId, List<Integer>> entry : removedNodeMap.entrySet()) {
          long diskUsage =
              regionDiskUsageMap != null ? regionDiskUsageMap.getOrDefault(entry.getKey(), 0L) : 0L;
          for (int removedNodeId : entry.getValue()) {
            regionCounter.merge(removedNodeId, 1, Integer::sum);
            diskCounter.merge(removedNodeId, diskUsage, Long::sum);
          }
        }
      }
      // migrations and cost show what needs to be migrated
      migrations = remainReplicasMap.size();
      for (TConsensusGroupId regionId : remainReplicasMap.keySet()) {
        costBytes +=
            regionDiskUsageMap != null ? regionDiskUsageMap.getOrDefault(regionId, 0L) : 0L;
      }
    } else {
      // Final phase: only show available nodes, add migrated regions to counters
      for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
        int nodeId = entry.getValue().getLocation().getDataNodeId();
        regionCounter.merge(nodeId, 1, Integer::sum);
        long diskUsage =
            regionDiskUsageMap != null ? regionDiskUsageMap.getOrDefault(entry.getKey(), 0L) : 0L;
        diskCounter.merge(nodeId, diskUsage, Long::sum);
        migrations++;
        costBytes += diskUsage;
      }
    }

    // Compute variance metrics (only over available nodes for meaningful comparison)
    Map<Integer, Integer> availableRegionCounter = new TreeMap<>();
    Map<Integer, Long> availableDiskCounter = new TreeMap<>();
    for (int nodeId : availableDataNodeMap.keySet()) {
      availableRegionCounter.put(nodeId, regionCounter.getOrDefault(nodeId, 0));
      availableDiskCounter.put(nodeId, diskCounter.getOrDefault(nodeId, 0L));
    }
    long varRegion =
        MigratorLogHelper.computeVariance(
            availableRegionCounter, availableDataNodeMap.keySet(), 1L);
    long varDisk =
        MigratorLogHelper.computeVariance(
            availableDiskCounter,
            availableDataNodeMap.keySet(),
            MigratorLogHelper.DISK_SCALE_FACTOR);

    // Build diskCounter display string (includes all nodes for Initial, available only for Final)
    List<Integer> displayNodeIds =
        result == null ? sortedNodeIds : new ArrayList<>(availableDataNodeMap.keySet());
    if (result != null) {
      Collections.sort(displayNodeIds);
    }
    StringBuilder diskSb = new StringBuilder("{");
    boolean first = true;
    for (int nodeId : displayNodeIds) {
      if (!first) {
        diskSb.append(", ");
      }
      diskSb.append(nodeId).append("=").append(toMB(diskCounter.getOrDefault(nodeId, 0L)));
      first = false;
    }
    diskSb.append("}");

    // Build regionCounter display map (includes all nodes for Initial, available only for Final)
    Map<Integer, Integer> displayRegionCounter = new TreeMap<>();
    for (int nodeId : displayNodeIds) {
      displayRegionCounter.put(nodeId, regionCounter.getOrDefault(nodeId, 0));
    }

    // Log main line
    logger.info(
        "[{}] [{}] regionCounter={}, diskCounter={}, Var(region)={}, Var(disk)={}, migrations={}, cost={}",
        tag,
        label,
        displayRegionCounter,
        diskSb,
        varRegion,
        varDisk,
        migrations,
        toMB(costBytes));

    // Log per-database max load if multi-database
    Set<String> databases = new TreeSet<>(regionDatabaseMap.values());
    if (databases.size() > 1) {
      // Only compute db max load over available nodes
      List<Integer> availableNodeIds = new ArrayList<>(availableDataNodeMap.keySet());
      Collections.sort(availableNodeIds);
      Map<String, Map<Integer, Integer>> dbNodeCounter = new TreeMap<>();
      for (String db : databases) {
        Map<Integer, Integer> nodeCounter = new TreeMap<>();
        for (int nodeId : availableNodeIds) {
          nodeCounter.put(nodeId, 0);
        }
        dbNodeCounter.put(db, nodeCounter);
      }
      for (TRegionReplicaSet rs : allocatedRegionGroups) {
        String db = regionDatabaseMap.get(rs.getRegionId());
        if (db != null && dbNodeCounter.containsKey(db)) {
          for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
            if (dbNodeCounter.get(db).containsKey(loc.getDataNodeId())) {
              dbNodeCounter.get(db).merge(loc.getDataNodeId(), 1, Integer::sum);
            }
          }
        }
      }
      if (result != null) {
        for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
          String db = regionDatabaseMap.get(entry.getKey());
          if (db != null && dbNodeCounter.containsKey(db)) {
            int nodeId = entry.getValue().getLocation().getDataNodeId();
            dbNodeCounter.get(db).merge(nodeId, 1, Integer::sum);
          }
        }
      }

      Map<String, Integer> dbMaxLoad = new TreeMap<>();
      int dbLoadSquaredSum = 0;
      for (Map.Entry<String, Map<Integer, Integer>> entry : dbNodeCounter.entrySet()) {
        int maxInDb = entry.getValue().values().stream().max(Integer::compareTo).orElse(0);
        dbMaxLoad.put(entry.getKey(), maxInDb);
        dbLoadSquaredSum += maxInDb * maxInDb;
      }
      logger.info(
          "[{}] [{}] dbMaxLoad={}, dbLoadSquaredSum={}", tag, label, dbMaxLoad, dbLoadSquaredSum);
    }
  }

  /**
   * Infer the removed node ID for each region that needs migration by comparing the
   * databaseAllocatedRegionGroupMap (which may still contain removed nodes' locations) with the
   * remainReplicasMap (which only has remaining locations).
   *
   * <p>For each region in remainReplicasMap, find the same region in
   * databaseAllocatedRegionGroupMap's lists, then diff the locations to find the removed node.
   *
   * @return mapping from each region needing migration to the list of removed node IDs
   */
  static Map<TConsensusGroupId, List<Integer>> inferRemovedNodeMap(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {

    Map<TConsensusGroupId, List<Integer>> removedNodeMap = new HashMap<>();

    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> entry : remainReplicasMap.entrySet()) {
      TConsensusGroupId regionId = entry.getKey();
      Set<Integer> remainNodeIds = new TreeSet<>();
      for (TDataNodeLocation loc : entry.getValue().getDataNodeLocations()) {
        remainNodeIds.add(loc.getDataNodeId());
      }

      // Find this region's full replica set in databaseAllocatedRegionGroupMap
      String db = regionDatabaseMap.get(regionId);
      if (db != null && databaseAllocatedRegionGroupMap.containsKey(db)) {
        for (TRegionReplicaSet fullRs : databaseAllocatedRegionGroupMap.get(db)) {
          if (fullRs.getRegionId().equals(regionId)) {
            // Collect all nodes in fullRs that are not in remain and not in available
            List<Integer> removedNodes = new ArrayList<>();
            for (TDataNodeLocation loc : fullRs.getDataNodeLocations()) {
              int nodeId = loc.getDataNodeId();
              if (!remainNodeIds.contains(nodeId) && !availableDataNodeMap.containsKey(nodeId)) {
                removedNodes.add(nodeId);
              }
            }
            if (!removedNodes.isEmpty()) {
              removedNodeMap.put(regionId, removedNodes);
            }
            break;
          }
        }
      }
    }
    return removedNodeMap;
  }

  /**
   * Compute region count variance after applying a migration result. Used by Greedy and Random
   * allocators to evaluate candidate solutions.
   *
   * @param result migration result: regionId -> chosen DataNode
   * @param allocatedRegionGroups current allocated region groups (before migration)
   * @param availableDataNodeMap available DataNodes (defines the node set for variance)
   * @return integer-proportional variance: n * Σ(x²) - (Σx)²
   */
  static long computeResultVariance(
      Map<TConsensusGroupId, TDataNodeConfiguration> result,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap) {
    Map<Integer, Integer> counter = new HashMap<>();
    for (int nodeId : availableDataNodeMap.keySet()) {
      counter.put(nodeId, 0);
    }
    for (TRegionReplicaSet rs : allocatedRegionGroups) {
      for (TDataNodeLocation loc : rs.getDataNodeLocations()) {
        counter.computeIfPresent(loc.getDataNodeId(), (k, v) -> v + 1);
      }
    }
    for (Map.Entry<TConsensusGroupId, TDataNodeConfiguration> entry : result.entrySet()) {
      int nodeId = entry.getValue().getLocation().getDataNodeId();
      counter.merge(nodeId, 1, Integer::sum);
    }
    return MigratorLogHelper.computeVariance(counter, availableDataNodeMap.keySet(), 1L);
  }

  private static String toMB(long bytes) {
    return (bytes / MigratorLogHelper.DISK_SCALE_FACTOR) + "MB";
  }

  private ScaleInAllocatorLogger() {
    // utility class
  }
}
