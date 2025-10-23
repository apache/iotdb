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

package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.structure.BalanceTreeMap;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.partition.DataPartitionPolicyTable;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The SeriesPartitionSlotBalancer provides interfaces to generate optimal Partition allocation and
 * migration plans
 */
public class PartitionBalancer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionBalancer.class);

  private final IManager configManager;

  private final DataPartitionAllocationStrategy dataPartitionAllocationStrategy;
  // Map<DatabaseName, DataPartitionPolicyTable>, employed by INHERIT allocation strategy
  private final Map<String, DataPartitionPolicyTable> dataPartitionPolicyTableMap;

  private enum DataPartitionAllocationStrategy {
    // The INHERIT strategy tries to allocate adjacent DataPartitions as
    // consistent as possible, while ensuring load balancing.
    INHERIT,
    // The SHUFFLE strategy tries to allocate adjacent DataPartitions as
    // inconsistent as possible, note the result could be unbalanced.
    SHUFFLE
  }

  public PartitionBalancer(IManager configManager) {
    this.configManager = configManager;
    this.dataPartitionPolicyTableMap = new ConcurrentHashMap<>();
    switch (ConfigNodeDescriptor.getInstance().getConf().getDataPartitionAllocationStrategy()) {
      case "INHERIT":
        this.dataPartitionAllocationStrategy = DataPartitionAllocationStrategy.INHERIT;
        break;
      case "SHUFFLE":
        this.dataPartitionAllocationStrategy = DataPartitionAllocationStrategy.SHUFFLE;
        break;
      default:
        LOGGER.warn(
            "Unknown DataPartition allocation strategy {}, using INHERIT strategy by default.",
            ConfigNodeDescriptor.getInstance().getConf().getDataPartitionAllocationStrategy());
        this.dataPartitionAllocationStrategy = DataPartitionAllocationStrategy.INHERIT;
        break;
    }
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<DatabaseName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      final Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    final Map<String, SchemaPartitionTable> result = new HashMap<>();

    for (final Map.Entry<String, List<TSeriesPartitionSlot>> slotsMapEntry :
        unassignedSchemaPartitionSlotsMap.entrySet()) {
      final String database = slotsMapEntry.getKey();
      final List<TSeriesPartitionSlot> unassignedPartitionSlots = slotsMapEntry.getValue();

      // Filter available SchemaRegionGroups and
      // sort them by the number of allocated SchemaPartitions
      final BalanceTreeMap<TConsensusGroupId, Integer> counter = new BalanceTreeMap<>();
      final List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          getPartitionManager()
              .getSortedRegionGroupSlotsCounter(database, TConsensusGroupType.SchemaRegion);
      for (final Pair<Long, TConsensusGroupId> pair : regionSlotsCounter) {
        counter.put(pair.getRight(), pair.getLeft().intValue());
      }

      // Enumerate SeriesPartitionSlot
      final Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new HashMap<>();
      for (final TSeriesPartitionSlot seriesPartitionSlot : unassignedPartitionSlots) {
        // Greedy allocation: allocate the unassigned SchemaPartition to
        // the RegionGroup whose allocated SchemaPartitions is the least
        final TConsensusGroupId consensusGroupId = counter.getKeyWithMinValue();
        schemaPartitionMap.put(seriesPartitionSlot, consensusGroupId);
        counter.put(consensusGroupId, counter.get(consensusGroupId) + 1);
      }
      result.put(database, new SchemaPartitionTable(schemaPartitionMap));
    }

    return result;
  }

  /**
   * Allocate DataPartitions
   *
   * @param unassignedDataPartitionSlotsMap DataPartitionSlots that should be assigned
   * @throws DatabaseNotExistsException If some specific Databases don't exist
   * @throws NoAvailableRegionGroupException If there are no available RegionGroups
   * @return Map<DatabaseName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws DatabaseNotExistsException, NoAvailableRegionGroupException {
    Map<String, DataPartitionTable> result = new TreeMap<>();

    for (Map.Entry<String, Map<TSeriesPartitionSlot, TTimeSlotList>> slotsMapEntry :
        unassignedDataPartitionSlotsMap.entrySet()) {
      final String database = slotsMapEntry.getKey();
      final Map<TSeriesPartitionSlot, TTimeSlotList> unassignedPartitionSlotsMap =
          slotsMapEntry.getValue();

      // Filter available DataRegionGroups and
      // sort them by the number of allocated DataPartitions
      BalanceTreeMap<TConsensusGroupId, Integer> availableDataRegionGroupCounter =
          new BalanceTreeMap<>();
      List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          getPartitionManager()
              .getSortedRegionGroupSlotsCounter(database, TConsensusGroupType.DataRegion);
      for (Pair<Long, TConsensusGroupId> pair : regionSlotsCounter) {
        availableDataRegionGroupCounter.put(pair.getRight(), pair.getLeft().intValue());
      }

      DataPartitionTable dataPartitionTable = new DataPartitionTable();
      if (!dataPartitionPolicyTableMap.containsKey(database)) {
        throw new DatabaseNotExistsException(database);
      }
      DataPartitionPolicyTable allotTable = dataPartitionPolicyTableMap.get(database);
      allotTable.acquireLock();
      try {
        // Enumerate SeriesPartitionSlot
        for (Map.Entry<TSeriesPartitionSlot, TTimeSlotList> seriesPartitionEntry :
            unassignedPartitionSlotsMap.entrySet()) {
          SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();

          // Enumerate TimePartitionSlot in ascending order
          TSeriesPartitionSlot seriesPartitionSlot = seriesPartitionEntry.getKey();
          List<TTimePartitionSlot> timePartitionSlots =
              seriesPartitionEntry.getValue().getTimePartitionSlots();
          timePartitionSlots.sort(Comparator.comparingLong(TTimePartitionSlot::getStartTime));
          switch (dataPartitionAllocationStrategy) {
            case INHERIT:
              inheritAllocationStrategy(
                  database,
                  allotTable,
                  seriesPartitionSlot,
                  timePartitionSlots,
                  availableDataRegionGroupCounter,
                  seriesPartitionTable);
              break;
            case SHUFFLE:
              shuffleAllocationStrategy(
                  database,
                  seriesPartitionSlot,
                  timePartitionSlots,
                  availableDataRegionGroupCounter,
                  seriesPartitionTable);
              break;
          }
          dataPartitionTable
              .getDataPartitionMap()
              .put(seriesPartitionEntry.getKey(), seriesPartitionTable);
        }
      } finally {
        allotTable.releaseLock();
      }
      result.put(database, dataPartitionTable);
    }

    return result;
  }

  private void inheritAllocationStrategy(
      String database,
      DataPartitionPolicyTable allotTable,
      TSeriesPartitionSlot seriesPartitionSlot,
      List<TTimePartitionSlot> timePartitionSlots,
      BalanceTreeMap<TConsensusGroupId, Integer> availableDataRegionGroupCounter,
      SeriesPartitionTable seriesPartitionTable) {
    for (TTimePartitionSlot timePartitionSlot : timePartitionSlots) {

      // 1. The historical DataPartition will try to inherit successor DataPartition first
      TConsensusGroupId successor =
          getPartitionManager()
              .getSuccessorDataPartition(database, seriesPartitionSlot, timePartitionSlot);
      if (successor != null && availableDataRegionGroupCounter.containsKey(successor)) {
        seriesPartitionTable.putDataPartition(timePartitionSlot, successor);
        availableDataRegionGroupCounter.put(
            successor, availableDataRegionGroupCounter.get(successor) + 1);
        continue;
      }

      // 2. Assign DataPartition base on the DataAllotTable
      TConsensusGroupId allotGroupId =
          allotTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot);
      if (availableDataRegionGroupCounter.containsKey(allotGroupId)) {
        seriesPartitionTable.putDataPartition(timePartitionSlot, allotGroupId);
        availableDataRegionGroupCounter.put(
            allotGroupId, availableDataRegionGroupCounter.get(allotGroupId) + 1);
        continue;
      }

      // 3. The allotDataRegionGroup is unavailable,
      // try to inherit predecessor DataPartition
      TConsensusGroupId predecessor =
          getPartitionManager()
              .getPredecessorDataPartition(database, seriesPartitionSlot, timePartitionSlot);
      if (predecessor != null && availableDataRegionGroupCounter.containsKey(predecessor)) {
        seriesPartitionTable.putDataPartition(timePartitionSlot, predecessor);
        availableDataRegionGroupCounter.put(
            predecessor, availableDataRegionGroupCounter.get(predecessor) + 1);
        continue;
      }

      // 4. Assign the DataPartition to DataRegionGroup with the least DataPartitions
      // If the above DataRegionGroups are unavailable
      TConsensusGroupId greedyGroupId = availableDataRegionGroupCounter.getKeyWithMinValue();
      seriesPartitionTable.putDataPartition(timePartitionSlot, greedyGroupId);
      availableDataRegionGroupCounter.put(
          greedyGroupId, availableDataRegionGroupCounter.get(greedyGroupId) + 1);
      LOGGER.warn(
          "[PartitionBalancer] The SeriesSlot: {} in TimeSlot: {} will be allocated to DataRegionGroup: {}, because the original target: {} is currently unavailable.",
          seriesPartitionSlot,
          timePartitionSlot,
          greedyGroupId,
          allotGroupId);
    }
  }

  private void shuffleAllocationStrategy(
      String database,
      TSeriesPartitionSlot seriesPartitionSlot,
      List<TTimePartitionSlot> timePartitionSlots,
      BalanceTreeMap<TConsensusGroupId, Integer> availableDataRegionGroupCounter,
      SeriesPartitionTable seriesPartitionTable) {
    final SecureRandom random = new SecureRandom();
    List<TConsensusGroupId> availableDataRegionGroups =
        new ArrayList<>(availableDataRegionGroupCounter.keySet());
    for (TTimePartitionSlot timePartitionSlot : timePartitionSlots) {
      if (availableDataRegionGroups.size() == 1) {
        // Only one available DataRegionGroup
        seriesPartitionTable.putDataPartition(
            timePartitionSlot, availableDataRegionGroups.iterator().next());
        continue;
      }
      TConsensusGroupId predecessor =
          getPartitionManager()
              .getPredecessorDataPartition(database, seriesPartitionSlot, timePartitionSlot);
      TConsensusGroupId successor =
          getPartitionManager()
              .getSuccessorDataPartition(database, seriesPartitionSlot, timePartitionSlot);
      if (predecessor != null
          && successor != null
          && !predecessor.equals(successor)
          && availableDataRegionGroups.size() == 2) {
        // Only two available DataRegionGroups and predecessor equals successor
        seriesPartitionTable.putDataPartition(
            timePartitionSlot, random.nextBoolean() ? successor : predecessor);
        continue;
      }
      TConsensusGroupId targetGroupId;
      do {
        // Randomly pick a DataRegionGroup from availableDataRegionGroups
        targetGroupId =
            availableDataRegionGroups.get(random.nextInt(availableDataRegionGroups.size()));
      } while (targetGroupId.equals(predecessor) || targetGroupId.equals(successor));
      seriesPartitionTable.putDataPartition(timePartitionSlot, targetGroupId);
    }
  }

  /**
   * Re-balance the DataPartitionPolicyTable.
   *
   * @param database Database name
   */
  public void reBalanceDataPartitionPolicy(String database) {
    try {
      DataPartitionPolicyTable dataPartitionPolicyTable =
          dataPartitionPolicyTableMap.computeIfAbsent(
              database, empty -> new DataPartitionPolicyTable());

      dataPartitionPolicyTable.acquireLock();
      try {
        dataPartitionPolicyTable.reBalanceDataPartitionPolicy(
            getPartitionManager().getAllRegionGroupIds(database, TConsensusGroupType.DataRegion));
        dataPartitionPolicyTable.logDataAllotTable(database);
      } finally {
        dataPartitionPolicyTable.releaseLock();
      }

    } catch (DatabaseNotExistsException e) {
      LOGGER.error("Database {} not exists when updateDataAllotTable", database);
    }
  }

  /** Set up the PartitionBalancer when the current ConfigNode becomes leader. */
  public void setupPartitionBalancer() {
    dataPartitionPolicyTableMap.clear();
    getClusterSchemaManager()
        .getDatabaseNames(null)
        .forEach(
            database -> {
              DataPartitionPolicyTable dataPartitionPolicyTable = new DataPartitionPolicyTable();
              dataPartitionPolicyTableMap.put(database, dataPartitionPolicyTable);

              dataPartitionPolicyTable.acquireLock();
              try {
                // Put all DataRegionGroups into the DataPartitionPolicyTable
                dataPartitionPolicyTable.reBalanceDataPartitionPolicy(
                    getPartitionManager()
                        .getAllRegionGroupIds(database, TConsensusGroupType.DataRegion));
                // Load the last DataAllotTable
                dataPartitionPolicyTable.setDataAllotMap(
                    getPartitionManager().getLastDataAllotTable(database));
              } catch (DatabaseNotExistsException e) {
                LOGGER.error("Database {} not exists when setupPartitionBalancer", database);
              } finally {
                dataPartitionPolicyTable.releaseLock();
              }
            });
  }

  /** Clear the PartitionBalancer when the current ConfigNode is no longer the leader. */
  public void clearPartitionBalancer() {
    dataPartitionPolicyTableMap.clear();
  }

  public void clearDataPartitionPolicyTable(String database) {
    dataPartitionPolicyTableMap.remove(database);
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
