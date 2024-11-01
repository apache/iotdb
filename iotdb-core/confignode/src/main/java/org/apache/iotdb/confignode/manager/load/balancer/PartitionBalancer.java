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

  // Map<DatabaseName, DataPartitionPolicyTable>
  private final Map<String, DataPartitionPolicyTable> dataPartitionPolicyTableMap;

  public PartitionBalancer(IManager configManager) {
    this.configManager = configManager;
    this.dataPartitionPolicyTableMap = new ConcurrentHashMap<>();
  }

  /**
   * Allocate SchemaPartitions
   *
   * @param unassignedSchemaPartitionSlotsMap SchemaPartitionSlots that should be assigned
   * @return Map<DatabaseName, SchemaPartitionTable>, the allocating result
   */
  public Map<String, SchemaPartitionTable> allocateSchemaPartition(
      Map<String, List<TSeriesPartitionSlot>> unassignedSchemaPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    Map<String, SchemaPartitionTable> result = new HashMap<>();

    for (Map.Entry<String, List<TSeriesPartitionSlot>> slotsMapEntry :
        unassignedSchemaPartitionSlotsMap.entrySet()) {
      final String database = slotsMapEntry.getKey();
      final List<TSeriesPartitionSlot> unassignedPartitionSlots = slotsMapEntry.getValue();

      // Filter available SchemaRegionGroups and
      // sort them by the number of allocated SchemaPartitions
      BalanceTreeMap<TConsensusGroupId, Integer> counter = new BalanceTreeMap<>();
      List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          getPartitionManager()
              .getSortedRegionGroupSlotsCounter(database, TConsensusGroupType.SchemaRegion);
      for (Pair<Long, TConsensusGroupId> pair : regionSlotsCounter) {
        counter.put(pair.getRight(), pair.getLeft().intValue());
      }

      // Enumerate SeriesPartitionSlot
      Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new HashMap<>();
      for (TSeriesPartitionSlot seriesPartitionSlot : unassignedPartitionSlots) {
        // Greedy allocation: allocate the unassigned SchemaPartition to
        // the RegionGroup whose allocated SchemaPartitions is the least
        TConsensusGroupId consensusGroupId = counter.getKeyWithMinValue();
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
      try {
        allotTable.acquireLock();
        // Enumerate SeriesPartitionSlot
        for (Map.Entry<TSeriesPartitionSlot, TTimeSlotList> seriesPartitionEntry :
            unassignedPartitionSlotsMap.entrySet()) {
          SeriesPartitionTable seriesPartitionTable = new SeriesPartitionTable();

          // Enumerate TimePartitionSlot in ascending order
          TSeriesPartitionSlot seriesPartitionSlot = seriesPartitionEntry.getKey();
          List<TTimePartitionSlot> timePartitionSlots =
              seriesPartitionEntry.getValue().getTimePartitionSlots();
          timePartitionSlots.sort(Comparator.comparingLong(TTimePartitionSlot::getStartTime));

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

      try {
        dataPartitionPolicyTable.acquireLock();
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
              try {
                dataPartitionPolicyTable.acquireLock();

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
