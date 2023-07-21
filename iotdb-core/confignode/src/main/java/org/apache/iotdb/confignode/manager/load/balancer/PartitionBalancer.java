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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.commons.structure.BalanceTreeMap;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NoAvailableRegionGroupException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.partition.DataAllotTable;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The SeriesPartitionSlotBalancer provides interfaces to generate optimal Partition allocation and
 * migration plans
 */
public class PartitionBalancer {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();
  private static final long TIME_PARTITION_INTERVAL =
      CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionBalancer.class);

  private final IManager configManager;

  // Map<DatabaseName, DataAllotTable>
  private final Map<String, DataAllotTable> dataAllotTableMap;

  public PartitionBalancer(IManager configManager) {
    this.configManager = configManager;
    this.dataAllotTableMap = new ConcurrentHashMap<>();
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
    Map<String, SchemaPartitionTable> result = new ConcurrentHashMap<>();

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
      Map<TSeriesPartitionSlot, TConsensusGroupId> schemaPartitionMap = new ConcurrentHashMap<>();
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
   * @return Map<DatabaseName, DataPartitionTable>, the allocating result
   */
  public Map<String, DataPartitionTable> allocateDataPartition(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> unassignedDataPartitionSlotsMap)
      throws NoAvailableRegionGroupException {
    Map<String, DataPartitionTable> result = new ConcurrentHashMap<>();

    for (Map.Entry<String, Map<TSeriesPartitionSlot, TTimeSlotList>> slotsMapEntry :
        unassignedDataPartitionSlotsMap.entrySet()) {
      final String database = slotsMapEntry.getKey();
      final Map<TSeriesPartitionSlot, TTimeSlotList> unassignedPartitionSlotsMap =
          slotsMapEntry.getValue();

      // Filter available DataRegionGroups and
      // sort them by the number of allocated DataPartitions
      BalanceTreeMap<TConsensusGroupId, Integer> counter = new BalanceTreeMap<>();
      List<Pair<Long, TConsensusGroupId>> regionSlotsCounter =
          getPartitionManager()
              .getSortedRegionGroupSlotsCounter(database, TConsensusGroupType.DataRegion);
      for (Pair<Long, TConsensusGroupId> pair : regionSlotsCounter) {
        counter.put(pair.getRight(), pair.getLeft().intValue());
      }

      DataAllotTable allotTable = dataAllotTableMap.get(database);
      allotTable.acquireReadLock();
      TTimePartitionSlot currentTimePartition = allotTable.getCurrentTimePartition();
      DataPartitionTable dataPartitionTable = new DataPartitionTable();

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

          for (TTimePartitionSlot timePartitionSlot : timePartitionSlots) {

            // 1. The historical DataPartition will try to inherit successor DataPartition first
            if (timePartitionSlot.getStartTime() < currentTimePartition.getStartTime()) {
              TConsensusGroupId successor =
                  getPartitionManager()
                      .getSuccessorDataPartition(database, seriesPartitionSlot, timePartitionSlot);
              if (successor != null && counter.containsKey(successor)) {
                seriesPartitionTable.putDataPartition(timePartitionSlot, successor);
                counter.put(successor, counter.get(successor) + 1);
                continue;
              }
            }

            // 2. Assign DataPartition base on the DataAllotTable
            TConsensusGroupId allotGroupId = allotTable.getRegionGroupId(seriesPartitionSlot);
            if (counter.containsKey(allotGroupId)) {
              seriesPartitionTable.putDataPartition(timePartitionSlot, allotGroupId);
              counter.put(allotGroupId, counter.get(allotGroupId) + 1);
              continue;
            }

            // 3. Assign the DataPartition to DataRegionGroup with the least DataPartitions
            // If the above DataRegionGroups are unavailable
            TConsensusGroupId greedyGroupId = counter.getKeyWithMinValue();
            seriesPartitionTable.putDataPartition(timePartitionSlot, greedyGroupId);
            counter.put(greedyGroupId, counter.get(greedyGroupId) + 1);
          }

          dataPartitionTable
              .getDataPartitionMap()
              .put(seriesPartitionEntry.getKey(), seriesPartitionTable);
        }
      } finally {
        allotTable.releaseReadLock();
      }
      result.put(database, dataPartitionTable);
    }

    return result;
  }

  /**
   * Try to re-balance the DataPartitionPolicy when new DataPartitions are created.
   *
   * @param assignedDataPartition new created DataPartitions
   */
  public void reBalanceDataPartitionPolicyIfNecessary(
      Map<String, DataPartitionTable> assignedDataPartition) {
    assignedDataPartition.forEach(
        (database, dataPartitionTable) -> {
          if (updateDataPartitionCount(database, dataPartitionTable)) {
            // Update the DataAllotTable if the currentTimePartition is updated
            updateDataAllotTable(database);
          }
        });
  }

  /**
   * Update the DataPartitionCount in DataAllotTable
   *
   * @param database Database name
   * @param dataPartitionTable new created DataPartitionTable
   * @return true if the currentTimePartition is updated, false otherwise
   */
  public boolean updateDataPartitionCount(String database, DataPartitionTable dataPartitionTable) {
    try {
      dataAllotTableMap
          .get(database)
          .addTimePartitionCount(dataPartitionTable.getTimeSlotCountMap());
      return dataAllotTableMap
          .get(database)
          .updateCurrentTimePartition(
              getPartitionManager().getRegionGroupCount(database, TConsensusGroupType.DataRegion));
    } catch (DatabaseNotExistsException e) {
      LOGGER.error("Database {} not exists", database);
      return false;
    }
  }

  /**
   * Update the DataAllotTable
   *
   * @param database Database name
   */
  public void updateDataAllotTable(String database) {
    TTimePartitionSlot currentTimePartition =
        dataAllotTableMap.get(database).getCurrentTimePartition();
    Map<TSeriesPartitionSlot, TConsensusGroupId> allocatedTable = new ConcurrentHashMap<>();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      Pair<TTimePartitionSlot, TConsensusGroupId> lastDataPartition =
          getPartitionManager().getLastDataPartition(database, seriesPartitionSlot);
      if (lastDataPartition != null
          && currentTimePartition.compareTo(lastDataPartition.getLeft()) < 0) {
        // Put all future DataPartitions into the allocatedTable
        allocatedTable.put(seriesPartitionSlot, lastDataPartition.getRight());
      }
    }

    try {
      dataAllotTableMap
          .get(database)
          .updateDataAllotTable(
              getPartitionManager().getAllRegionGroupIds(database, TConsensusGroupType.DataRegion),
              allocatedTable);
    } catch (DatabaseNotExistsException e) {
      LOGGER.error("Database {} not exists", database);
    }
  }

  /** Set up the PartitionBalancer when the current ConfigNode becomes leader. */
  public void setupPartitionBalancer() {
    dataAllotTableMap.clear();
    getClusterSchemaManager()
        .getDatabaseNames()
        .forEach(
            database -> {
              dataAllotTableMap.put(database, new DataAllotTable());
              DataAllotTable dataAllotTable = dataAllotTableMap.get(database);
              dataAllotTable.acquireWriteLock();
              try {
                int threshold =
                    DataAllotTable.timePartitionThreshold(
                        getPartitionManager()
                            .getRegionGroupCount(database, TConsensusGroupType.DataRegion));
                TTimePartitionSlot maxTimePartitionSlot =
                    getPartitionManager().getMaxTimePartitionSlot(database);
                TTimePartitionSlot minTimePartitionSlot =
                    getPartitionManager().getMinTimePartitionSlot(database);
                TTimePartitionSlot currentTimePartition = maxTimePartitionSlot.deepCopy();
                while (currentTimePartition.compareTo(minTimePartitionSlot) > 0) {
                  int seriesSlotCount =
                      getPartitionManager().countSeriesSlot(database, currentTimePartition);
                  if (seriesSlotCount >= threshold) {
                    dataAllotTable.setCurrentTimePartition(currentTimePartition.getStartTime());
                    break;
                  }
                  dataAllotTable.addTimePartitionCount(
                      Collections.singletonMap(currentTimePartition, seriesSlotCount));
                  currentTimePartition.setStartTime(
                      currentTimePartition.getStartTime() - TIME_PARTITION_INTERVAL);
                }

                dataAllotTable.setDataAllotTable(
                    getPartitionManager().getLastDataAllotTable(database));
              } catch (DatabaseNotExistsException e) {
                LOGGER.error("Database {} not exists", database);
              } finally {
                dataAllotTable.releaseWriteLock();
              }
            });
  }

  /** Clear the PartitionBalancer when the current ConfigNode is no longer the leader. */
  public void clearPartitionBalancer() {
    dataAllotTableMap.clear();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
