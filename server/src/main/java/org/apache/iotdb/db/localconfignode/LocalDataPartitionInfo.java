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

package org.apache.iotdb.db.localconfignode;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// This class is used for data partition maintaining the map between storage group and
// dataRegionIds.
public class LocalDataPartitionInfo {

  // storageGroup -> LocalDataPartitionTable
  private Map<PartialPath, LocalDataPartitionTable> partitionTableMap;

  private static class LocalDataPartitionTableHolder {
    private static final LocalDataPartitionInfo INSTANCE = new LocalDataPartitionInfo();

    private LocalDataPartitionTableHolder() {}
  }

  private LocalDataPartitionInfo() {}

  public static LocalDataPartitionInfo getInstance() {
    return LocalDataPartitionTableHolder.INSTANCE;
  }

  public synchronized void init(Map<String, List<DataRegionId>> regionInfos)
      throws IllegalPathException {
    partitionTableMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, List<DataRegionId>> entry : regionInfos.entrySet()) {
      String storageGroupName = entry.getKey();
      List<DataRegionId> regionIds = entry.getValue();
      LocalDataPartitionTable table = new LocalDataPartitionTable(storageGroupName, regionIds);
      partitionTableMap.put(new PartialPath(storageGroupName), table);
    }
  }

  public synchronized void clear() {
    if (partitionTableMap != null) {
      partitionTableMap.clear();
      partitionTableMap = null;
    }
  }

  public DataRegionId getDataRegionId(PartialPath storageGroup, PartialPath path) {
    if (!partitionTableMap.containsKey(storageGroup)) {
      return null;
    }
    LocalDataPartitionTable table = partitionTableMap.get(storageGroup);
    return table.getDataRegionId(path);
  }

  /**
   * Try to allocate a data region for the new time partition slot. This function will try to create
   * new data region to make expansion if the existing data regions meet some condition.
   *
   * @param storageGroup The path for the storage group.
   * @param path The full path for the series.
   * @return The data region id for the time partition slot.
   */
  public DataRegionId allocateDataRegionForNewSlot(PartialPath storageGroup, PartialPath path) {
    LocalDataPartitionTable table = partitionTableMap.get(storageGroup);
    return table.getDataRegionWithAutoExtension(path);
  }

  public List<DataRegionId> getDataRegionIdsByStorageGroup(PartialPath storageGroup) {
    if (partitionTableMap.containsKey(storageGroup)) {
      LocalDataPartitionTable partitionTable = partitionTableMap.get(storageGroup);
      return partitionTable.getAllDataRegionId();
    }
    return Collections.emptyList();
  }

  public synchronized void registerStorageGroup(PartialPath storageGroup) {
    if (partitionTableMap.containsKey(storageGroup)) {
      return;
    }
    partitionTableMap.put(storageGroup, new LocalDataPartitionTable(storageGroup.getFullPath()));
  }

  public synchronized void deleteStorageGroup(PartialPath storageGroup) {
    LocalDataPartitionTable partitionTable = partitionTableMap.remove(storageGroup);
    if (partitionTable != null) {
      partitionTable.clear();
    }
  }
}
