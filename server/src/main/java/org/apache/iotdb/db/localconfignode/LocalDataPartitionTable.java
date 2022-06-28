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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used for data partition maintaining the map between storage group and
// dataRegionIds.
public class LocalDataPartitionTable {

  private AtomicInteger dataRegionIdGenerator;

  private Map<PartialPath, List<DataRegionId>> table;

  private static class LocalDataPartitionTableHolder {
    private static final LocalDataPartitionTable INSTANCE = new LocalDataPartitionTable();

    private LocalDataPartitionTableHolder() {};
  }

  private LocalDataPartitionTable() {}

  public static LocalDataPartitionTable getInstance() {
    return LocalDataPartitionTableHolder.INSTANCE;
  }

  public synchronized void init(Map<String, List<DataRegionId>> recoveredLocalDataRegionInfo)
      throws IllegalPathException {
    table = new ConcurrentHashMap<>();
    dataRegionIdGenerator = new AtomicInteger(0);
    for (Map.Entry<String, List<DataRegionId>> entry : recoveredLocalDataRegionInfo.entrySet()) {
      String storageGroup = entry.getKey();
      List<DataRegionId> dataRegionIdList = new CopyOnWriteArrayList<>();
      table.put(new PartialPath(storageGroup), dataRegionIdList);
      for (DataRegionId dataRegionId : recoveredLocalDataRegionInfo.get(storageGroup)) {
        dataRegionIdList.add(dataRegionId);

        if (dataRegionId.getId() >= dataRegionIdGenerator.get()) {
          dataRegionIdGenerator.set(dataRegionId.getId() + 1);
        }
      }
    }
  }

  public synchronized void clear() {
    if (table != null) {
      table.clear();
      table = null;
    }

    if (dataRegionIdGenerator != null) {
      dataRegionIdGenerator = null;
    }
  }

  public synchronized void putDataRegionId(PartialPath storageGroup, DataRegionId dataRegionId) {
    table.get(storageGroup).add(dataRegionId);
  }

  public synchronized void removeDataRegionId(PartialPath storageGroup, DataRegionId dataRegionId) {
    table.get(storageGroup).remove(dataRegionId);
  }

  public DataRegionId getDataRegionId(PartialPath storageGroup, PartialPath path) {
    if (!table.containsKey(storageGroup)) {
      return null;
    }
    return table.get(storageGroup).get(0);
  }

  public List<DataRegionId> getInvolvedDataRegionIds(
      PartialPath storageGroup, PartialPath pathPattern, boolean isPrefixMatch) {
    List<DataRegionId> result = new ArrayList<>();
    if (table.containsKey(storageGroup)) {
      result.addAll(table.get(storageGroup));
    }
    return result;
  }

  public List<DataRegionId> getDataRegionIdsByStorageGroup(PartialPath storageGroup) {
    return table.getOrDefault(storageGroup, Collections.emptyList());
  }

  public synchronized void setDataPartitionInfo(PartialPath storageGroup) {
    List<DataRegionId> dataRegionIdList;
    if (table.containsKey(storageGroup)) {
      dataRegionIdList = table.get(storageGroup);
    } else {
      dataRegionIdList = new CopyOnWriteArrayList<>();
    }
    dataRegionIdList.add(generateDataRegionId());
    table.put(storageGroup, dataRegionIdList);
  }

  public synchronized List<DataRegionId> deleteStorageGroup(PartialPath storageGroup) {
    if (table.containsKey(storageGroup)) {
      return table.remove(storageGroup);
    }
    return Collections.emptyList();
  }

  // This method may be extended to implement multi dataRegion for one storageGroup
  // todo keep consistent with the partition method of config node in new cluster
  private DataRegionId generateDataRegionId() {
    return new DataRegionId(dataRegionIdGenerator.getAndIncrement());
  }
}
