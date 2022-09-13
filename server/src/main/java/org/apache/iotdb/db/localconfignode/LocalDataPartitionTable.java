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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalDataPartitionTable {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDataPartitionTable.class);

  private String storageGroupName;
  private final int regionNum;
  private DataRegionId[] regionIds;

  public LocalDataPartitionTable(String storageGroupName, List<DataRegionId> regions) {
    this.storageGroupName = storageGroupName;
    this.regionNum = regions.size();
    regions.sort(Comparator.comparingInt(ConsensusGroupId::getId));
    this.regionIds = new DataRegionId[regions.size()];
    for (int i = 0; i < regions.size(); ++i) {
      regionIds[i] = regions.get(i);
      DataRegionIdGenerator.getInstance().setIfGreater(regionIds[i].getId());
    }
  }

  public LocalDataPartitionTable(String storageGroupName) {
    this.storageGroupName = storageGroupName;
    this.regionNum = IoTDBDescriptor.getInstance().getConfig().getDataRegionNum();
    this.regionIds = new DataRegionId[regionNum];
  }

  /**
   * Get the data region id which the path located in.
   *
   * @param path The full path for the series.
   * @return The region id for the path.
   */
  public DataRegionId getDataRegionId(PartialPath path) {
    int idx = Math.abs(path.hashCode() % regionNum);
    return regionIds[idx];
  }

  /**
   * Get all data region id of current storage group
   *
   * @return data region id in list
   */
  public List<DataRegionId> getAllDataRegionId() {
    return Arrays.asList(regionIds);
  }

  public DataRegionId getDataRegionWithAutoExtension(PartialPath path) {
    int idx = Math.abs(path.hashCode() % regionNum);
    if (regionIds[idx] == null) {
      int nextId = DataRegionIdGenerator.getInstance().getNextId();
      regionIds[idx] = new DataRegionId(nextId);
    }
    return regionIds[idx];
  }

  public void clear() {
    // TODO: clear the table
    regionIds = null;
  }

  public static class DataRegionIdGenerator {
    private static final DataRegionIdGenerator INSTANCE = new DataRegionIdGenerator();
    private final AtomicInteger idCounter = new AtomicInteger(0);

    public static DataRegionIdGenerator getInstance() {
      return INSTANCE;
    }

    public void setCurrentId(int id) {
      idCounter.set(id);
    }

    public int getNextId() {
      return idCounter.getAndIncrement();
    }

    /**
     * Update the id counter when recovering, make sure that after all data regions is recovered,
     * the id counter is greater than any existed region id
     */
    public void setIfGreater(int id) {
      int originVal = idCounter.get();
      while (originVal <= id && !idCounter.compareAndSet(originVal, id + 1)) {
        originVal = idCounter.get();
      }
    }

    @TestOnly
    public void reset() {
      this.idCounter.set(0);
    }
  }
}
