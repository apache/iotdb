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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import java.util.Iterator;
import java.util.Optional;

public class StorageEngineTimePartitionIterator {
  private final Iterator<DataRegion> dataRegionIterator;
  private final Optional<DataRegionFilterFunc> dataRegionFilter;
  private final Optional<TimePartitionFilterFunc> timePartitionFilter;
  private DataRegion currentDataRegion;
  private long currentTimePartition;
  private Iterator<Long> timePartitionIterator;

  public StorageEngineTimePartitionIterator(
      Optional<DataRegionFilterFunc> dataRegionFilter,
      Optional<TimePartitionFilterFunc> timePartitionFilter) {
    this.dataRegionIterator = StorageEngine.getInstance().getAllDataRegions().iterator();
    this.dataRegionFilter = dataRegionFilter;
    this.timePartitionFilter = timePartitionFilter;
  }

  public boolean next() throws Exception {
    while (true) {
      if (timePartitionIterator != null && timePartitionIterator.hasNext()) {
        currentTimePartition = timePartitionIterator.next();
        if (timePartitionFilter.isPresent()
            && !timePartitionFilter.get().apply(currentDataRegion, currentTimePartition)) {
          continue;
        }
        return true;
      } else if (!nextDataRegion()) {
        return false;
      } // should not have else branch
    }
  }

  private boolean nextDataRegion() throws Exception {
    while (dataRegionIterator.hasNext()) {
      currentDataRegion = dataRegionIterator.next();
      if (currentDataRegion == null || currentDataRegion.isDeleted()) {
        continue;
      }
      if (dataRegionFilter.isPresent() && !dataRegionFilter.get().apply(currentDataRegion)) {
        continue;
      }
      timePartitionIterator = currentDataRegion.getTimePartitions().iterator();
      if (timePartitionIterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  public DataRegion currentDataRegion() {
    return currentDataRegion;
  }

  public long currentTimePartition() {
    return currentTimePartition;
  }

  @FunctionalInterface
  public interface DataRegionFilterFunc {
    boolean apply(DataRegion currentDataRegion) throws Exception;
  }

  @FunctionalInterface
  public interface TimePartitionFilterFunc {
    boolean apply(DataRegion currentDataRegion, long currentTimePartition) throws Exception;
  }
}
