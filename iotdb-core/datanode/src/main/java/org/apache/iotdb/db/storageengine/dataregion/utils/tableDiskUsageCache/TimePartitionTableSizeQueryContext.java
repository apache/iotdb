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

package org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Map;

public class TimePartitionTableSizeQueryContext implements Accountable {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOf(TimePartitionTableSizeQueryContext.class);
  private final Map<String, Long> tableSizeResultMap;
  Map<TsFileID, Long> tsFileIDOffsetInValueFileMap;

  public TimePartitionTableSizeQueryContext(Map<String, Long> tableSizeResultMap) {
    this.tableSizeResultMap = tableSizeResultMap;
  }

  public void addCachedTsFileIDAndOffsetInValueFile(TsFileID tsFileID, long offset) {
    if (tsFileIDOffsetInValueFileMap == null) {
      tsFileIDOffsetInValueFileMap = new HashMap<>();
    }
    tsFileIDOffsetInValueFileMap.put(tsFileID, offset);
  }

  public void replaceCachedTsFileID(TsFileID originTsFileID, TsFileID newTsFileID) {
    if (tsFileIDOffsetInValueFileMap == null) {
      return;
    }
    Long offset = tsFileIDOffsetInValueFileMap.remove(originTsFileID);
    if (offset != null) {
      tsFileIDOffsetInValueFileMap.put(newTsFileID, offset);
    }
  }

  public void updateResult(String table, long size) {
    tableSizeResultMap.computeIfPresent(table, (k, v) -> v + size);
  }

  public Map<String, Long> getTableSizeResultMap() {
    return tableSizeResultMap;
  }

  public boolean hasCachedTsFileID(TsFileID tsFileID) {
    return tsFileIDOffsetInValueFileMap != null
        && tsFileIDOffsetInValueFileMap.containsKey(tsFileID);
  }

  public Long getCachedTsFileIdOffset(TsFileID tsFileID) {
    return tsFileIDOffsetInValueFileMap == null ? null : tsFileIDOffsetInValueFileMap.get(tsFileID);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            tableSizeResultMap,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY)
        + ramBytesUsedOfTsFileIDOffsetMap();
  }

  // tsFileIDOffsetInValueFileMap should be null af first
  public long ramBytesUsedOfTsFileIDOffsetMap() {
    if (tsFileIDOffsetInValueFileMap == null) {
      return 0;
    }
    return RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP
        + tsFileIDOffsetInValueFileMap.size()
            * (RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY
                + Long.BYTES
                + TsFileID.SHALLOW_SIZE);
  }
}
