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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataRegionTableSizeQueryContext implements Accountable {

  private final boolean needAllData;

  private final Map<Long, TimePartitionTableSizeQueryContext>
      timePartitionTableSizeQueryContextMap = new LinkedHashMap<>();
  private int objectFileNum = 0;

  private long previousUsedTimePartition;
  private TimePartitionTableSizeQueryContext previousUsedTimePartitionContext;

  public DataRegionTableSizeQueryContext(boolean needAllData) {
    this.needAllData = needAllData;
  }

  public Map<Long, TimePartitionTableSizeQueryContext> getTimePartitionTableSizeQueryContextMap() {
    return timePartitionTableSizeQueryContextMap;
  }

  public boolean isEmpty() {
    return timePartitionTableSizeQueryContextMap.isEmpty();
  }

  public int getObjectFileNum() {
    return objectFileNum;
  }

  public long getObjectFileSize() {
    long totalSize = 0;
    for (TimePartitionTableSizeQueryContext timePartitionContext :
        timePartitionTableSizeQueryContextMap.values()) {
      totalSize += timePartitionContext.getObjectFileSize();
    }
    return totalSize;
  }

  public void addCachedTsFileIDAndOffsetInValueFile(TsFileID tsFileID, long offset) {
    switchTimePartition(tsFileID.timePartitionId);
    previousUsedTimePartitionContext.addCachedTsFileIDAndOffsetInValueFile(tsFileID, offset);
  }

  public void replaceCachedTsFileID(TsFileID originTsFileID, TsFileID newTsFileID) {
    switchTimePartition(originTsFileID.timePartitionId);
    previousUsedTimePartitionContext.replaceCachedTsFileID(originTsFileID, newTsFileID);
  }

  public void updateResult(String table, long size, long currentTimePartition) {
    switchTimePartition(currentTimePartition);
    previousUsedTimePartitionContext.updateResult(table, size, needAllData);
  }

  private void switchTimePartition(long currentTimePartition) {
    if (currentTimePartition != previousUsedTimePartition
        || previousUsedTimePartitionContext == null) {
      TimePartitionTableSizeQueryContext currentTimePartitionContext =
          timePartitionTableSizeQueryContextMap.compute(
              currentTimePartition,
              (k, v) ->
                  (v == null && needAllData)
                      ? new TimePartitionTableSizeQueryContext(new HashMap<>())
                      : v);
      if (currentTimePartitionContext == null) {
        return;
      }
      previousUsedTimePartition = currentTimePartition;
      previousUsedTimePartitionContext = currentTimePartitionContext;
    }
  }

  public void addTimePartition(
      long timePartition, TimePartitionTableSizeQueryContext timePartitionTableSizeQueryContext) {
    timePartitionTableSizeQueryContextMap.put(timePartition, timePartitionTableSizeQueryContext);
  }

  public void updateObjectFileNum(int delta) {
    this.objectFileNum += delta;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
