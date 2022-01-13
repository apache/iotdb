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
package org.apache.iotdb.db.engine.storagegroup;

import java.util.Map;

/** This interface manages last time and flush time for sequence and unsequence determination */
public interface ILastFlushTimeManager {

  // region set
  void setLastTimeAll(long timePartitionId, Map<String, Long> lastTimeMap);

  void setLastTime(long timePartitionId, String path, long time);

  void setFlushedTimeAll(long timePartitionId, Map<String, Long> flushedTimeMap);

  void setFlushedTime(long timePartitionId, String path, long time);

  void setGlobalFlushedTimeAll(Map<String, Long> globalFlushedTimeMap);

  void setGlobalFlushedTime(String path, long time);
  // endregion

  // region update
  void updateLastTime(long timePartitionId, String path, long time);

  void updateFlushedTime(long timePartitionId, String path, long time);

  void updateGlobalFlushedTime(String path, long time);

  void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time);
  // endregion

  // region ensure
  void ensureLastTimePartition(long timePartitionId);

  void ensureFlushedTimePartition(long timePartitionId);

  long ensureFlushedTimePartitionAndInit(long timePartitionId, String path, long initTime);
  // endregion

  // region support upgrade methods
  void applyNewlyFlushedTimeToFlushedTime();

  /**
   * update latest flush time for partition id
   *
   * @param partitionId partition id
   * @param latestFlushTime lastest flush time
   * @return true if update latest flush time success
   */
  boolean updateLatestFlushTimeToPartition(long partitionId, long latestFlushTime);

  boolean updateLatestFlushTime(long partitionId);
  // endregion

  // region query
  long getFlushedTime(long timePartitionId, String path);

  long getLastTime(long timePartitionId, String path);

  long getGlobalFlushedTime(String path);
  // endregion

  // region clear
  void clearLastTime();

  void clearFlushedTime();

  void clearGlobalFlushedTime();
  // endregion
}
