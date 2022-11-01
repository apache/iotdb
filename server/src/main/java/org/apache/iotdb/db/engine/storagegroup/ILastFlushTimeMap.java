/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
public interface ILastFlushTimeMap {

  /**
   * String basic total, 40B
   *
   * <ul>
   *   <li>Object header: Mark Word + Classic Pointer, 12B
   *   <li>char[] reference 4B
   *   <li>hash code, 4B
   *   <li>padding 4B
   *   <li>char[] header + length 16B
   * </ul>
   */
  long STRING_BASE_SIZE = 40;

  long LONG_SIZE = 24;

  long HASHMAP_NODE_BASIC_SIZE = 14 + STRING_BASE_SIZE + LONG_SIZE;

  // region set
  void setMultiDeviceFlushedTime(long timePartitionId, Map<String, Long> flushedTimeMap);

  void setOneDeviceFlushedTime(long timePartitionId, String path, long time);

  void setMultiDeviceGlobalFlushedTime(Map<String, Long> globalFlushedTimeMap);

  void setOneDeviceGlobalFlushedTime(String path, long time);
  // endregion

  // region update

  void updateFlushedTime(long timePartitionId, String path, long time);

  void updateGlobalFlushedTime(String path, long time);

  void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time);
  // endregion

  // region ensure
  boolean checkAndCreateFlushedTimePartition(long timePartitionId);

  // endregion

  // region support upgrade methods
  void applyNewlyFlushedTimeToFlushedTime();

  boolean updateLatestFlushTime(long partitionId, Map<String, Long> updateMap);
  // endregion

  // region query
  long getFlushedTime(long timePartitionId, String path);

  long getGlobalFlushedTime(String path);
  // endregion

  // region clear
  void clearFlushedTime();

  void clearGlobalFlushedTime();
  // endregion

  void removePartition(long partitionId);

  long getMemSize(long partitionId);
}
