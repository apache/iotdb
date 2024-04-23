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

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Map;

/** This interface manages last time and flush time for sequence and unsequence determination */
public interface ILastFlushTimeMap {

  // region update
  /** Update partitionLatestFlushedTime. */
  void updateOneDeviceFlushedTime(long timePartitionId, IDeviceID deviceId, long time);

  void updateMultiDeviceFlushedTime(long timePartitionId, Map<IDeviceID, Long> flushedTimeMap);

  /** Update globalLatestFlushedTimeForEachDevice. */
  void updateOneDeviceGlobalFlushedTime(IDeviceID path, long time);

  void updateMultiDeviceGlobalFlushedTime(Map<IDeviceID, Long> globalFlushedTimeMap);

  /** Update both partitionLatestFlushedTime and globalLatestFlushedTimeForEachDevice. */
  void updateLatestFlushTime(long partitionId, Map<IDeviceID, Long> updateMap);
  // endregion

  // region ensure
  boolean checkAndCreateFlushedTimePartition(long timePartitionId);

  // endregion

  // region read
  long getFlushedTime(long timePartitionId, IDeviceID deviceId);

  long getGlobalFlushedTime(IDeviceID path);
  // endregion

  // region clear
  void clearFlushedTime();

  void clearGlobalFlushedTime();
  // endregion

  void degradeLastFlushTime(long partitionId);

  long getMemSize(long partitionId);
}
