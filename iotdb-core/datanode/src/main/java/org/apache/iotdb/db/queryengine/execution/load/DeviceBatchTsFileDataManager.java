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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import java.io.File;

/** Like TsFileDataManager, but one batch (TsFilePieceNode) belongs to the same device. */
public class DeviceBatchTsFileDataManager extends TsFileDataManager {

  private String currentDeviceId;

  public DeviceBatchTsFileDataManager(
      DispatchFunction dispatchFunction,
      PlanNodeId planNodeId,
      File targetFile,
      DataPartitionBatchFetcher partitionBatchFetcher,
      long maxMemorySize,
      String userName) {
    super(dispatchFunction, planNodeId, targetFile, partitionBatchFetcher, maxMemorySize, userName);
  }

  @Override
  protected boolean addOrSendChunkData(ChunkData chunkData) {
    if (currentDeviceId != null && !currentDeviceId.equals(chunkData.getDevice())) {
      // a new device, flush previous data first
      boolean flushSucceed = flushChunkData(true);
      if (!flushSucceed) {
        return false;
      }
    }
    // add the chunk into the batch
    currentDeviceId = chunkData.getDevice();
    nonDirectionalChunkData.add(chunkData);
    dataSize += chunkData.getDataSize();
    if (dataSize > maxMemorySize) {
      return flushChunkData(false);
    }

    return true;
  }
}
