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

import java.util.TreeSet;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.SystemInfo;

public class StorageGroupInfo {

  private long storageGroupMemCostThreshold;

  private long unsealedResourceMemCost;
  private long bytesMemCost;
  private long chunkMetadataMemCost;
  private long walMemCost;
  
  private TreeSet<TsFileProcessor> reportedTsps = new TreeSet<>(
      (o1, o2) -> (int) (o2.getTsFileProcessorInfo().getTsFileProcessorMemCost() - o1
          .getTsFileProcessorInfo().getTsFileProcessorMemCost()));

  public StorageGroupInfo() {
    storageGroupMemCostThreshold = IoTDBDescriptor.getInstance().getConfig()
        .getStorageGroupMemBlockSize();
    unsealedResourceMemCost = 0;
    bytesMemCost = 0;
    chunkMetadataMemCost = 0;
    walMemCost = IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();
  }

  public void reportTsFileProcessorInfo(TsFileProcessor tsFileProcessor) {
    reportedTsps.add(tsFileProcessor);
  }

  public void addUnsealedResourceMemCost(long cost) {
    unsealedResourceMemCost += cost;
  }

  public void addChunkMetadataMemCost(long cost) {
    chunkMetadataMemCost += cost;
  }

  public void addBytesMemCost(long cost) {
    bytesMemCost += cost;
  }

  public void resetUnsealedResourceMemCost(long cost) {
    unsealedResourceMemCost -= cost;
  }

  public void resetChunkMetadataMemCost(long cost) {
    chunkMetadataMemCost -= cost;
  }

  public void resetBytesMemCost(long cost) {
    bytesMemCost -= cost;
  }

  public void resetWalMemCost(long cost) {
    walMemCost -= cost;
  }

  public boolean checkIfNeedToReportStatusToSystem(long delta) {
    return (getStorageGroupMemCost() + delta) >= storageGroupMemCostThreshold;
  }

  public long getStorageGroupMemCost() {
    return unsealedResourceMemCost + bytesMemCost + chunkMetadataMemCost + walMemCost;
  }

  public TsFileProcessor getLargestTsFileProcessor() {
    return reportedTsps.first();
  }

  public void closeTsFileProcessorAndReportToSystem(TsFileProcessor tsFileProcessor) {
    reportedTsps.remove(tsFileProcessor);
    SystemInfo.getInstance().resetStorageGroupInfoStatus(this);
  }
}
