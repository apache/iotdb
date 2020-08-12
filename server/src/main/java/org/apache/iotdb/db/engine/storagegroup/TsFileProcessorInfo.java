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

import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class TsFileProcessorInfo {

  private StorageGroupInfo storageGroupInfo;

  private long unsealedResourceMemCost;
  private long bytesMemCost;
  private long chunkMetadataMemCost;
  private long walMemCost;

  public TsFileProcessorInfo(StorageGroupInfo storageGroupInfo) {
    this.storageGroupInfo = storageGroupInfo;
    this.unsealedResourceMemCost = 0;
    this.bytesMemCost = 0;
    this.chunkMetadataMemCost = 0;
    this.walMemCost = IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();
  }

  public void addUnsealedResourceMemCost(long cost) {
    unsealedResourceMemCost += cost;
    storageGroupInfo.addUnsealedResourceMemCost(cost);
  }

  public void addChunkMetadataMemCost(long cost) {
    chunkMetadataMemCost += cost;
    storageGroupInfo.addChunkMetadataMemCost(cost);
  }

  public void addBytesMemCost(long cost) {
    bytesMemCost += cost;
    storageGroupInfo.addBytesMemCost(cost);
  }

  public void clear() {
    storageGroupInfo.resetUnsealedResourceMemCost(unsealedResourceMemCost);
    storageGroupInfo.resetChunkMetadataMemCost(chunkMetadataMemCost);
    storageGroupInfo.resetWalMemCost(walMemCost);
    walMemCost = 0;
    unsealedResourceMemCost = 0;
    chunkMetadataMemCost = 0;
  }

  public void resetBytesMemCost(long cost) {
    storageGroupInfo.resetBytesMemCost(cost);
    bytesMemCost -= cost;
  }

  public long getTsFileProcessorMemCost() {
    return unsealedResourceMemCost + bytesMemCost + chunkMetadataMemCost + walMemCost;
  }
}
