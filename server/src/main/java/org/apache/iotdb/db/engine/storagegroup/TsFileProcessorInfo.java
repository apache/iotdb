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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;

public class TsFileProcessorInfo {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private long unsealedResourceMemCost;
  private long bytesMemCost;
  private long chunkMetadataMemCost;
  private long walMemCost;
  private long accumulatedMemCost = 0;

  public TsFileProcessorInfo() {
    unsealedResourceMemCost = 0;
    bytesMemCost = 0;
    chunkMetadataMemCost = 0;
    walMemCost = IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();
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

  public void resetUnsealedResourceMemCost() {
    unsealedResourceMemCost = 0;
  }

  public void resetChunkMetadataMemCost() {
    chunkMetadataMemCost = 0;
  }

  public void resetBytesMemCost(long cost) {
    bytesMemCost -= cost;
  }

  public long getTsFileProcessorMemCost() {
    return unsealedResourceMemCost + bytesMemCost + chunkMetadataMemCost + walMemCost;
  }

  public boolean checkIfNeedReportTsFileProcessorStatus(long cost) {
    this.accumulatedMemCost += cost;
    int tsFileProcessorNum = StorageEngine.getInstance().countTsFileProcessors();
    return tsFileProcessorNum != 0
        && accumulatedMemCost > config.getReserveMemSize() / tsFileProcessorNum;
  }
}
