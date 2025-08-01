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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.model;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl.DiskAlignedChunkHandleImpl;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.utils.SharedTimeDataBuffer;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;

public class AlignedChunkOffset extends AbstractChunkOffset {

  // Used by aligned device to share the same time buffer
  private final SharedTimeDataBuffer sharedTimeDataBuffer;

  public AlignedChunkOffset(
      long offSet,
      IDeviceID devicePath,
      String measurement,
      SharedTimeDataBuffer sharedTimeDataBuffer) {
    super(offSet, devicePath, measurement);
    this.sharedTimeDataBuffer = sharedTimeDataBuffer;
  }

  @Override
  public IChunkHandle generateChunkHandle(
      String filePath, TsFileID tsFileID, Statistics<? extends Serializable> statistics) {
    return new DiskAlignedChunkHandleImpl(
        getDeviceID(),
        getMeasurement(),
        filePath,
        tsFileID,
        true,
        getOffSet(),
        statistics,
        sharedTimeDataBuffer);
  }
}
