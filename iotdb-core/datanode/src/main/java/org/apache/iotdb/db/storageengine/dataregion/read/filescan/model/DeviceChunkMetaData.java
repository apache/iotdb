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

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;

public class DeviceChunkMetaData extends AbstractDeviceChunkMetaData {

  private final List<IChunkMetadata> measurementChunkMetadata;
  private int index;

  public DeviceChunkMetaData(
      IDeviceID devicePath, List<IChunkMetadata> measurementChunkMetadataMap) {
    super(devicePath);
    this.measurementChunkMetadata = measurementChunkMetadataMap;
    this.index = -1;
  }

  @Override
  public boolean hasNextValueChunkMetadata() {
    return index < measurementChunkMetadata.size() - 1;
  }

  @Override
  public IChunkMetadata nextValueChunkMetadata() {
    index++;
    return measurementChunkMetadata.get(index);
  }

  @Override
  public AbstractChunkOffset getChunkOffset() {
    return new ChunkOffset(
        measurementChunkMetadata.get(index).getOffsetOfChunkHeader(),
        getDevicePath(),
        measurementChunkMetadata.get(index).getMeasurementUid());
  }
}
