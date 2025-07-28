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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.model;

import org.apache.iotdb.db.storageengine.dataregion.utils.SharedTimeDataBuffer;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;

public class AlignedDeviceChunkMetaData extends AbstractDeviceChunkMetaData {

  private int alignedChunkMetadataIndex;
  private int valueChunkMetadataIndex;
  private int curValueSize;
  private final List<AlignedChunkMetadata> alignedChunkMetadataList;

  public AlignedDeviceChunkMetaData(
      IDeviceID devicePath, List<AlignedChunkMetadata> alignedChunkMetadataList) {
    super(devicePath);
    this.alignedChunkMetadataList = alignedChunkMetadataList;
    this.alignedChunkMetadataIndex = 0;
    this.curValueSize = alignedChunkMetadataList.get(0).getValueChunkMetadataList().size();
    this.valueChunkMetadataIndex = -1;
  }

  @Override
  public boolean hasNextValueChunkMetadata() {
    return alignedChunkMetadataIndex < alignedChunkMetadataList.size() - 1
        || valueChunkMetadataIndex < curValueSize - 1;
  }

  @Override
  public IChunkMetadata nextValueChunkMetadata() {
    if (valueChunkMetadataIndex < curValueSize - 1) {
      valueChunkMetadataIndex++;
    } else {
      alignedChunkMetadataIndex++;
      valueChunkMetadataIndex = 0;
      curValueSize =
          alignedChunkMetadataList
              .get(alignedChunkMetadataIndex)
              .getValueChunkMetadataList()
              .size();
    }
    return alignedChunkMetadataList
        .get(alignedChunkMetadataIndex)
        .getValueChunkMetadataList()
        .get(valueChunkMetadataIndex);
  }

  @Override
  public AbstractChunkOffset getChunkOffset() {
    AlignedChunkMetadata alignedChunkMetadata =
        alignedChunkMetadataList.get(alignedChunkMetadataIndex);
    IChunkMetadata valueChunkMetaData =
        alignedChunkMetadata.getValueChunkMetadataList().get(valueChunkMetadataIndex);
    return new AlignedChunkOffset(
        valueChunkMetaData.getOffsetOfChunkHeader(),
        getDevicePath(),
        valueChunkMetaData.getMeasurementUid(),
        new SharedTimeDataBuffer(alignedChunkMetadata.getTimeChunkMetadata()));
  }
}
