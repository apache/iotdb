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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AlignedDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ClosedFileScanHandleImpl implements IFileScanHandle {

  private final TsFileResource tsFileResource;
  private final Map<IDeviceID, List<List<Modification>>> modificationMap;

  public ClosedFileScanHandleImpl(
      TsFileResource tsFileResource, Map<IDeviceID, List<List<Modification>>> modificationMap) {
    this.tsFileResource = tsFileResource;
    this.modificationMap = modificationMap;
  }

  @Override
  public TsFileDeviceStartEndTimeIterator getDeviceStartEndTimeIterator() throws IOException {
    ITimeIndex timeIndex = tsFileResource.getTimeIndex();
    return timeIndex instanceof DeviceTimeIndex
        ? new TsFileDeviceStartEndTimeIterator((DeviceTimeIndex) timeIndex)
        : new TsFileDeviceStartEndTimeIterator(tsFileResource.buildDeviceTimeIndex());
  }

  @Override
  public boolean isDeviceTimeDeleted(IDeviceID deviceID, long timestamp) {
    return false;
  }

  @Override
  public Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException {
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(getFilePath(), true);
    TsFileDeviceIterator deviceIterator = tsFileReader.getAllDevicesIteratorWithIsAligned();

    List<AbstractDeviceChunkMetaData> deviceChunkMetaDataList = new LinkedList<>();
    while (deviceIterator.hasNext()) {
      Pair<IDeviceID, Boolean> deviceIDWithIsAligned = deviceIterator.next();
      if (!deviceIDWithIsAligned.right) {
        Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> metadata =
            tsFileReader.getTimeseriesMetadataOffsetByDevice(
                deviceIterator.getFirstMeasurementNodeOfCurrentDevice(),
                Collections.emptySet(),
                true);
        deviceChunkMetaDataList.add(new DeviceChunkMetaData(deviceIDWithIsAligned.left, metadata));
      } else {
        // isAligned
        List<AlignedChunkMetadata> alignedDeviceChunkMetaData =
            tsFileReader.getAlignedChunkMetadata(deviceIDWithIsAligned.left);
        deviceChunkMetaDataList.add(
            new AlignedDeviceChunkMetaData(deviceIDWithIsAligned.left, alignedDeviceChunkMetaData));
      }
    }
    return deviceChunkMetaDataList.iterator();
  }

  @Override
  public boolean isTimeSeriesTimeDeleted(String timeSeriesName, long timestamp) {
    return false;
  }

  @Override
  public boolean isClosed() {
    return tsFileResource.isClosed();
  }

  @Override
  public boolean isDeleted() {
    return tsFileResource.isDeleted();
  }

  @Override
  public String getFilePath() {
    return tsFileResource.getTsFilePath();
  }
}
