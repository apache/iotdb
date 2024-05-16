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

import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IFileScanHandle;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AlignedDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.ChunkOffsetInfo;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.DeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.DeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ITimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClosedFileScanHandleImpl implements IFileScanHandle {

  private final TsFileResource tsFileResource;
  private final Map<IDeviceID, Map<String, List<Modification>>> deviceToModifications;

  public ClosedFileScanHandleImpl(
      TsFileResource tsFileResource,
      Map<IDeviceID, Map<String, List<Modification>>> deviceToModifications) {
    this.tsFileResource = tsFileResource;
    this.deviceToModifications = deviceToModifications;
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
    for (List<Modification> modificationList : deviceToModifications.get(deviceID).values()) {
      if (modificationList.stream()
          .anyMatch(
              modification ->
                  modification instanceof Deletion
                      && ((Deletion) modification).getStartTime() <= timestamp
                      && ((Deletion) modification).getEndTime() >= timestamp)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isTimeSeriesTimeDeleted(
      IDeviceID deviceID, String timeSeriesName, long timestamp) {
    return deviceToModifications.get(deviceID).get(timeSeriesName).stream()
        .anyMatch(
            modification ->
                modification instanceof Deletion
                    && ((Deletion) modification).getStartTime() <= timestamp
                    && ((Deletion) modification).getEndTime() >= timestamp);
  }

  @Override
  public Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException {

    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(getFilePath(), true);
    TsFileDeviceIterator deviceIterator = tsFileReader.getAllDevicesIteratorWithIsAligned();

    List<AbstractDeviceChunkMetaData> deviceChunkMetaDataList = new LinkedList<>();
    // Traverse each device in current tsFile and get all the relating chunkMetaData
    while (deviceIterator.hasNext()) {
      Pair<IDeviceID, Boolean> deviceIDWithIsAligned = deviceIterator.next();
      if (!deviceIDWithIsAligned.right) {
        // device is not aligned
        Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> metadataForDevice =
            tsFileReader.getTimeseriesMetadataOffsetByDevice(
                deviceIterator.getFirstMeasurementNodeOfCurrentDevice(),
                Collections.emptySet(),
                true);
        deviceChunkMetaDataList.add(
            new DeviceChunkMetaData(
                deviceIDWithIsAligned.left,
                metadataForDevice.values().stream()
                    .flatMap(pair -> pair.getLeft().stream())
                    .collect(Collectors.toList())));
      } else {
        // device is aligned
        List<AlignedChunkMetadata> alignedDeviceChunkMetaData =
            tsFileReader.getAlignedChunkMetadata(deviceIDWithIsAligned.left);
        deviceChunkMetaDataList.add(
            new AlignedDeviceChunkMetaData(deviceIDWithIsAligned.left, alignedDeviceChunkMetaData));
      }
    }
    return deviceChunkMetaDataList.iterator();
  }

  @Override
  public Iterator<IChunkHandle> getChunkHandles(
      List<ChunkOffsetInfo> chunkInfoList, List<Statistics<? extends Serializable>> statisticsList)
      throws IOException {
    TsFileSequenceReader reader =
        FileReaderManager.getInstance().get(tsFileResource.getTsFilePath(), true);
    List<IChunkHandle> chunkHandleList = new ArrayList<>();
    for (int i = 0; i < chunkInfoList.size(); i++) {
      ChunkOffsetInfo chunkOffset = chunkInfoList.get(i);
      chunkHandleList.add(
          chunkOffset.isAligned()
              ? new DiskChunkHandleImpl(reader, chunkOffset.getOffSet(), statisticsList.get(i))
              : new DiskAlignedChunkHandleImpl(
                  reader,
                  chunkOffset.getOffSet(),
                  statisticsList.get(i),
                  chunkOffset.getSharedTimeDataBuffer()));
    }
    return chunkHandleList.iterator();
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

  @Override
  public TsFileResource getTsResource() {
    return tsFileResource;
  }
}
