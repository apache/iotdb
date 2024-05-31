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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractChunkOffset;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.model.AbstractDeviceChunkMetaData;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileDeviceStartEndTimeIterator;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * IFileScanHandle will supply interfaces for metadata checking and chunk scanning for specified one
 * TsFile.
 */
public interface IFileScanHandle {

  /**
   * Get timeIndex for devices in current TsFile.
   *
   * @return the iterator of DeviceStartEndTime, which includes devicePath and startEndTime of this
   *     devicePath.
   */
  TsFileDeviceStartEndTimeIterator getDeviceStartEndTimeIterator() throws IOException;

  /**
   * Check whether timestamp is deleted in specified device.
   *
   * @param deviceID the devicePath needs to be checked.
   * @param timeStamp time value needed to be checked.
   * @return A boolean value, which indicates whether the timestamp is deleted.
   */
  boolean isDeviceTimeDeleted(IDeviceID deviceID, long timeStamp) throws IllegalPathException;

  /**
   * Get all the chunkMetaData in current TsFile. ChunkMetaData will be organized in device level.
   *
   * @return the iterator of DeviceChunkMetaData, which includes the devicePath, measurementId and
   *     relating chunkMetaDataList.
   */
  Iterator<AbstractDeviceChunkMetaData> getAllDeviceChunkMetaData() throws IOException;

  /**
   * Check whether timestamp in specified timeSeries is deleted.
   *
   * @param deviceID the devicePath needs to be checked.
   * @param timeSeriesName the timeSeries needs to be checked.
   * @param timeStamp time value needed to be checked
   * @return A boolean value, which indicates whether the timestamp is deleted.
   */
  boolean isTimeSeriesTimeDeleted(IDeviceID deviceID, String timeSeriesName, long timeStamp)
      throws IllegalPathException;

  /**
   * Get the chunkHandles of chunks needed to be scanned. ChunkHandles are used to read chunk.
   *
   * @param chunkInfoList the list of ChunkOffset, which decides the chunk needed to be scanned.
   * @param statisticsList the list of Statistics, which will be used when there is only one page in
   *     chunk.
   * @return the iterator of IChunkHandle.
   */
  Iterator<IChunkHandle> getChunkHandles(
      List<AbstractChunkOffset> chunkInfoList,
      List<Statistics<? extends Serializable>> statisticsList,
      List<Integer> orderedList)
      throws IOException;

  /** If the TsFile of this handle is closed. */
  boolean isClosed();

  /** If the TsFile of this handle is deleted. */
  boolean isDeleted();

  /** Get TsFileResource of current TsFile. */
  TsFileResource getTsResource();
}
