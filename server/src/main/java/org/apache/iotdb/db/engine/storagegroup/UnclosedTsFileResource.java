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

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnclosedTsFileResource extends TsFileResource {

  /**
   * Chunk metadata list of unsealed tsfile. Only be set in a temporal TsFileResource in a query
   * process.
   */
  private Map<PartialPath, List<ChunkMetadata>> pathToChunkMetadataListMap;

  /** Mem chunk data. Only be set in a temporal TsFileResource in a query process. */
  private Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap;

  /** used for unsealed file to get TimeseriesMetadata */
  private Map<PartialPath, TimeseriesMetadata> pathToTimeSeriesMetadataMap;

  /**
   * If it is not null, it indicates that the current tsfile resource is a snapshot of the
   * originTsFileResource, and if so, when we want to used the lock, we should try to acquire the
   * lock of originTsFileResource
   */
  private TsFileResource originTsFileResource;

  private TsFileProcessor processor;

  /** unsealed TsFile, for writter */
  public UnclosedTsFileResource(
      File file, TsFileProcessor processor, int deviceNumInLastClosedTsFile) {
    this.file = file;
    this.version = FilePathUtils.splitAndGetTsFileVersion(this.file.getName());
    this.timeIndex = CONFIG.getTimeIndexLevel().getTimeIndex(deviceNumInLastClosedTsFile);
    this.timeIndexType = (byte) CONFIG.getTimeIndexLevel().ordinal();
    this.processor = processor;
  }

  /** unsealed TsFile, for query */
  public UnclosedTsFileResource(
      PartialPath path,
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<ChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.timeIndexType = originTsFileResource.timeIndexType;
    this.pathToReadOnlyMemChunkMap = new HashMap<>();
    pathToReadOnlyMemChunkMap.put(path, readOnlyMemChunk);
    this.pathToChunkMetadataListMap = new HashMap<>();
    pathToChunkMetadataListMap.put(path, chunkMetadataList);
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
    generatePathToTimeSeriesMetadataMap();
  }

  /** unsealed TsFile, for query */
  public UnclosedTsFileResource(
      Map<PartialPath, List<ReadOnlyMemChunk>> pathToReadOnlyMemChunkMap,
      Map<PartialPath, List<ChunkMetadata>> pathToChunkMetadataListMap,
      TsFileResource originTsFileResource)
      throws IOException {
    this.file = originTsFileResource.file;
    this.timeIndex = originTsFileResource.timeIndex;
    this.timeIndexType = originTsFileResource.timeIndexType;
    this.pathToReadOnlyMemChunkMap = pathToReadOnlyMemChunkMap;
    this.pathToChunkMetadataListMap = pathToChunkMetadataListMap;
    this.originTsFileResource = originTsFileResource;
    this.version = originTsFileResource.version;
    generatePathToTimeSeriesMetadataMap();
  }

  public List<ChunkMetadata> getChunkMetadataList(PartialPath seriesPath) {
    return new ArrayList<>(pathToChunkMetadataListMap.get(seriesPath));
  }

  public List<ReadOnlyMemChunk> getReadOnlyMemChunk(PartialPath seriesPath) {
    return pathToReadOnlyMemChunkMap.get(seriesPath);
  }

  public TimeseriesMetadata getTimeSeriesMetadataByPath(PartialPath seriesPath) {
    if (pathToTimeSeriesMetadataMap.containsKey(seriesPath)) {
      return pathToTimeSeriesMetadataMap.get(seriesPath);
    }
    return null;
  }

  private void generatePathToTimeSeriesMetadataMap() throws IOException {
    pathToTimeSeriesMetadataMap = new HashMap<>();
    for (PartialPath path : pathToChunkMetadataListMap.keySet()) {
      TimeseriesMetadata timeSeriesMetadata = new TimeseriesMetadata();
      timeSeriesMetadata.setOffsetOfChunkMetaDataList(-1);
      timeSeriesMetadata.setDataSizeOfChunkMetaDataList(-1);

      if (pathToChunkMetadataListMap.containsKey(path)
          && !pathToChunkMetadataListMap.get(path).isEmpty()) {
        timeSeriesMetadata.setMeasurementId(
            pathToChunkMetadataListMap.get(path).get(0).getMeasurementUid());
        TSDataType dataType = pathToChunkMetadataListMap.get(path).get(0).getDataType();
        timeSeriesMetadata.setTSDataType(dataType);
      } else if (pathToReadOnlyMemChunkMap.containsKey(path)
          && !pathToReadOnlyMemChunkMap.get(path).isEmpty()) {
        timeSeriesMetadata.setMeasurementId(
            pathToReadOnlyMemChunkMap.get(path).get(0).getMeasurementUid());
        TSDataType dataType = pathToReadOnlyMemChunkMap.get(path).get(0).getDataType();
        timeSeriesMetadata.setTSDataType(dataType);
      }
      if (timeSeriesMetadata.getTSDataType() != null) {
        Statistics<?> seriesStatistics =
            Statistics.getStatsByType(timeSeriesMetadata.getTSDataType());
        // flush chunkMetadataList one by one
        for (ChunkMetadata chunkMetadata : pathToChunkMetadataListMap.get(path)) {
          seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
        }

        for (ReadOnlyMemChunk memChunk : pathToReadOnlyMemChunkMap.get(path)) {
          if (!memChunk.isEmpty()) {
            seriesStatistics.mergeStatistics(memChunk.getChunkMetaData().getStatistics());
          }
        }
        timeSeriesMetadata.setStatistics(seriesStatistics);
      } else {
        timeSeriesMetadata = null;
      }
      pathToTimeSeriesMetadataMap.put(path, timeSeriesMetadata);
    }
  }

  public TsFileProcessor getProcessor() {
    return processor;
  }

  public void setProcessor(TsFileProcessor processor) {
    this.processor = processor;
  }

  /**
   * If originTsFileResource is not null, we should acquire the read lock of originTsFileResource
   * before construct the current TsFileResource
   */
  @Override
  public void writeLock() {
    if (originTsFileResource == null) {
      tsFileLock.writeLock();
    } else {
      originTsFileResource.writeLock();
    }
  }

  @Override
  public void writeUnlock() {
    if (originTsFileResource == null) {
      tsFileLock.writeUnlock();
    } else {
      originTsFileResource.writeUnlock();
    }
  }

  @Override
  public void readLock() {
    if (originTsFileResource == null) {
      tsFileLock.readLock();
    } else {
      originTsFileResource.readLock();
    }
  }

  @Override
  public void readUnlock() {
    if (originTsFileResource == null) {
      tsFileLock.readUnlock();
    } else {
      originTsFileResource.readUnlock();
    }
  }
}
