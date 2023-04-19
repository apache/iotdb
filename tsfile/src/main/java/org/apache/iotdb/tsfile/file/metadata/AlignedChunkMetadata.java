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
package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class AlignedChunkMetadata implements IChunkMetadata {

  // ChunkMetadata for time column
  private final IChunkMetadata timeChunkMetadata;
  // ChunkMetadata for all subSensors in the vector
  private final List<IChunkMetadata> valueChunkMetadataList;

  /** ChunkLoader of metadata, used to create IChunkReader */
  private IChunkLoader chunkLoader;

  public AlignedChunkMetadata(
      IChunkMetadata timeChunkMetadata, List<IChunkMetadata> valueChunkMetadataList) {
    this.timeChunkMetadata = timeChunkMetadata;
    this.valueChunkMetadataList = valueChunkMetadataList;
  }

  @Override
  public Statistics getStatistics() {
    return valueChunkMetadataList.size() == 1 && valueChunkMetadataList.get(0) != null
        ? valueChunkMetadataList.get(0).getStatistics()
        : timeChunkMetadata.getStatistics();
  }

  public Statistics getStatistics(int index) {
    IChunkMetadata v = valueChunkMetadataList.get(index);
    return v == null ? null : v.getStatistics();
  }

  public List<Statistics> getValueStatisticsList() {
    List<Statistics> valueStatisticsList = new ArrayList<>();
    for (IChunkMetadata v : valueChunkMetadataList) {
      valueStatisticsList.add(v == null ? null : v.getStatistics());
    }
    return valueStatisticsList;
  }

  public Statistics getTimeStatistics() {
    return timeChunkMetadata.getStatistics();
  }

  @Override
  public boolean isModified() {
    return timeChunkMetadata.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    timeChunkMetadata.setModified(modified);
    for (IChunkMetadata v : valueChunkMetadataList) {
      if (v != null) {
        v.setModified(modified);
      }
    }
  }

  @Override
  public boolean isSeq() {
    return timeChunkMetadata.isSeq();
  }

  @Override
  public void setSeq(boolean seq) {
    timeChunkMetadata.setSeq(seq);
    for (IChunkMetadata v : valueChunkMetadataList) {
      if (v != null) {
        v.setSeq(seq);
      }
    }
  }

  @Override
  public long getVersion() {
    return timeChunkMetadata.getVersion();
  }

  @Override
  public void setVersion(long version) {
    timeChunkMetadata.setVersion(version);
    for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
      if (valueChunkMetadata != null) {
        valueChunkMetadata.setVersion(version);
      }
    }
  }

  @Override
  public long getOffsetOfChunkHeader() {
    return timeChunkMetadata.getOffsetOfChunkHeader();
  }

  @Override
  public long getStartTime() {
    return timeChunkMetadata.getStartTime();
  }

  @Override
  public long getEndTime() {
    return timeChunkMetadata.getEndTime();
  }

  @Override
  public boolean isFromOldTsFile() {
    return false;
  }

  @Override
  public IChunkLoader getChunkLoader() {
    return chunkLoader;
  }

  @Override
  public boolean needSetChunkLoader() {
    return chunkLoader == null;
  }

  @Override
  public void setChunkLoader(IChunkLoader chunkLoader) {
    this.chunkLoader = chunkLoader;
  }

  @Override
  public void setFilePath(String filePath) {
    timeChunkMetadata.setFilePath(filePath);
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      if (chunkMetadata != null) {
        chunkMetadata.setFilePath(filePath);
      }
    }
  }

  @Override
  public void setClosed(boolean closed) {
    timeChunkMetadata.setClosed(closed);
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      if (chunkMetadata != null) {
        chunkMetadata.setClosed(closed);
      }
    }
  }

  @Override
  public TSDataType getDataType() {
    return timeChunkMetadata.getDataType();
  }

  @Override
  public String getMeasurementUid() {
    return timeChunkMetadata.getMeasurementUid();
  }

  @Override
  public void insertIntoSortedDeletions(TimeRange timeRange) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TimeRange> getDeleteIntervalList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int serializeTo(OutputStream outputStream, boolean serializeStatistic) {
    throw new UnsupportedOperationException("VectorChunkMetadata doesn't support serial method");
  }

  @Override
  public byte getMask() {
    return 0;
  }

  public IChunkMetadata getTimeChunkMetadata() {
    return timeChunkMetadata;
  }

  public List<IChunkMetadata> getValueChunkMetadataList() {
    return valueChunkMetadataList;
  }
}
