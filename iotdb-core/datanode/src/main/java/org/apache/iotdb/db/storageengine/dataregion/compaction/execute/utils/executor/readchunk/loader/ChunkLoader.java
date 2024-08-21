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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;

import java.io.IOException;
import java.util.List;

public abstract class ChunkLoader {

  protected String file;
  protected ChunkMetadata chunkMetadata;
  protected ModifiedStatus modifiedStatus;

  protected ChunkLoader(String file, ChunkMetadata chunkMetadata) {
    this.file = file;
    this.chunkMetadata = chunkMetadata;
    calculateModifiedStatus();
  }

  protected ChunkLoader() {}

  public String getFile() {
    return file;
  }

  private void calculateModifiedStatus() {
    this.modifiedStatus = ModifiedStatus.NONE_DELETED;
    if (chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0) {
      return;
    }
    ChunkMetadata chunkMetadata = getChunkMetadata();
    List<TimeRange> deleteIntervalList = chunkMetadata.getDeleteIntervalList();
    if (deleteIntervalList == null || deleteIntervalList.isEmpty()) {
      return;
    }
    long startTime = chunkMetadata.getStartTime();
    long endTime = chunkMetadata.getEndTime();
    TimeRange chunkTimeRange = new TimeRange(startTime, endTime);
    for (TimeRange timeRange : deleteIntervalList) {
      if (timeRange.contains(chunkTimeRange)) {
        this.modifiedStatus = ModifiedStatus.ALL_DELETED;
        break;
      } else if (timeRange.overlaps(chunkTimeRange)) {
        this.modifiedStatus = ModifiedStatus.PARTIAL_DELETED;
      }
    }
  }

  protected ModifiedStatus calculatePageModifiedStatus(PageHeader pageHeader) {
    if (this.modifiedStatus != ModifiedStatus.PARTIAL_DELETED) {
      return this.modifiedStatus;
    }
    ModifiedStatus pageModifiedStatus = ModifiedStatus.NONE_DELETED;
    if (pageHeader.getStatistics() == null || pageHeader.getStatistics().getCount() == 0) {
      return pageModifiedStatus;
    }
    List<TimeRange> deleteIntervalList = chunkMetadata.getDeleteIntervalList();
    long startTime = pageHeader.getStartTime();
    long endTime = pageHeader.getEndTime();
    TimeRange pageTimeRange = new TimeRange(startTime, endTime);
    for (TimeRange timeRange : deleteIntervalList) {
      if (timeRange.contains(pageTimeRange)) {
        pageModifiedStatus = ModifiedStatus.ALL_DELETED;
        break;
      } else if (timeRange.overlaps(pageTimeRange)) {
        pageModifiedStatus = ModifiedStatus.PARTIAL_DELETED;
      }
    }
    return pageModifiedStatus;
  }

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  public ModifiedStatus getModifiedStatus() {
    return modifiedStatus;
  }

  public abstract Chunk getChunk() throws IOException;

  public abstract boolean isEmpty();

  public abstract ChunkHeader getHeader() throws IOException;

  public abstract List<PageLoader> getPages() throws IOException;

  public abstract void clear();
}
