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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DiskUsageStatisticUtil implements Closeable {

  protected static final Logger logger = LoggerFactory.getLogger(DiskUsageStatisticUtil.class);
  protected List<TsFileResource> resourcesWithReadLock;
  protected final Iterator<TsFileResource> iterator;

  public DiskUsageStatisticUtil(TsFileManager tsFileManager, long timePartition) {
    List<TsFileResource> seqResources = tsFileManager.getTsFileListSnapshot(timePartition, true);
    List<TsFileResource> unseqResources = tsFileManager.getTsFileListSnapshot(timePartition, false);
    List<TsFileResource> resources =
        Stream.concat(seqResources.stream(), unseqResources.stream()).collect(Collectors.toList());
    acquireReadLocks(resources);
    iterator = resourcesWithReadLock.iterator();
  }

  public boolean hasNextFile() {
    return iterator.hasNext();
  }

  public abstract long[] getResult();

  protected void acquireReadLocks(List<TsFileResource> resources) {
    this.resourcesWithReadLock = new ArrayList<>(resources.size());
    try {
      for (TsFileResource resource : resources) {
        resource.readLock();
        if (resource.isDeleted() || !resource.isClosed()) {
          resource.readUnlock();
          continue;
        }
        resourcesWithReadLock.add(resource);
      }
    } catch (Exception e) {
      releaseReadLocks();
      throw e;
    }
  }

  protected void releaseReadLocks() {
    if (resourcesWithReadLock == null) {
      return;
    }
    for (TsFileResource resource : resourcesWithReadLock) {
      resource.readUnlock();
    }
    resourcesWithReadLock = null;
  }

  public abstract void calculateNextFile();

  protected long calculateStartOffsetOfChunkGroup(
      TsFileSequenceReader reader,
      MetadataIndexNode firstMeasurementNodeOfCurrentDevice,
      Pair<IDeviceID, Boolean> deviceIsAlignedPair)
      throws IOException {
    int chunkGroupHeaderSize =
        new ChunkGroupHeader(deviceIsAlignedPair.getLeft()).getSerializedSize();
    if (deviceIsAlignedPair.getRight()) {
      List<TimeseriesMetadata> timeColumnTimeseriesMetadata = new ArrayList<>(1);
      reader.readITimeseriesMetadata(
          timeColumnTimeseriesMetadata, firstMeasurementNodeOfCurrentDevice, "");
      IChunkMetadata iChunkMetadata =
          timeColumnTimeseriesMetadata.get(0).getChunkMetadataList().get(0);
      return iChunkMetadata.getOffsetOfChunkHeader() - chunkGroupHeaderSize;
    } else {
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      reader.getDeviceTimeseriesMetadata(
          timeseriesMetadataList,
          firstMeasurementNodeOfCurrentDevice,
          Collections.emptySet(),
          true);
      long minOffset = Long.MAX_VALUE;
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          minOffset = Math.min(minOffset, chunkMetadata.getOffsetOfChunkHeader());
          break;
        }
      }
      return minOffset - chunkGroupHeaderSize;
    }
  }

  @Override
  public void close() throws IOException {
    releaseReadLocks();
  }
}
