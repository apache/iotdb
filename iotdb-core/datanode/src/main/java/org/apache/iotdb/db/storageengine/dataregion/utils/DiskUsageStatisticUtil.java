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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DiskUsageStatisticUtil implements Closeable {

  protected static final Logger logger = LoggerFactory.getLogger(DiskUsageStatisticUtil.class);
  protected Queue<TsFileResource> resourcesWithReadLock;
  protected final Iterator<TsFileResource> iterator;
  protected final LongConsumer timeSeriesMetadataIoSizeRecorder;

  public DiskUsageStatisticUtil(
      TsFileManager tsFileManager,
      long timePartition,
      Optional<FragmentInstanceContext> operatorContext) {
    this.timeSeriesMetadataIoSizeRecorder =
        operatorContext
            .<LongConsumer>map(
                context ->
                    context.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet)
            .orElse(null);
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
    this.resourcesWithReadLock = new LinkedList<>();
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

  public void calculateNextFile() {
    TsFileResource tsFileResource = iterator.next();
    try {
      if (tsFileResource.isDeleted()) {
        return;
      }
      TsFileSequenceReader reader =
          FileReaderManager.getInstance()
              .get(
                  tsFileResource.getTsFilePath(),
                  tsFileResource.getTsFileID(),
                  true,
                  timeSeriesMetadataIoSizeRecorder);
      calculateNextFile(tsFileResource, reader);
    } catch (Exception e) {
      logger.error("Failed to scan file {}", tsFileResource.getTsFile().getAbsolutePath(), e);
    } finally {
      // this operation including readUnlock
      FileReaderManager.getInstance().decreaseFileReaderReference(tsFileResource, true);
      iterator.remove();
    }
  }

  protected abstract void calculateNextFile(
      TsFileResource tsFileResource, TsFileSequenceReader reader)
      throws IOException, IllegalPathException;

  protected long calculateStartOffsetOfChunkGroup(
      TsFileSequenceReader reader,
      MetadataIndexNode firstMeasurementNodeOfCurrentDevice,
      Pair<IDeviceID, Boolean> deviceIsAlignedPair)
      throws IOException {
    int chunkGroupHeaderSize =
        new ChunkGroupHeader(deviceIsAlignedPair.getLeft()).getSerializedSize();
    if (deviceIsAlignedPair.getRight()) {
      TimeseriesMetadata timeColumnTimeseriesMetadata =
          reader.getTimeColumnMetadata(
              firstMeasurementNodeOfCurrentDevice, timeSeriesMetadataIoSizeRecorder);
      IChunkMetadata iChunkMetadata = timeColumnTimeseriesMetadata.getChunkMetadataList().get(0);
      return iChunkMetadata.getOffsetOfChunkHeader() - chunkGroupHeaderSize;
    } else {
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      reader.getDeviceTimeseriesMetadata(
          timeseriesMetadataList,
          firstMeasurementNodeOfCurrentDevice,
          Collections.emptySet(),
          true,
          timeSeriesMetadataIoSizeRecorder);
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
