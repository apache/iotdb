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

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class DiskUsageStatisticUtil implements Closeable {

  protected static final Logger logger = LoggerFactory.getLogger(DiskUsageStatisticUtil.class);
  protected Queue<TsFileResource> resourcesWithReadLock;
  protected final long timePartition;
  protected final Iterator<TsFileResource> iterator;
  protected final LongConsumer timeSeriesMetadataIoSizeRecorder;
  protected final LongConsumer timeSeriesMetadataCountRecorder;

  public DiskUsageStatisticUtil(
      TsFileManager tsFileManager,
      long timePartition,
      Optional<FragmentInstanceContext> fragmentInstanceContext) {
    this.timePartition = timePartition;
    this.timeSeriesMetadataIoSizeRecorder =
        fragmentInstanceContext
            .<LongConsumer>map(
                context ->
                    context.getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize()::addAndGet)
            .orElse(null);
    this.timeSeriesMetadataCountRecorder =
        fragmentInstanceContext
            .<LongConsumer>map(
                context ->
                    context.getQueryStatistics().getLoadTimeSeriesMetadataFromDiskCount()
                        ::addAndGet)
            .orElse(null);
    List<TsFileResource> seqResources = tsFileManager.getTsFileListSnapshot(timePartition, true);
    List<TsFileResource> unseqResources = tsFileManager.getTsFileListSnapshot(timePartition, false);
    List<TsFileResource> resources =
        Stream.concat(seqResources.stream(), unseqResources.stream()).collect(Collectors.toList());
    acquireReadLocks(resources);
    iterator = resourcesWithReadLock.iterator();
  }

  public long getTimePartition() {
    return timePartition;
  }

  public boolean hasNextFile() {
    return iterator.hasNext();
  }

  protected void acquireReadLocks(List<TsFileResource> resources) {
    this.resourcesWithReadLock = new LinkedList<>();
    try {
      for (TsFileResource resource : resources) {
        if (!resource.isClosed()) {
          continue;
        }
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
    if (tsFileResource.isDeleted() || calculateWithoutOpenFile(tsFileResource)) {
      iterator.remove();
      tsFileResource.readUnlock();
      return;
    }
    FileReaderManager.getInstance().increaseFileReaderReference(tsFileResource, true);
    try {
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

  protected abstract boolean calculateWithoutOpenFile(TsFileResource tsFileResource);

  protected abstract void calculateNextFile(
      TsFileResource tsFileResource, TsFileSequenceReader reader)
      throws IOException, IllegalPathException;

  protected Offsets calculateStartOffsetOfChunkGroupAndTimeseriesMetadata(
      TsFileSequenceReader reader,
      MetadataIndexNode firstMeasurementNodeOfCurrentDevice,
      Pair<IDeviceID, Boolean> deviceIsAlignedPair,
      long rootMeasurementNodeStartOffset)
      throws IOException {
    int chunkGroupHeaderSize =
        new ChunkGroupHeader(deviceIsAlignedPair.getLeft()).getSerializedSize();
    if (deviceIsAlignedPair.getRight()) {
      Pair<Long, Long> timeseriesMetadataOffsetPair =
          getTimeColumnMetadataOffset(reader, firstMeasurementNodeOfCurrentDevice);
      IChunkMetadata firstChunkMetadata =
          reader
              .getChunkMetadataListByTimeseriesMetadataOffset(
                  timeseriesMetadataOffsetPair.getLeft(), timeseriesMetadataOffsetPair.getRight())
              .get(0);
      if (timeSeriesMetadataCountRecorder != null) {
        timeSeriesMetadataIoSizeRecorder.accept(
            timeseriesMetadataOffsetPair.getRight() - timeseriesMetadataOffsetPair.getLeft());
        timeSeriesMetadataCountRecorder.accept(1);
      }
      return new Offsets(
          firstChunkMetadata.getOffsetOfChunkHeader() - chunkGroupHeaderSize,
          timeseriesMetadataOffsetPair.getLeft(),
          rootMeasurementNodeStartOffset);
    } else {
      Map<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> timeseriesMetadataOffsetByDevice =
          reader.getTimeseriesMetadataOffsetByDevice(
              firstMeasurementNodeOfCurrentDevice, Collections.emptySet(), true);
      long minTimeseriesMetadataOffset = 0;
      long minChunkOffset = Long.MAX_VALUE;
      for (Map.Entry<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> entry :
          timeseriesMetadataOffsetByDevice.entrySet()) {
        minTimeseriesMetadataOffset =
            minTimeseriesMetadataOffset == 0
                ? entry.getValue().getRight().getLeft()
                : minTimeseriesMetadataOffset;
        if (timeSeriesMetadataIoSizeRecorder != null) {
          timeSeriesMetadataIoSizeRecorder.accept(
              entry.getValue().getRight().getRight() - entry.getValue().getRight().getLeft());
          timeSeriesMetadataCountRecorder.accept(1);
        }
        for (IChunkMetadata chunkMetadata : entry.getValue().getLeft()) {
          minChunkOffset = Math.min(minChunkOffset, chunkMetadata.getOffsetOfChunkHeader());
          break;
        }
      }
      return new Offsets(
          minChunkOffset - chunkGroupHeaderSize,
          minTimeseriesMetadataOffset,
          rootMeasurementNodeStartOffset);
    }
  }

  private Pair<Long, Long> getTimeColumnMetadataOffset(
      TsFileSequenceReader reader, MetadataIndexNode measurementNode) throws IOException {
    if (measurementNode.isDeviceLevel()) {
      throw new IllegalArgumentException("device level metadata index node is not supported");
    }
    List<IMetadataIndexEntry> children = measurementNode.getChildren();
    long startOffset = children.get(0).getOffset();
    long endOffset =
        children.size() > 1 ? children.get(1).getOffset() : measurementNode.getEndOffset();
    if (measurementNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      return new Pair<>(startOffset, endOffset);
    } else {
      MetadataIndexNode metadataIndexNode =
          reader.readMetadataIndexNode(
              startOffset, endOffset, false, timeSeriesMetadataIoSizeRecorder);
      return getTimeColumnMetadataOffset(reader, metadataIndexNode);
    }
  }

  @Override
  public void close() {
    releaseReadLocks();
  }

  protected static class Offsets {
    protected final long firstChunkOffset;
    protected final long firstTimeseriesMetadataOffset;
    protected final long firstMeasurementNodeOffset;

    public Offsets(
        long firstChunkOffset,
        long firstTimeseriesMetadataOffset,
        long firstMeasurementNodeOffset) {
      this.firstChunkOffset = firstChunkOffset;
      this.firstTimeseriesMetadataOffset = firstTimeseriesMetadataOffset;
      this.firstMeasurementNodeOffset = firstMeasurementNodeOffset;
    }

    protected long minusOffsetForTableModel(Offsets other) {
      return firstChunkOffset
          - other.firstChunkOffset
          + firstTimeseriesMetadataOffset
          - other.firstTimeseriesMetadataOffset
          + firstMeasurementNodeOffset
          - other.firstMeasurementNodeOffset;
    }

    protected long minusOffsetForTreeModel(Offsets other) {
      return firstChunkOffset
          - other.firstChunkOffset
          + firstTimeseriesMetadataOffset
          - other.firstTimeseriesMetadataOffset;
    }
  }
}
