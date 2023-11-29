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

package org.apache.iotdb.db.queryengine.execution.operator.scan;

import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

public class ChunkMetadataProvider {

  private final TimeSeriesMetadataProvider sourceProvider;

  private IChunkMetadata firstChunkMetadata;
  private final PriorityQueue<IChunkMetadata> cachedChunkMetadata;

  private final SeriesScanUtil.TimeOrderUtils orderUtils;

  public ChunkMetadataProvider(
      TimeSeriesMetadataProvider sourceProvider, SeriesScanUtil.TimeOrderUtils orderUtils) {
    this.sourceProvider = sourceProvider;
    this.cachedChunkMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                chunkMetadata -> orderUtils.getOrderTime(chunkMetadata.getStatistics())));
    this.orderUtils = orderUtils;
  }

  public boolean isCachedChunkMetadataConsumed() {
    return firstChunkMetadata == null && cachedChunkMetadata.isEmpty();
  }

  public boolean hasCurrentChunk() {
    return firstChunkMetadata != null;
  }

  public boolean hasNext() {
    return !cachedChunkMetadata.isEmpty()
        && (sourceProvider.hasCurrentFile() || sourceProvider.hasNext());
  }

  public void filterFirstChunkMetadata() throws IOException {
    if (firstChunkMetadata != null && !isChunkOverlapped() && !firstChunkMetadata.isModified()) {
      Filter queryFilter = scanOptions.getPushDownFilter();
      Statistics statistics = firstChunkMetadata.getStatistics();
      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        long rowCount = statistics.getCount();
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentChunk();
          paginationController.consumeOffset(rowCount);
        }
      } else if (!queryFilter.satisfy(statistics)) {
        skipCurrentChunk();
      }
    }
  }

  public void skipCurrentChunk() {
    firstChunkMetadata = null;
  }

  public boolean currentChunkModified() {
    return firstChunkMetadata.isModified();
  }

  /** construct first chunk metadata */
  public void initFirstChunkMetadata() throws IOException {
    if (sourceProvider.hasCurrentFile()) {
      /*
       * try to unpack all overlapped TimeSeriesMetadata to cachedChunkMetadata
       */
      sourceProvider.unpackAllOverlappedTsFilesToTimeSeriesMetadata(
          orderUtils.getOverlapCheckTime(
              timeSeriesMetadataProvider.getCurrentFile().getStatistics()));
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
          orderUtils.getOverlapCheckTime(
              timeSeriesMetadataProvider.getCurrentFile().getStatistics()),
          true);
    } else {
      /*
       * first time series metadata is already unpacked, consume cached ChunkMetadata
       */
      while (!cachedChunkMetadata.isEmpty()) {
        firstChunkMetadata = cachedChunkMetadata.peek();
        sourceProvider.unpackAllOverlappedTsFilesToTimeSeriesMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()));
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), false);
        if (firstChunkMetadata.equals(cachedChunkMetadata.peek())) {
          firstChunkMetadata = cachedChunkMetadata.poll();
          break;
        }
      }
    }
  }

  public void unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
      long endpointTime, boolean init) {
    while (!seqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, seqTimeSeriesMetadata.get(0).getStatistics())) {
      unpackOneTimeSeriesMetadata(seqTimeSeriesMetadata.remove(0));
    }
    while (!unSeqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unSeqTimeSeriesMetadata.peek().getStatistics())) {
      unpackOneTimeSeriesMetadata(unSeqTimeSeriesMetadata.poll());
    }

    if (sourceProvider.hasCurrentFile()
        && orderUtils.isOverlapped(endpointTime, sourceProvider.getCurrentFile().getStatistics())) {
      unpackOneTimeSeriesMetadata(sourceProvider.getCurrentFile());
      sourceProvider.skipCurrentFile();
    }

    if (init && firstChunkMetadata == null && !cachedChunkMetadata.isEmpty()) {
      firstChunkMetadata = cachedChunkMetadata.poll();
    }
  }

  private void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata) {
    List<IChunkMetadata> chunkMetadataList =
        FileLoaderUtils.loadChunkMetadataList(timeSeriesMetadata);
    chunkMetadataList.forEach(chunkMetadata -> chunkMetadata.setSeq(timeSeriesMetadata.isSeq()));

    cachedChunkMetadata.addAll(chunkMetadataList);
  }

  public boolean currentChunkOverlapped() {
    Statistics chunkStatistics = firstChunkMetadata.getStatistics();
    return !cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(chunkStatistics, cachedChunkMetadata.peek().getStatistics());
  }

  public IChunkMetadata getCurrentChunk() {
    return firstChunkMetadata;
  }
}
