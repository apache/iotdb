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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader.VersionPageReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class InvertedSeriesReader {

  private final Path seriesPath;
  private final TSDataType dataType;
  private final QueryContext context;
  private long queryTime;

  /*
   * There is at most one is not null between timeFilter and valueFilter
   *
   * timeFilter is pushed down to all pages (seq, unseq) without correctness problem
   *
   * valueFilter is pushed down to non-overlapped page only
   */
  private final Filter timeFilter;
  private final Filter valueFilter;

  /*
   * file cache
   */
  private final List<TsFileResource> seqFileResource;
  private final PriorityQueue<TsFileResource> unseqFileResource;

  /*
   * chunk cache
   */
  private ChunkMetaData firstChunkMetaData;
  private ChunkMetaData lastSeqChunkMetadata;
  private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));

  /*
   * page cache
   */
  private PriorityQueue<VersionPageReader> cachedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(VersionPageReader::getStartTime));


  public InvertedSeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter,
      long queryTime) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.seqFileResource = dataSource.getSeqResources();
    this.unseqFileResource = sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
    this.queryTime = queryTime;
  }

  public boolean hasNextChunk() throws IOException {

    if (firstChunkMetaData != null) {
      return true;
    }

    // init first chunk metadata whose startTime is minimum
    UnpackAllOverlappedFilesToChunkMetadatas();

    return firstChunkMetaData != null;
  }

  public Statistics currentChunkStatistics() {
    return firstChunkMetaData.getStatistics();
  }

  public void skipCurrentChunk() {
    firstChunkMetaData = null;
  }

  public TimeValuePair getChunkLastPair() throws IOException {
    Statistics chunkStatistics = firstChunkMetaData.getStatistics();
    if (containedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    } else {
      unpackOneChunkMetaData(firstChunkMetaData);
      TimeValuePair resultPair = new TimeValuePair(0, null);
      for (VersionPageReader pageReader : cachedPageReaders) {
        Statistics pageStatistics = pageReader.getStatistics();
        if (containedByTimeFilter(pageStatistics)) {
          resultPair = constructLastPair(pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
        } else {
          BatchData batchData = pageReader.getAllSatisfiedPageData();
          resultPair = new TimeValuePair(
                  batchData.getMaxTimestamp(),
                  batchData.getTsPrimitiveTypeByIndex(batchData.length() - 1));
        }
      }
      return resultPair;
    }
  }

  private void unpackOneChunkMetaData(ChunkMetaData chunkMetaData) throws IOException {
    initChunkReader(chunkMetaData)
        .getPageReaderListWithTerminateTime(queryTime)
        .forEach(
            pageReader ->
                cachedPageReaders.add(
                    new VersionPageReader(chunkMetaData.getVersion(), pageReader)));
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, timeFilter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader;
  }


  private PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getEndTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getEndTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO2, minTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }


  /**
   * unpack all overlapped seq/unseq files and find the first chunk metadata
   * <p>
   * Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void UnpackAllOverlappedFilesToChunkMetadatas() throws IOException {
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.remove(index);
      List<ChunkMetaData> chunkMetadata = FileLoaderUtils.loadChunkMetadataFromTsFileResource(
          resource, seriesPath, context);
      if (!chunkMetadata.isEmpty()) {
        for (int i = chunkMetadata.size() - 1; i >= 0; i--) {
          if (chunkMetadata.get(i).getStartTime() <= queryTime) {
            lastSeqChunkMetadata = chunkMetadata.get(chunkMetadata.size() - 1);
            break;
          }
        }
        break;
      }
    }

    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()
        && (lastSeqChunkMetadata == null || (lastSeqChunkMetadata.getStartTime()
        <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())))) {
      unseqChunkMetadatas.addAll(
          FileLoaderUtils.loadChunkMetadataFromTsFileResource(
              unseqFileResource.poll(), seriesPath, context, timeFilter));
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}