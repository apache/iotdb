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
package org.apache.iotdb.db.query.executor;

import java.sql.Time;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
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
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class PreviousFillExecutor {

  private final Path seriesPath;
  private final TSDataType dataType;
  private final QueryContext context;
  private long queryTime;

  private final Filter timeFilter;

  /*
   * file cache
   */
  private final List<TsFileResource> seqFileResource;
  private final PriorityQueue<TsFileResource> unseqFileResource;

  /*
   * chunk cache
   */
  private ChunkMetaData lastSeqChunkMetaData;
  private final List<ChunkMetaData> chunkMetadatas;

  public PreviousFillExecutor(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter,
      long queryTime) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.seqFileResource = dataSource.getSeqResources();
    this.unseqFileResource = sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());
    this.chunkMetadatas = new ArrayList<>();
    this.timeFilter = timeFilter;
    this.queryTime = queryTime;
  }

  public TimeValuePair getLastPoint() throws IOException {
    UnpackAllOverlappedFilesToChunkMetadatas();

    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    long lastVersion = 0;
    while (!chunkMetadatas.isEmpty()) {
      ChunkMetaData chunkMetaData = chunkMetadatas.remove(0);
      TimeValuePair lastChunkPoint = getChunkLastPoint(chunkMetaData);
      if (shouldUpdate(
          lastPoint.getTimestamp(), chunkMetaData.getVersion(),
          lastChunkPoint.getTimestamp(), lastVersion)) {
        lastPoint = lastChunkPoint;
      }
    }
    return lastPoint;
  }

  private TimeValuePair getChunkLastPoint(ChunkMetaData chunkMetaData) throws IOException {
    Statistics chunkStatistics = chunkMetaData.getStatistics();
    if (!timeFilter.satisfy(chunkStatistics)) {
      return null;
    }
    if (containedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    } else {
      List<IPageReader> pageReaders = unpackChunkReaderToPageReaderList(chunkMetaData);
      TimeValuePair lastPoint = new TimeValuePair(0, null);
      for (IPageReader pageReader : pageReaders) {
        TimeValuePair lastPagePoint = getPageLastPoint(pageReader);
        if (lastPoint.getTimestamp() < lastPagePoint.getTimestamp()) {
          lastPoint = lastPagePoint;
        }
      }
      return lastPoint;
    }
  }

  private TimeValuePair getPageLastPoint(IPageReader pageReader) throws IOException {
    Statistics pageStatistics = pageReader.getStatistics();
    if (!timeFilter.satisfy(pageStatistics)) {
      return null;
    }
    if (containedByTimeFilter(pageStatistics)) {
      return constructLastPair(
          pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
    } else {
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      return batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
    }
  }

  private boolean shouldUpdate(long time, long version, long newTime, long newVersion) {
    return time < newTime || (time == newTime && version < newVersion);
  }

  private List<IPageReader> unpackChunkReaderToPageReaderList(ChunkMetaData metaData) throws IOException {
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
    return chunkReader.getPageReaderList();
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
   */
  private void UnpackAllOverlappedFilesToChunkMetadatas() throws IOException {
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.remove(index);
      List<ChunkMetaData> chunkMetadata = FileLoaderUtils.loadChunkMetadataFromTsFileResource(
          resource, seriesPath, context);
      if (!chunkMetadata.isEmpty()) {
        for (int i = chunkMetadata.size() - 1; i >= 0; i--) {
          if (chunkMetadata.get(i).getStartTime() <= queryTime) {
            lastSeqChunkMetaData = chunkMetadata.get(chunkMetadata.size() - 1);
            chunkMetadatas.add(lastSeqChunkMetaData);
            break;
          }
        }
        break;
      }
    }

    while (!unseqFileResource.isEmpty()
        && (lastSeqChunkMetaData == null || (lastSeqChunkMetaData.getStartTime()
        <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())))) {
      chunkMetadatas.addAll(
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