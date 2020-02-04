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
package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class SeriesReader {

  private final Path seriesPath;
  private final TSDataType dataType;
  private final QueryContext context;
  private final Filter timeFilter;
  private final Filter valueFilter;

  private final List<TsFileResource> seqFileResource;
  private final PriorityQueue<TsFileResource> unseqFileResource;

  private final List<ChunkMetaData> seqChunkMetadatas = new LinkedList<>();
  private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));

  private boolean hasCachedFirstChunkMetadata;
  private ChunkMetaData firstChunkMetaData;

  private PriorityQueue<VersionPair<IPageReader>> overlappedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(pageReader -> pageReader.data.getStatistics().getStartTime()));

  private PriorityMergeReader mergeReader = new PriorityMergeReader();

  private boolean hasCachedNextBatch;
  private BatchData cachedBatchData;

  private long currentPageEndTime = Long.MAX_VALUE;


  public SeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    this.seqFileResource = dataSource.getSeqResources();
    this.unseqFileResource = sortUnSeqFileResources(dataSource.getUnseqResources());
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }

  @TestOnly
  public SeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    this.seqFileResource = seqFileResource;
    this.unseqFileResource = sortUnSeqFileResources(unseqFileResource);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }


  public boolean hasNextChunk() throws IOException {
    if (hasCachedFirstChunkMetadata) {
      return true;
    }
    // init first chunkReader whose startTime is minimum
    tryToInitFirstChunk();

    return hasCachedFirstChunkMetadata;
  }

  /**
   * Because seq data and unseq data intersect, the minimum startTime taken from two files at a time
   * is used as the reference time to start reading data
   */
  private void tryToInitFirstChunk() throws IOException {
    tryToFillChunkMetadatas();
    hasCachedFirstChunkMetadata = true;
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      // only has seq
      firstChunkMetaData = seqChunkMetadatas.remove(0);
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      // only has unseq
      firstChunkMetaData = unseqChunkMetadatas.poll();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // has seq and unseq
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.peek().getStartTime()) {
        firstChunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        firstChunkMetaData = unseqChunkMetadatas.poll();
      }
    } else {
      // no seq nor unseq
      hasCachedFirstChunkMetadata = false;
    }
    tryToFillChunkMetadatas();
  }

  public boolean isChunkOverlapped() {
    Statistics chunkStatistics = firstChunkMetaData.getStatistics();
    return mergeReader.hasNextTimeValuePair()
        || (!seqChunkMetadatas.isEmpty()
        && chunkStatistics.getEndTime() >= seqChunkMetadatas.get(0).getStartTime())
        || (!unseqChunkMetadatas.isEmpty()
        && chunkStatistics.getEndTime() >= unseqChunkMetadatas.peek().getStartTime());
  }

  public Statistics currentChunkStatistics() {
    return firstChunkMetaData.getStatistics();
  }

  public void skipCurrentChunk() {
    hasCachedFirstChunkMetadata = false;
    firstChunkMetaData = null;
  }

  public boolean hasNextPage() throws IOException {
    if (!overlappedPageReaders.isEmpty()) {
      return true;
    }

    fillOverlappedPageReaders();

    return !overlappedPageReaders.isEmpty();
  }

  private void fillOverlappedPageReaders() throws IOException {
    if (!hasCachedFirstChunkMetadata) {
      return;
    }
    unpackOneChunkMetaData(firstChunkMetaData);
    hasCachedFirstChunkMetadata = false;
    firstChunkMetaData = null;
  }

  private void unpackOneChunkMetaData(ChunkMetaData chunkMetaData) throws IOException {
    initChunkReader(chunkMetaData)
        .getPageReaderList()
        .forEach(
            pageReader ->
                overlappedPageReaders.add(
                    new VersionPair(chunkMetaData.getVersion(), pageReader)));
  }


  public BatchData nextPage() throws IOException {
    BatchData pageData = Objects
        .requireNonNull(overlappedPageReaders.poll().data, "No Batch data")
        .getAllSatisfiedPageData();
    if (valueFilter == null) {
      return pageData;
    }
    BatchData batchData = new BatchData(pageData.getDataType());
    while (pageData.hasCurrent()) {
      if (valueFilter.satisfy(pageData.currentTime(), pageData.currentValue())) {
        batchData.putAnObject(pageData.currentTime(), pageData.currentValue());
      }
      pageData.next();
    }
    return batchData;
  }

  public boolean isPageOverlapped() {
    Statistics pageStatistics = overlappedPageReaders.peek().data.getStatistics();
    return mergeReader.hasNextTimeValuePair()
        || (!seqChunkMetadatas.isEmpty()
        && pageStatistics.getEndTime() >= seqChunkMetadatas.get(0).getStartTime())
        || (!unseqChunkMetadatas.isEmpty()
        && pageStatistics.getEndTime() >= unseqChunkMetadatas.peek().getStartTime());
  }

  public Statistics currentPageStatistics() throws IOException {
    if (overlappedPageReaders.isEmpty() || overlappedPageReaders.peek().data == null) {
      throw new IOException("No next page statistics.");
    }
    return overlappedPageReaders.peek().data.getStatistics();
  }

  public void skipCurrentPage() {
    overlappedPageReaders.poll();
  }

  public boolean hasNextOverlappedPage() throws IOException {

    if (hasCachedNextBatch) {
      return true;
    }

    putAllDirectlyOverlappedPageReadersIntoMergeReader();

    if (mergeReader.hasNextTimeValuePair()) {
      cachedBatchData = new BatchData(dataType);
      currentPageEndTime = mergeReader.getCurrentLargestEndTime();
      while (mergeReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();
        if (timeValuePair.getTimestamp() > currentPageEndTime) {
          break;
        }
        // unpack all overlapped chunks
        while (true) {
          tryToFillChunkMetadatas();
          boolean hasOverlappedChunkMetadata = false;
          if (!seqChunkMetadatas.isEmpty()
              && timeValuePair.getTimestamp() >= seqChunkMetadatas.get(0).getStartTime()) {
            unpackOneChunkMetaData(seqChunkMetadatas.remove(0));
            hasOverlappedChunkMetadata = true;
          }
          if (!unseqChunkMetadatas.isEmpty()
              && timeValuePair.getTimestamp() >= unseqChunkMetadatas.peek().getStartTime()) {
            unpackOneChunkMetaData(unseqChunkMetadatas.poll());
            hasOverlappedChunkMetadata = true;
          }
          if (!hasOverlappedChunkMetadata) {
            break;
          }
        }

        // put all overlapped pages into merge reader
        while (!overlappedPageReaders.isEmpty()
            && timeValuePair.getTimestamp()
            >= overlappedPageReaders.peek().data.getStatistics().getStartTime()) {
          VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
          mergeReader.addReader(
              pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
              pageReader.data.getStatistics().getEndTime());
        }

        timeValuePair = mergeReader.nextTimeValuePair();
        if (valueFilter == null) {
          cachedBatchData.putAnObject(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
        } else if (valueFilter
            .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
          cachedBatchData.putAnObject(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
        }
      }
      hasCachedNextBatch = cachedBatchData.hasCurrent();
    }
    return hasCachedNextBatch;
  }

  private void putAllDirectlyOverlappedPageReadersIntoMergeReader() throws IOException {

    if (mergeReader.hasNextTimeValuePair()) {
      currentPageEndTime = mergeReader.getCurrentLargestEndTime();
    } else if (!overlappedPageReaders.isEmpty()) {
      // put the first page into merge reader
      currentPageEndTime = overlappedPageReaders.peek().data.getStatistics().getEndTime();
      VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
      mergeReader.addReader(
          pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
          pageReader.data.getStatistics().getEndTime());
    } else {
      return;
    }

    // unpack all overlapped seq chunk meta data into overlapped page readers
    while (!seqChunkMetadatas.isEmpty()
        && currentPageEndTime >= seqChunkMetadatas.get(0).getStartTime()) {
      unpackOneChunkMetaData(seqChunkMetadatas.remove(0));
      tryToFillChunkMetadatas();
    }
    // unpack all overlapped unseq chunk meta data into overlapped page readers
    while (!unseqChunkMetadatas.isEmpty()
        && currentPageEndTime >= unseqChunkMetadatas.peek().getStartTime()) {
      unpackOneChunkMetaData(unseqChunkMetadatas.poll());
      tryToFillChunkMetadatas();
    }

    // put all page that directly overlapped with first page into merge reader
    while (!overlappedPageReaders.isEmpty()
        && currentPageEndTime >= overlappedPageReaders.peek().data.getStatistics().getStartTime()) {
      VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
      mergeReader.addReader(
          pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
          pageReader.data.getStatistics().getEndTime());
    }
  }

  public BatchData nextOverlappedPage() throws IOException {
    if (hasCachedNextBatch || hasNextOverlappedPage()) {
      hasCachedNextBatch = false;
      return cachedBatchData;
    }
    throw new IOException("No more batch data");
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      return null;
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

  private List<ChunkMetaData> loadSatisfiedChunkMetadatas(TsFileResource resource)
      throws IOException {
    List<ChunkMetaData> currentChunkMetaDataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      currentChunkMetaDataList = resource.getChunkMetaDataList();
    }
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
    }

    for (ChunkMetaData data : currentChunkMetaDataList) {
      if (data.getChunkLoader() == null) {
        TsFileSequenceReader tsFileSequenceReader = FileReaderManager.getInstance()
            .get(resource, resource.isClosed());
        data.setChunkLoader(new ChunkLoaderImpl(tsFileSequenceReader));
      }
    }
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          currentChunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    if (timeFilter != null) {
      currentChunkMetaDataList.removeIf(
          a -> !timeFilter.satisfyStartEndTime(a.getStartTime(), a.getEndTime()));
    }
    return currentChunkMetaDataList;
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResources(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getStartTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO1, minTimeOfO2);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  /**
   * Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void tryToFillChunkMetadatas() throws IOException {
    // Fill sequence chunkMetadatas until it is not empty
    while (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
      seqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(seqFileResource.remove(0)));
    }
    // Fill unsequence chunkMetadatas until it is not empty
    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
      unseqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(unseqFileResource.poll()));
    }
  }

  public void setTimeFilter(long timestamp) {
    ((UnaryFilter) timeFilter).setValue(timestamp);
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  private class VersionPair<T> {

    protected long version;
    protected T data;

    public VersionPair(long version, T data) {
      this.version = version;
      this.data = data;
    }
  }
}
