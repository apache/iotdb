///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.iotdb.db.query.reader.seriesRelated;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import java.util.TreeSet;
//import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
//import org.apache.iotdb.db.engine.modification.Modification;
//import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
//import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
//import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
//import org.apache.iotdb.db.query.context.QueryContext;
//import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
//import org.apache.iotdb.db.query.reader.MemChunkLoader;
//import org.apache.iotdb.db.query.reader.chunkRelated.DiskChunkReader;
//import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
//import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
//import org.apache.iotdb.db.utils.QueryUtils;
//import org.apache.iotdb.db.utils.TimeValuePair;
//import org.apache.iotdb.tsfile.file.header.PageHeader;
//import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
//import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
//import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
//import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
//import org.apache.iotdb.tsfile.read.common.BatchData;
//import org.apache.iotdb.tsfile.read.common.Chunk;
//import org.apache.iotdb.tsfile.read.common.Path;
//import org.apache.iotdb.tsfile.read.common.TimeRange;
//import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
//import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
//import org.apache.iotdb.tsfile.read.filter.basic.Filter;
//import org.apache.iotdb.tsfile.read.reader.IChunkReader;
//import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
//
//public class NewSeriesReaderWithoutValueFilterBak implements ManagedSeriesReader {
//
//  private List<TsFileResource> seqTsFiles;
//  // disk chunk && mem chunk
//  private List<ChunkMetaData> seqChunkMetadatas;
//
//  private TreeSet<TsFileResource> unseqTsFiles;
//  // disk chunk && mem chunk
//  private TreeSet<ChunkMetaData> unseqChunkMetadatas;
//
//  private Filter timeFilter;
//  private TSDataType dataType;
//
//  private ChunkMetaData cachedChunkMetaData;
//  private boolean hasCachedNextChunk;
//
//  private IChunkReader currentChunkReader;
//  private boolean isCurrentChunkReaderInit = false;
//
//
//  private PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
//
//  private Path path;
//  private QueryContext context;
//
//  private PageHeader cachedPageHeader;
//  private boolean hasCachedNextPage;
//  private IChunkReader overlappedChunkReader;
//  private long latestDirectlyOverlappedPageEndTime;
//
//  private boolean managedByQueryManager;
//  private boolean hasRemaining;
//
//  public NewSeriesReaderWithoutValueFilterBak(QueryDataSource queryDataSource, TSDataType dataType,
//      Filter timeFilter, QueryContext context) throws IOException {
//    Path seriesPath = queryDataSource.getSeriesPath();
//    TreeSet<TsFileResource> unseqTsFilesSet = new TreeSet<>((o1, o2) -> {
//      Map<String, Long> startTimeMap = o1.getStartTimeMap();
//      Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
//      Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
//      Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());
//
//      return Long.compare(minTimeOfO1, minTimeOfO2);
//    });
//    unseqTsFilesSet.addAll(queryDataSource.getUnseqResources());
//
//    this.path = seriesPath;
//    this.seqTsFiles = queryDataSource.getSeqResources();
//    this.unseqTsFiles = unseqTsFilesSet;
//    this.timeFilter = timeFilter;
//    this.dataType = dataType;
//    this.context = context;
//
//    seqChunkMetadatas = loadChunkMetadatas(seqTsFiles.remove(0));
//    TreeSet<ChunkMetaData> unseqChunkMetadataSet = new TreeSet<>(
//        Comparator.comparingLong(ChunkMetaData::getStartTime));
//    unseqChunkMetadataSet.addAll(loadChunkMetadatas(unseqTsFiles.pollFirst()));
//    unseqChunkMetadatas = unseqChunkMetadataSet;
//
//    // 把所有 未封口的 顺序文件的 chunk metadata 都加进来
//
//    List<TsFileResource> unsealedResources = new ArrayList<>();
//    for (TsFileResource resource : seqTsFiles) {
//      seqChunkMetadatas.addAll(loadChunkMetadatas(resource));
//      unsealedResources.add(resource);
//    }
//    seqTsFiles.removeAll(unsealedResources);
//    unsealedResources.clear();
//
//    // 把所有 未封口的 乱序文件的 chunk metadata 都加进来
//
//    for (TsFileResource resource : unseqTsFiles) {
//      unseqChunkMetadatas.addAll(loadChunkMetadatas(resource));
//      unsealedResources.add(resource);
//    }
//    for (TsFileResource resource : unsealedResources) {
//      unseqTsFiles.remove(resource);
//    }
//
//  }
//
//  public boolean hasNextBatch() throws IOException {
//    if (hasNextChunk() && hasNextPage() && hasNextSatisfiedPage()) {
//      return true;
//    }
//    return false;
//  }
//
//  public BatchData nextBatch() throws IOException {
//    return nextPageData();
//  }
//
//  /**
//   * for raw data query
//   */
//  public boolean hasNextSatisfiedPage() throws IOException {
//    if (currentChunkReader.hasNextSatisfiedPage() || priorityMergeReader.hasNext()) {
//      return true;
//    }
//    hasCachedNextPage = false;
//    return false;
//  }
//
//  public BatchData nextPageData() throws IOException {
//
//    if (priorityMergeReader.hasNext()) {
//      return nextOverlappedPage();
//    }
//
//    if (currentChunkReader.hasNextSatisfiedPage()) {
//      return currentChunkReader.nextPageData();
//    }
//    return null;
//  }
//
//
//  /**
//   * for aggregation and group by
//   */
//  public boolean hasNextChunk() throws IOException {
//    if (hasCachedNextChunk) {
//      return true;
//    }
//
//    /**
//     * 只要 metadata 空了就从 resource 里补充一个
//     */
//    if (seqChunkMetadatas.isEmpty() && !seqTsFiles.isEmpty()) {
//      seqChunkMetadatas.addAll(loadChunkMetadatas(seqTsFiles.remove(0)));
//    }
//
//    if (unseqChunkMetadatas.isEmpty() && !unseqTsFiles.isEmpty()) {
//      unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqTsFiles.pollFirst()));
//    }
//    /**
//     * 拿顺序或乱序的第一个 ChunkMetadata，缓存起来
//     */
//    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
//      cachedChunkMetaData = seqChunkMetadatas.remove(0);
//      hasCachedNextChunk = true;
//      isCurrentChunkReaderInit = false;
//    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
//      cachedChunkMetaData = unseqChunkMetadatas.pollFirst();
//      hasCachedNextChunk = true;
//      isCurrentChunkReaderInit = false;
//    } else if (!seqChunkMetadatas.isEmpty()) {
//      // seq 和 unseq 的 chunk metadata 都不为空
//      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.first().getStartTime()) {
//        cachedChunkMetaData = seqChunkMetadatas.remove(0);
//      } else {
//        cachedChunkMetaData = unseqChunkMetadatas.pollFirst();
//      }
//      hasCachedNextChunk = true;
//      isCurrentChunkReaderInit = false;
//    } else {
//      // do not has chunk metadata in seq or unseq
//      hasCachedNextChunk = false;
//    }
//
//    if (!isCurrentChunkReaderInit) {
//      currentChunkReader = initChunkReader(cachedChunkMetaData);
//      isCurrentChunkReaderInit = true;
//    }
//
//    ChunkMetaData overlappedChunkMeta = getOverlappedChunkMeta();
//    if (overlappedChunkMeta != null) {
//      overlappedChunkReader = initChunkReader(overlappedChunkMeta);
//    }
//    return hasCachedNextChunk;
//  }
//
//  /**
//   * 加载一个 TsFileResource 的所有 ChunkMetadata， 如果是未封口的，把 memchunk 也加进来
//   */
//  private List<ChunkMetaData> loadChunkMetadatas(TsFileResource resource) throws IOException {
//    List<ChunkMetaData> currentChunkMetaDataList;
//    if (resource == null) {
//      return new ArrayList<>();
//    }
//    if (resource.isClosed()) {
//      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, path);
//    } else {
//      currentChunkMetaDataList = resource.getChunkMetaDataList();
//    }
//    // get modifications and apply to metadatas
//    List<Modification> pathModifications = context
//        .getPathModifications(resource.getModFile(), path.getFullPath());
//    for (ChunkMetaData data : currentChunkMetaDataList) {
//      if (data.getChunkLoader() == null) {
//        data.setChunkLoader(
//            new ChunkLoaderImpl(new TsFileSequenceReader(resource.getFile().getAbsolutePath())));
//      }
//    }
//    if (!pathModifications.isEmpty()) {
//      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
//    }
//    ReadOnlyMemChunk readOnlyMemChunk = resource.getReadOnlyMemChunk();
//    if (readOnlyMemChunk != null) {
//      currentChunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
//    }
//    return currentChunkMetaDataList;
//  }
//
//  public boolean canUseChunkStatistics() throws IOException {
//    ChunkMetaData overlappedChunkMeta = getOverlappedChunkMeta();
//    return canUseStatistics() && overlappedChunkMeta == null;
//  }
//
//  public boolean canUseChunkStatistics(TimeRange timeRange) throws IOException {
//    long minTime = cachedChunkMetaData.getStartTime();
//    long maxTime = cachedChunkMetaData.getEndTime();
//    return canUseChunkStatistics() && timeRange.contains(new TimeRange(minTime, maxTime));
//  }
//
//  public boolean canUseNextChunkData(long endTime) throws IOException {
//    return cachedChunkMetaData.getStartTime() <= endTime;
//  }
//
//  public boolean canUseNextPageData(long endTime) throws IOException {
//    return cachedPageHeader.getStartTime() <= endTime;
//  }
//
//  private ChunkMetaData getOverlappedChunkMeta() throws IOException {
//    if (!seqChunkMetadatas.isEmpty() && cachedChunkMetaData.getEndTime() >= seqChunkMetadatas.get(0)
//        .getStartTime()) {
//      return seqChunkMetadatas.remove(0);
//    }
//    if (!unseqChunkMetadatas.isEmpty() && cachedChunkMetaData.getEndTime() >=
//        unseqChunkMetadatas.first().getStartTime()) {
//      return unseqChunkMetadatas.pollFirst();
//    }
//    return null;
//  }
//
//  private boolean canUseStatistics() {
//    if (timeFilter == null || !timeFilter.containStartEndTime(cachedChunkMetaData.getStartTime(),
//        cachedChunkMetaData.getEndTime())) {
//      return false;
//    }
//    return true;
//  }
//
//  public Statistics nextChunkStatistics() throws IOException {
//    if (hasCachedNextChunk || hasNextChunk()) {
//      hasCachedNextChunk = false;
//      return cachedChunkMetaData.getStatistics();
//    } else {
//      throw new IOException("no more chunk metadata");
//    }
//  }
//
//  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
//    IChunkLoader chunkLoader = metaData.getChunkLoader();
//    if (chunkLoader instanceof MemChunkLoader) {
//      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
//      return new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
//    }
//    Chunk chunk = chunkLoader.getChunk(metaData);
//    return new ChunkReader(chunk, timeFilter);
//  }
//
//
//  public boolean hasNextPage() throws IOException {
//    if (hasCachedNextPage) {
//      return true;
//    }
//
//    if (isCurrentChunkReaderInit && currentChunkReader.hasNextSatisfiedPage()) {
//      cachedPageHeader = currentChunkReader.nextPageHeader();
//      hasCachedNextPage = true;
//    } else {
//      hasCachedNextChunk = false;
//      isCurrentChunkReaderInit = false;
//    }
//    return hasCachedNextPage;
//  }
//
//  public boolean canUsePageStatistics() throws IOException {
//    boolean isOverlapped = false;
//    PageHeader cachedPageHeader = this.cachedPageHeader;
//
//    while (overlappedChunkReader != null && overlappedChunkReader.hasNextSatisfiedPage()) {
//      PageHeader pageHeader = overlappedChunkReader.nextPageHeader();
//      if (cachedPageHeader.getEndTime() >= pageHeader.getStartTime()) {
//        isOverlapped = true;
//        latestDirectlyOverlappedPageEndTime = cachedPageHeader.getEndTime();
//        priorityMergeReader.addReaderWithPriority(new DiskChunkReader(overlappedChunkReader), 1);
//        priorityMergeReader.addReaderWithPriority(new DiskChunkReader(currentChunkReader), 1);
//      }
//    }
//
//    return canUseStatistics() && !isOverlapped;
//  }
//
//  public boolean canUsePageStatistics(TimeRange timeRange) throws IOException {
//    long minTime = cachedPageHeader.getStartTime();
//    long maxTime = cachedPageHeader.getEndTime();
//    return canUsePageStatistics() && timeRange.contains(new TimeRange(minTime, maxTime));
//  }
//
//
//  public BatchData nextOverlappedPage() throws IOException {
//    BatchData batchData = new BatchData(dataType);
//    while (priorityMergeReader.hasNext()) {
//      TimeValuePair timeValuePair = priorityMergeReader.next();
//      if (timeValuePair.getTimestamp() > latestDirectlyOverlappedPageEndTime) {
//        break;
//      }
//      batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
//    }
//    return batchData;
//
//  }
//
//
//  public Statistics nextPageStatistic() throws IOException {
//    if (hasCachedNextPage || hasNextPage()) {
//      hasCachedNextPage = false;
//      return cachedPageHeader.getStatistics();
//    } else {
//      throw new IOException("no next page header");
//    }
//  }
//
//  public void close() throws IOException {
//
//  }
//
//  @Override
//  public boolean isManagedByQueryManager() {
//    return managedByQueryManager;
//  }
//
//  @Override
//  public void setManagedByQueryManager(boolean managedByQueryManager) {
//    this.managedByQueryManager = managedByQueryManager;
//  }
//
//  @Override
//  public boolean hasRemaining() {
//    return hasRemaining;
//  }
//
//  @Override
//  public void setHasRemaining(boolean hasRemaining) {
//    this.hasRemaining = hasRemaining;
//  }
//}
