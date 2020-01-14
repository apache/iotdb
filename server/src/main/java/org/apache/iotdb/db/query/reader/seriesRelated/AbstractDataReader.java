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
import java.util.TreeSet;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunkRelated.ChunkDataIterator;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/*
 * This class implements a pause read method, pseudocode is:
 *
 *
 * while(hasNextChunk()){
 *    while(hasNextPage()){
 *      while(hasNextBatch()){
 *        nextBatch()
 *      }
 *    }
 * }
 */
public abstract class AbstractDataReader implements ManagedSeriesReader {

  private final QueryDataSource queryDataSource;
  private final QueryContext context;
  private final Path seriesPath;
  private final TSDataType dataType;
  protected Filter filter;

  private final List<TsFileResource> seqFileResource;
  private final TreeSet<TsFileResource> unseqFileResource;

  private final List<ChunkMetaData> seqChunkMetadatas = new ArrayList<>();
  private final TreeSet<ChunkMetaData> unseqChunkMetadatas = new TreeSet<>(
      Comparator.comparingLong(ChunkMetaData::getStartTime));

  private final List<IChunkLoader> openedChunkLoaders = new LinkedList<>();

  protected boolean hasCachedNextChunk;
  private boolean isCurrentChunkReaderInit;
  protected IChunkReader chunkReader;
  protected ChunkMetaData chunkMetaData;

  protected List<VersionPair<IChunkReader>> overlappedChunkReader = new ArrayList<>();
  protected List<VersionPair<IChunkReader>> overlappedPages = new ArrayList<>();

  protected boolean hasCachedNextPage;
  protected PageHeader currentPage;

  private boolean hasCachedNextBatch;
  protected PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
  private long latestDirectlyOverlappedPageEndTime = Long.MAX_VALUE;

  private boolean hasRemaining;
  private boolean managedByQueryManager;


  public AbstractDataReader(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context) throws StorageEngineException, IOException {
    queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context);
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = queryDataSource.setTTL(filter);

    seqFileResource = queryDataSource.getSeqResources();
    unseqFileResource = sortUnSeqFileResources(queryDataSource.getUnseqResources());

    removeInvalidFiles();
    tryToFillChunkMetadatas();
  }

  //for test
  public AbstractDataReader(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context, QueryDataSource dataSource)
      throws IOException {
    queryDataSource = dataSource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = queryDataSource.setTTL(filter);

    seqFileResource = queryDataSource.getSeqResources();
    unseqFileResource = sortUnSeqFileResources(queryDataSource.getUnseqResources());

    removeInvalidFiles();
    tryToFillChunkMetadatas();
  }

  //for test
  public AbstractDataReader(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context, List<TsFileResource> seqResources) throws IOException {
    this.queryDataSource = null;
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = filter;

    this.seqFileResource = seqResources;
    this.unseqFileResource = new TreeSet<>();

    removeInvalidFiles();
    tryToFillChunkMetadatas();
  }


  protected boolean hasNextChunk() throws IOException {
    if (hasCachedNextChunk) {
      return true;
    }
    removeInvalidChunks();
    searchMinimumChunk();
    //When the new chunk cannot be found, it is time to end all methods
    if (!hasCachedNextChunk) {
      return false;
    }
    unpackOverlappedFiles();
    unPackOverlappedChunks();
    return hasCachedNextChunk;
  }


  protected boolean hasNextPage() throws IOException {
    if (hasCachedNextPage) {
      return true;
    }
    if (!isCurrentChunkReaderInit) {
      chunkReader = initChunkReader(chunkMetaData);
      isCurrentChunkReaderInit = true;
    }
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
      fillOverlappedPages();
      return hasCachedNextPage;
    }

    isCurrentChunkReaderInit = false;
    chunkMetaData.getChunkLoader().close();
    hasCachedNextChunk = hasCachedNextPage;
    return hasCachedNextPage;
  }


  public boolean hasNextBatch() throws IOException {
    if (hasCachedNextBatch) {
      return true;
    }
    if (chunkReader.hasNextSatisfiedPage()) {
      priorityMergeReader
          .addReaderWithPriority(new ChunkDataIterator(chunkReader), chunkMetaData.getVersion());
      hasCachedNextBatch = true;
    }
    for (int i = 0; i < overlappedPages.size(); i++) {
      VersionPair<IChunkReader> reader = overlappedPages.get(i);
      priorityMergeReader
          .addReaderWithPriority(new ChunkDataIterator(reader.data), reader.version);
      hasCachedNextBatch = true;
    }
    overlappedPages.clear();

    hasCachedNextPage = hasCachedNextBatch;
    return hasCachedNextBatch;
  }

  public BatchData nextBatch() throws IOException {
    if (priorityMergeReader.hasNext()) {
      hasCachedNextBatch = false;
      return nextOverlappedPage();
    }
    throw new IOException("no next data");
  }


  protected BatchData nextOverlappedPage() throws IOException {
    BatchData batchData = new BatchData(dataType);
    while (priorityMergeReader.hasNext()) {
      TimeValuePair timeValuePair = priorityMergeReader.current();
      //TODO should add a batchSize to limit the number of reads per time
      if (timeValuePair.getTimestamp() > latestDirectlyOverlappedPageEndTime) {
        break;
      }
      batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
      priorityMergeReader.next();
    }
    return batchData;
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      return null;
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    openedChunkLoaders.add(chunkLoader);
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), filter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, filter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader;
  }

  private List<ChunkMetaData> loadChunkMetadatas(TsFileResource resource) throws IOException {
    List<ChunkMetaData> currentChunkMetaDataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      currentChunkMetaDataList = resource.getChunkMetaDataList();
    }
    List<Modification> pathModifications = context
        .getPathModifications(resource.getModFile(), seriesPath.getFullPath());
    for (ChunkMetaData data : currentChunkMetaDataList) {
      if (data.getChunkLoader() == null) {
        data.setChunkLoader(
            new ChunkLoaderImpl(new TsFileSequenceReader(resource.getFile().getAbsolutePath())));
      }
    }
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
    }
    ReadOnlyMemChunk readOnlyMemChunk = resource.getReadOnlyMemChunk();
    if (readOnlyMemChunk != null) {
      currentChunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
    }
    return currentChunkMetaDataList;
  }

  private TreeSet<TsFileResource> sortUnSeqFileResources(List<TsFileResource> tsFileResources) {
    TreeSet<TsFileResource> unseqTsFilesSet = new TreeSet<>((o1, o2) -> {
      Map<String, Long> startTimeMap = o1.getStartTimeMap();
      Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
      Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
      Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

      return Long.compare(minTimeOfO1, minTimeOfO2);
    });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  /**
   * Because you get a list of all the files, some files are not necessary when filters exist. This
   * method filters out the available data files based on the filter
   */
  private void removeInvalidFiles() {
    //filter seq files
    while (filter != null && !seqFileResource.isEmpty()) {
      if (!isValid(seqFileResource.get(0))) {
        seqFileResource.remove(0);
        continue;
      }
      break;
    }
    //filter unseq files
    while (filter != null && !unseqFileResource.isEmpty()) {
      if (!isValid(unseqFileResource.first())) {
        unseqFileResource.pollFirst();
        continue;
      }
      break;
    }
  }

  private boolean isValid(TsFileResource tsFileResource) {
    long startTime = tsFileResource.getStartTimeMap()
        .get(seriesPath.getDevice());
    long endTime = tsFileResource.getEndTimeMap()
        .getOrDefault(seriesPath.getDevice(), Long.MAX_VALUE);
    return filter.satisfyStartEndTime(startTime, endTime);
  }

  /**
   * unseq files are very special files that intersect not only with sequence files, but also with
   * other unseq files. So we need to find all tsfiles that overlapped with current chunk and
   * extract chunks from the resource.
   */
  private void unpackOverlappedFiles() throws IOException {
    while (!unseqFileResource.isEmpty()) {
      Map<String, Long> startTimeMap = unseqFileResource.first().getStartTimeMap();
      Long unSeqStartTime = startTimeMap.getOrDefault(seriesPath.getDevice(), Long.MAX_VALUE);
      if (chunkMetaData.getEndTime() >= unSeqStartTime) {
        unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
        continue;
      }
      break;
    }
    while (!seqFileResource.isEmpty()) {
      Map<String, Long> startTimeMap = seqFileResource.get(0).getStartTimeMap();
      Long seqStartTime = startTimeMap.getOrDefault(seriesPath.getDevice(), Long.MIN_VALUE);
      if (chunkMetaData.getEndTime() > seqStartTime) {
        seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
        continue;
      }
      break;
    }
  }

  /**
   * Because seq data and unseq data intersect, the minimum startTime taken from two files at a time
   * is used as the reference time to start reading data
   */
  private void searchMinimumChunk() {
    hasCachedNextChunk = true;
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = seqChunkMetadatas.remove(0);
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = unseqChunkMetadatas.pollFirst();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // neither seqChunkMetadatas nor unseqChunkMetadatas is null
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.first().getStartTime()) {
        chunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        chunkMetaData = unseqChunkMetadatas.pollFirst();
      }
    } else {
      hasCachedNextChunk = false;
    }
  }

  /**
   * Before reading the chunks, should first clean up all the useless chunks, because in the file
   * hierarchy, although the files are available, some of the internal chunks are still unavailable
   */
  private void removeInvalidChunks() throws IOException {
    //remove seq chunks
    while (filter != null && (!seqChunkMetadatas.isEmpty() || !seqFileResource.isEmpty())) {
      while (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
        seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
      }

      ChunkMetaData metaData = seqChunkMetadatas.get(0);
      if (!filter.satisfyStartEndTime(metaData.getStartTime(), metaData.getEndTime())) {
        seqChunkMetadatas.remove(0);
        continue;
      }
      break;
    }
    while (filter != null && (!unseqChunkMetadatas.isEmpty() || !unseqFileResource.isEmpty())) {
      while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
        unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
      }
      ChunkMetaData metaData = unseqChunkMetadatas.first();
      if (!filter.satisfyStartEndTime(metaData.getStartTime(), metaData.getEndTime())) {
        unseqChunkMetadatas.pollFirst();
        continue;
      }
      break;
    }
    tryToFillChunkMetadatas();
  }

  /**
   * Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may OOM, so we can only fill one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void tryToFillChunkMetadatas() throws IOException {
    while (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
      seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
    }
    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
      unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
    }
  }

  /**
   * Before calling this method, you should make sure that all the intersecting files are filled in
   * the container, because the files intersect, but some chunks may still be useless, so you need
   * to clean up all the unused chunks and populate the container. It should be noted that this
   * chunk collection is not in order, and all chunks should be used at once
   */
  private void unPackOverlappedChunks() throws IOException {
    while (!unseqChunkMetadatas.isEmpty()) {
      long startTime = unseqChunkMetadatas.first().getStartTime();

      if (chunkMetaData.getEndTime() >= startTime) {
        ChunkMetaData metaData = unseqChunkMetadatas.pollFirst();
        if (metaData != null) {
          IChunkReader chunkReader = initChunkReader(metaData);
          //When data points overlap, there should be a weight
          overlappedChunkReader.add(new VersionPair<>(metaData.getVersion(), chunkReader));
        }
        continue;
      }
      break;
    }
    while (!seqChunkMetadatas.isEmpty()) {
      long startTime = seqChunkMetadatas.get(0).getStartTime();

      if (chunkMetaData.getEndTime() >= startTime) {
        ChunkMetaData metaData = seqChunkMetadatas.remove(0);
        if (metaData != null) {
          IChunkReader chunkReader = initChunkReader(metaData);
          overlappedChunkReader.add(new VersionPair<>(metaData.getVersion(), chunkReader));
        }
        continue;
      }
      break;
    }
  }

  /**
   * This is just a fake page container, because no matter how chaotic the situation is, the data in
   * a chunk is always in order, so when the first page of the chunk cannot be used, there is no
   * need to look at the whole chunk.
   */
  private void fillOverlappedPages() {
    currentPage = chunkReader.nextPageHeader();
    hasCachedNextPage = true;
    latestDirectlyOverlappedPageEndTime = currentPage.getEndTime();
    while (!overlappedChunkReader.isEmpty()) {
      VersionPair<IChunkReader> iChunkReader = overlappedChunkReader.get(0);
      if (currentPage.getEndTime() > iChunkReader.data.nextPageHeader().getStartTime()) {
        overlappedPages.add(overlappedChunkReader.remove(0));
      }
    }
  }

  private class VersionPair<T> {

    private long version;
    private T data;

    public VersionPair(long version, T data) {
      this.version = version;
      this.data = data;
    }
  }

  public void close() throws IOException {
    if (chunkMetaData != null) {
      chunkMetaData.getChunkLoader().close();
    }
    for (int i = 0; i < openedChunkLoaders.size(); i++) {
      openedChunkLoaders.get(i).close();
    }
  }
}
