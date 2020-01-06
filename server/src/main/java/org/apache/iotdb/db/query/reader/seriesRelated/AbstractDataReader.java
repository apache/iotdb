package org.apache.iotdb.db.query.reader.seriesRelated;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.iotdb.db.query.reader.chunkRelated.DiskChunkReader;
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

public abstract class AbstractDataReader implements ManagedSeriesReader {

  private final QueryDataSource queryDataSource;
  private final QueryContext context;
  private final Path seriesPath;
  private final TSDataType dataType;
  protected Filter filter;

  private final TreeSet<ChunkMetaData> seqChunkMetadatas;
  private final TreeSet<ChunkMetaData> unseqChunkMetadatas;

  protected IChunkReader chunkReader;
  protected ChunkMetaData chunkMetaData;

  protected IChunkReader overlappedChunkReader;
  protected ChunkMetaData overlappedChunkMetadata;

  protected PageHeader currentPage;
  protected PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
  private long latestDirectlyOverlappedPageEndTime;

  private boolean hasRemaining;
  private boolean managedByQueryManager;

  public AbstractDataReader(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context) throws StorageEngineException, IOException {
    queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context);
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;
    if (filter != null) {
      filter = queryDataSource.setTTL(filter);
      this.filter = filter;
    }
    seqChunkMetadatas = initSeqMetadatas();
    unseqChunkMetadatas = initUnSeqMetadatas();

    nextChunkReader();
  }

  protected boolean hasNextChunk() throws IOException {
    if (chunkReader == null) {
      nextChunkReader();
    }
    return chunkReader != null || overlappedChunkReader != null;
  }

  protected boolean hasNextPage() throws IOException {
    if (currentPage != null) {
      return true;
    }
    if (chunkReader == null || !chunkReader.hasNextSatisfiedPage()) {
      chunkReader = null;
      currentPage = null;
      return false;
    }
    currentPage = chunkReader.nextPageHeader();
    return true;
  }

  protected boolean hasNextBatch() throws IOException {
    if (!hasNextChunk() || !hasNextPage()) {
      return false;
    }
    if (overlappedChunkMetadata != null) {
      initPriorityMergeReader();
    }
    if (!chunkReader.hasNextSatisfiedPage() && !priorityMergeReader.hasNext()) {
      currentPage = null;
      return false;
    }
    return true;
  }

  protected BatchData nextBatch() throws IOException {
    if (priorityMergeReader.hasNext()) {
      return nextOverlappedPage();
    }
    if (chunkReader.hasNextSatisfiedPage()) {
      return chunkReader.nextPageData();
    }
    return null;
  }

  protected void initPriorityMergeReader() throws IOException {
    while (overlappedChunkReader != null && overlappedChunkReader.hasNextSatisfiedPage()) {
      PageHeader overlappedPage = overlappedChunkReader.nextPageHeader();
      if (currentPage.getEndTime() >= overlappedPage.getStartTime()) {
        latestDirectlyOverlappedPageEndTime = currentPage.getEndTime();
        priorityMergeReader.addReaderWithPriority(new DiskChunkReader(chunkReader), 1);
        priorityMergeReader.addReaderWithPriority(new DiskChunkReader(overlappedChunkReader), 2);
      }
    }
  }

  protected BatchData nextOverlappedPage() throws IOException {
    BatchData batchData = new BatchData(dataType);
    while (priorityMergeReader.hasNext()) {
      TimeValuePair timeValuePair = priorityMergeReader.next();
      if (timeValuePair.getTimestamp() > latestDirectlyOverlappedPageEndTime) {
        break;
      }
      batchData.putAnObject(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
    }
    overlappedChunkMetadata = null;
    return batchData;
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      return null;
    }
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      return new MemChunkReader(memChunkLoader.getChunk(), filter);
    }
    Chunk chunk = chunkLoader.getChunk(metaData);
    return new ChunkReader(chunk, filter);
  }

  protected void nextChunkReader() throws IOException {
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = seqChunkMetadatas.pollFirst();
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = unseqChunkMetadatas.pollFirst();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // seq 和 unseq 的 chunk metadata 都不为空
      if (seqChunkMetadatas.first().getStartTime() <= unseqChunkMetadatas.first().getStartTime()) {
        chunkMetaData = seqChunkMetadatas.pollFirst();
      } else {
        chunkMetaData = unseqChunkMetadatas.pollFirst();
      }
    } else {
      chunkMetaData = null;
    }

    chunkReader = initChunkReader(chunkMetaData);
    initNextOverlappedChunk();
  }

  private void initNextOverlappedChunk() throws IOException {
    if (!seqChunkMetadatas.isEmpty() && chunkMetaData.getEndTime() >= seqChunkMetadatas
        .first().getStartTime()) {
      overlappedChunkMetadata = seqChunkMetadatas.pollFirst();
    } else if (!unseqChunkMetadatas.isEmpty()
        && chunkMetaData.getEndTime() >= unseqChunkMetadatas
        .first().getStartTime()) {
      overlappedChunkMetadata = unseqChunkMetadatas.pollFirst();
    } else {
      overlappedChunkMetadata = null;
    }
    overlappedChunkReader = initChunkReader(overlappedChunkMetadata);
  }

  private TreeSet<ChunkMetaData> initSeqMetadatas() throws IOException {
    List<TsFileResource> seqResources = queryDataSource.getSeqResources();

    TreeSet<ChunkMetaData> chunkMetaDataSet = new TreeSet<>(
        Comparator.comparingLong(ChunkMetaData::getStartTime));
    for (TsFileResource resource : seqResources) {
      chunkMetaDataSet.addAll(loadChunkMetadatas(resource));
    }
    return chunkMetaDataSet;
  }

  private TreeSet<ChunkMetaData> initUnSeqMetadatas() throws IOException {
    TreeSet<TsFileResource> unseqTsFilesSet = new TreeSet<>((o1, o2) -> {
      Map<String, Long> startTimeMap = o1.getStartTimeMap();
      Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
      Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
      Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

      return Long.compare(minTimeOfO1, minTimeOfO2);
    });
    unseqTsFilesSet.addAll(queryDataSource.getUnseqResources());

    TreeSet<ChunkMetaData> chunkMetaDataSet = new TreeSet<>(
        Comparator.comparingLong(ChunkMetaData::getStartTime));
    for (TsFileResource resource : unseqTsFilesSet) {
      chunkMetaDataSet.addAll(loadChunkMetadatas(resource));
    }
    return chunkMetaDataSet;
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

}
