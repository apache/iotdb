package org.apache.iotdb.db.query.reader.seriesRelated;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

  private final List<TsFileResource> seqFileResource;
  private final TreeSet<TsFileResource> unseqFileResource;

  private final List<ChunkMetaData> seqChunkMetadatas = new ArrayList<>();
  private final TreeSet<ChunkMetaData> unseqChunkMetadatas = new TreeSet<>(
      Comparator.comparingLong(ChunkMetaData::getStartTime));

  protected boolean hasCachedNextChunk;
  private boolean isCurrentChunkReaderInit;
  protected IChunkReader chunkReader;
  protected ChunkMetaData chunkMetaData;

  private Map<Integer, Long> metaDataVersionCache = new HashMap<>();

  protected List<IChunkReader> overlappedChunkReader = new ArrayList<>();
  protected List<IChunkReader> overlappedPages = new ArrayList<>();

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
    if (filter != null) {
      filter = queryDataSource.setTTL(filter);
      this.filter = filter;
    }

    seqFileResource = queryDataSource.getSeqResources();
    unseqFileResource = sortUnSeqFileResources();

    //过滤所有不能使用的文件
    while (filter != null && !seqFileResource.isEmpty()) {
      TsFileResource tsFileResource = seqFileResource.get(0);
      long startTime = tsFileResource.getStartTimeMap()
          .getOrDefault(seriesPath.getDevice(), Long.MIN_VALUE);
      //防止未封口文件
      long endTime = tsFileResource.getEndTimeMap()
          .getOrDefault(seriesPath.getDevice(), Long.MAX_VALUE);
      if (!filter.satisfyStartEndTime(startTime, endTime)) {
        seqFileResource.remove(0);
        continue;
      }
      break;
    }
    while (filter != null && !unseqFileResource.isEmpty()) {
      TsFileResource tsFileResource = unseqFileResource.first();
      Long startTime = tsFileResource.getStartTimeMap()
          .getOrDefault(seriesPath.getDevice(), Long.MIN_VALUE);
      Long endTime = tsFileResource.getEndTimeMap()
          .getOrDefault(seriesPath.getDevice(), Long.MAX_VALUE);
      if (!filter.satisfyStartEndTime(startTime, endTime)) {
        unseqFileResource.pollFirst();
        continue;
      }
      break;
    }
    fillMetadataContainer();
  }

  protected boolean hasNextChunk() throws IOException {
    if (hasCachedNextChunk) {
      return true;
    }
    fillMetadataContainer();
    //删除所有不能用的chunk
    while (filter != null && (!seqChunkMetadatas.isEmpty() || !seqFileResource.isEmpty())) {
      if (seqChunkMetadatas.isEmpty()) {
        seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
      }

      ChunkMetaData metaData = seqChunkMetadatas.get(0);
      if (!filter.satisfy(metaData.getStatistics())) {
        seqChunkMetadatas.remove(0);
        continue;
      }
      break;
    }
    while (filter != null && (!unseqChunkMetadatas.isEmpty() || !unseqFileResource.isEmpty())) {
      if (unseqChunkMetadatas.isEmpty()) {
        unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
      }
      ChunkMetaData metaData = unseqChunkMetadatas.first();
      if (!filter.satisfy(metaData.getStatistics())) {
        unseqChunkMetadatas.pollFirst();
        continue;
      }
      break;
    }
    fillMetadataContainer();
    hasCachedNextChunk = true;
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = seqChunkMetadatas.remove(0);
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      chunkMetaData = unseqChunkMetadatas.pollFirst();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // seq 和 unseq 的 chunk metadata 都不为空
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.first().getStartTime()) {
        chunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        chunkMetaData = unseqChunkMetadatas.pollFirst();
      }
    } else {
      hasCachedNextChunk = false;
    }
    //查找所有可能存在相交点的无序文件
    while (!unseqFileResource.isEmpty()) {
      Map<String, Long> startTimeMap = unseqFileResource.first().getStartTimeMap();
      Long unSeqStartTime = startTimeMap.getOrDefault(seriesPath.getDevice(), Long.MIN_VALUE);
      if (chunkMetaData.getEndTime() > unSeqStartTime) {
        unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
        continue;
      }
      break;
    }

    //找到所有的相交的chunk
    while (!unseqChunkMetadatas.isEmpty()) {
      long startTime = unseqChunkMetadatas.first().getStartTime();

      if (chunkMetaData.getEndTime() > startTime) {
        ChunkMetaData metaData = unseqChunkMetadatas.pollFirst();
        IChunkReader chunkReader = initChunkReader(metaData);
        metaDataVersionCache.put(chunkReader.hashCode(), metaData.getVersion());
        overlappedChunkReader.add(chunkReader);
        continue;
      }
      break;
    }
    while (!seqChunkMetadatas.isEmpty() || !seqFileResource.isEmpty()) {
      //需要注意的是有可能文件里不存在这个测点的数据
      while (seqChunkMetadatas.isEmpty() && !seqChunkMetadatas.isEmpty()) {
        seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
      }
      if (seqChunkMetadatas.isEmpty()) {
        break;
      }
      long startTime = seqChunkMetadatas.get(0).getStartTime();

      if (chunkMetaData.getEndTime() > startTime) {
        ChunkMetaData metaData = seqChunkMetadatas.remove(0);
        IChunkReader chunkReader = initChunkReader(metaData);
        metaDataVersionCache.put(chunkReader.hashCode(), metaData.getVersion());
        overlappedChunkReader.add(chunkReader);
        continue;
      }
      break;
    }
    return hasCachedNextChunk;
  }

  private void fillMetadataContainer() throws IOException {
    //补充一次防止为空
    if (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
      seqChunkMetadatas.addAll(loadChunkMetadatas(seqFileResource.remove(0)));
    }
    if (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
      unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqFileResource.pollFirst()));
    }
  }

  protected boolean hasNextPage() throws IOException {
    if (hasCachedNextPage) {
      return true;
    }
    if (!isCurrentChunkReaderInit) {
      chunkReader = initChunkReader(chunkMetaData);
      isCurrentChunkReaderInit = true;
    }

    if (isCurrentChunkReaderInit && chunkReader.hasNextSatisfiedPage()) {
      currentPage = chunkReader.nextPageHeader();
      hasCachedNextPage = true;
      latestDirectlyOverlappedPageEndTime = currentPage.getEndTime();
      while (!overlappedChunkReader.isEmpty()) {
        IChunkReader iChunkReader = overlappedChunkReader.get(0);
        if (currentPage.getEndTime() > iChunkReader.nextPageHeader().getStartTime()) {
          overlappedPages.add(overlappedChunkReader.remove(0));
        }
      }
      return hasCachedNextPage;
    }

    isCurrentChunkReaderInit = false;
    hasCachedNextChunk = hasCachedNextPage;
    metaDataVersionCache.clear();
    return hasCachedNextPage;
  }

  protected boolean hasNextBatch() throws IOException {
    if (hasCachedNextBatch) {
      return true;
    }
    if (chunkReader.hasNextSatisfiedPage()) {
      priorityMergeReader.addReaderWithPriority(new DiskChunkReader(chunkReader),
          Long.valueOf(chunkMetaData.getVersion()).intValue());
      hasCachedNextBatch = true;
    }
    for (int i = 0; i < overlappedPages.size(); i++) {
      IChunkReader reader = overlappedPages.get(i);
      priorityMergeReader.addReaderWithPriority(new DiskChunkReader(reader),
          metaDataVersionCache.getOrDefault(reader.hashCode(), 0L).intValue());
      hasCachedNextBatch = true;
    }
    overlappedPages.clear();

    hasCachedNextPage = hasCachedNextBatch;
    return hasCachedNextBatch;
  }

  protected BatchData nextBatch() throws IOException {
    if (priorityMergeReader.hasNext()) {
      hasCachedNextBatch = false;
      return nextOverlappedPage();
    }
    return null;
  }


  protected BatchData nextOverlappedPage() throws IOException {
    BatchData batchData = new BatchData(dataType);
    while (priorityMergeReader.hasNext()) {
      TimeValuePair timeValuePair = priorityMergeReader.current();
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
    IChunkReader chunkReader = null;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), filter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, filter);
    }
    chunkReader.hasNextSatisfiedPage();
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

  private TreeSet<TsFileResource> sortUnSeqFileResources() {
    TreeSet<TsFileResource> unseqTsFilesSet = new TreeSet<>((o1, o2) -> {
      Map<String, Long> startTimeMap = o1.getStartTimeMap();
      Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
      Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
      Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

      return Long.compare(minTimeOfO1, minTimeOfO2);
    });
    unseqTsFilesSet.addAll(queryDataSource.getUnseqResources());
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


  private class VersionChunkKvPair {

    private long version;
    private IChunkReader reader;
  }

  private class VersionPageKvPair {

    private long version;
    private DiskChunkReader reader;
  }
}
