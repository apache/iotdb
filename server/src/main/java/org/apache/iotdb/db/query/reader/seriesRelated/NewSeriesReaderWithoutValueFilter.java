package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.AbstractChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class NewSeriesReaderWithoutValueFilter {

  private List<TsFileResource> seqTsFiles;
  // disk chunk && mem chunk
  private List<ChunkMetaData> seqChunkMetadatas;

  private TreeSet<TsFileResource> unseqTsFiles;
  // disk chunk && mem chunk
  private TreeSet<ChunkMetaData> unseqChunkMetadatas;

  private Filter timeFilter;
  private TSDataType dataType;

  private ChunkMetaData cachedChunkMetaData;
  private boolean hasCachedNextChunk;

  private AbstractChunkReader currentChunkReader;
  private boolean isCurrentChunkReaderInit = false;


  private PriorityMergeReader priorityMergeReader = new PriorityMergeReader();

  private Path path;
  private QueryContext context;

  private PageHeader cachedPageHeader;
  private boolean hasCachedNextPage;

  public NewSeriesReaderWithoutValueFilter(Path seriesPath, TSDataType dataType, Filter timeFilter,
      QueryContext context) throws StorageEngineException, IOException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context);
    timeFilter = queryDataSource.setTTL(timeFilter);

    this.path = seriesPath;
    this.seqTsFiles = queryDataSource.getSeqResources();
    this.unseqTsFiles = queryDataSource.getUnseqResources();
    this.timeFilter = timeFilter;
    this.dataType = dataType;
    this.context = context;

    seqChunkMetadatas = loadChunkMetadatas(seqTsFiles.remove(0));
    unseqChunkMetadatas = loadChunkMetadatas(unseqTsFiles.remove(0));

    // 把所有 未封口的 顺序文件的 chunk metadata 都加进来

    List<TsFileResource> unsealedResources = new ArrayList<>();
    for (TsFileResource resource : seqTsFiles) {
      seqChunkMetadatas.addAll(loadChunkMetadatas(resource));
      unsealedResources.add(resource);
    }
    for (TsFileResource resource: unsealedResources) {
      seqTsFiles.remove(resource);
    }

    unsealedResources.clear();

    // 把所有 未封口的 乱序文件的 chunk metadata 都加进来

    for (TsFileResource resource : unseqTsFiles) {
      unseqChunkMetadatas.addAll(loadChunkMetadatas(resource));
      unsealedResources.add(resource);
    }
    for (TsFileResource resource: unsealedResources) {
      unseqTsFiles.remove(resource);
    }

  }



  /**
   * for raw data query
   */
  public boolean hasNextBatch() throws IOException {

    if (hasNextChunk()) {
      if (!isNextChunkOverlapped()) {
        return currentChunkReader.hasNextBatch();
      }
    }

    return false;
  }

  public BatchData nextBatch() throws IOException {
    currentChunkReader.nextBatch();
    return null;
  }





  /**
   * for aggregation and group by
   */

  public boolean hasNextChunk() throws IOException {
    if (hasCachedNextChunk) {
      return true;
    }

    /**
     * 只要 metadata 空了就从 resource 里补充一个
     */
    if (seqChunkMetadatas.isEmpty() && !seqTsFiles.isEmpty()) {
      seqChunkMetadatas.addAll(loadChunkMetadatas(seqTsFiles.remove(0)));
    }

    if (unseqChunkMetadatas.isEmpty() && !unseqTsFiles.isEmpty()) {
      unseqChunkMetadatas.addAll(loadChunkMetadatas(unseqTsFiles.remove(0)));
    }

    /**
     * 拿顺序或乱序的第一个 ChunkMetadata，缓存起来
     */
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      cachedChunkMetaData = seqChunkMetadatas.remove(0);
      hasCachedNextChunk = true;
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      cachedChunkMetaData = unseqChunkMetadatas.remove(0);
      hasCachedNextChunk = true;
    } else if (!seqChunkMetadatas.isEmpty()) {
      // seq 和 unseq 的 chunk metadata 都不为空
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.get(0).getStartTime()) {
        cachedChunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        cachedChunkMetaData = unseqChunkMetadatas.remove(0);
      }
      hasCachedNextChunk = true;
    } else {
      // do not has chunk metadata in seq or unseq
      hasCachedNextChunk = false;
    }
    return hasCachedNextChunk;
  }

  /**
   * 加载一个 TsFileResource 的所有 ChunkMetadata， 如果是未封口的，把 memchunk 也加进来
   */
  private List<ChunkMetaData> loadChunkMetadatas(TsFileResource resource) throws IOException {
    List<ChunkMetaData> currentChunkMetaDataList;
    if (resource.isClosed()) {
      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, path);
    } else {
      currentChunkMetaDataList = resource.getChunkMetaDataList();
    }
    // get modifications and apply to metadatas
    List<Modification> pathModifications = context
        .getPathModifications(resource.getModFile(), path.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
    }
    return currentChunkMetaDataList;
  }

  public boolean isNextChunkOverlapped() throws IOException {
    boolean isOverlapped = false;
    if (!seqChunkMetadatas.isEmpty() && cachedChunkMetaData.getEndTime() >= seqChunkMetadatas.get(0)
        .getStartTime()) {
      isOverlapped = true;
    }
    if (!unseqChunkMetadatas.isEmpty() && cachedChunkMetaData.getEndTime() >= unseqChunkMetadatas
        .get(0).getStartTime()) {
      isOverlapped = true;
    }

    /**
     * 初始化下一个 chunk reader
     */
    if (isOverlapped && !isCurrentChunkReaderInit) {
      initCurrentChunkReader();
    }

    return isOverlapped;
  }

  public ChunkMetaData nextChunkMetadata() throws IOException {
    if (hasCachedNextChunk || hasNextChunk()) {
      hasCachedNextChunk = false;
      return cachedChunkMetaData;
    } else {
      throw new IOException("no more chunk metadata");
    }
  }

  private void initCurrentChunkReader() throws IOException {
    IChunkLoader chunkLoader = cachedChunkMetaData.getChunkLoader();
    Chunk chunk = chunkLoader.getChunk(cachedChunkMetaData);
    currentChunkReader = new ChunkReader(chunk, timeFilter);
    isCurrentChunkReaderInit = true;
  }



  public boolean hasNextPage() throws IOException {
    if (hasCachedNextPage) {
      return true;
    }
    if (!isCurrentChunkReaderInit) {
      initCurrentChunkReader();
    }
    if (isCurrentChunkReaderInit && currentChunkReader.hasNextBatch()) {
      cachedPageHeader = currentChunkReader.nextPageHeader();
      hasCachedNextPage = true;
    }
    return hasCachedNextPage;
  }

  public boolean isNextPageOverlapped() {

    PageHeader pageHeader = cachedPageHeader;


    return false;
  }


  public BatchData nextOverlappedPage() throws IOException {
    BatchData batchData = new BatchData(dataType);
    long latestDirectlyOverlappedPageEndTime = 0;
    for (priorityMergeReader.hasNext()) {
      TimeValuePair timeValuePair = priorityMergeReader.current();
      if (timeValuePair.getTimestamp() > latestDirectlyOverlappedPageEndTime) {
        break;
      }
      batchData.putTime(timeValuePair.getTimestamp());
      batchData.putAnObject(timeValuePair.getValue().getValue());
    }

    return batchData;

  }



  public PageHeader nextPageHeader() throws IOException {
    if (hasCachedNextPage || hasNextPage()) {
      hasCachedNextPage = false;
      return cachedPageHeader;
    } else {
      throw new IOException("no next page header");
    }
  }

  public void close() throws IOException {

  }

}
