package org.apache.iotdb.db.engine.compaction.performer.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.inner.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.CrossSpaceCompactionWriter;
import org.apache.iotdb.db.engine.compaction.writer.InnerSpaceCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.reader.resource.CachedUnseqResourceMergeReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class FastCompactionPerformer implements ICrossCompactionPerformer {
  private Logger LOGGER = LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  private List<TsFileResource> seqFiles = Collections.emptyList();

  private List<TsFileResource> unseqFiles = Collections.emptyList();

  private static final int subTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getSubCompactionTaskNum();

  private final Map<TsFileResource, TsFileSequenceReader> readerCacheMap = new HashMap<>();

  private CompactionTaskSummary summary;

  private List<TsFileResource> targetFiles = Collections.emptyList();

  private final Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();

  private final Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManager.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  @Override
  public void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException {
    try (AbstractCompactionWriter compactionWriter =
        getCompactionWriter(seqFiles, unseqFiles, targetFiles)) {
      MultiTsFileDeviceIterator deviceIterator =
          new MultiTsFileDeviceIterator(seqFiles, unseqFiles);
      while (deviceIterator.hasNextDevice()) {
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean isAligned = deviceInfo.right;
        compactionWriter.startChunkGroup(device, isAligned);
        getAllChunkMetadataByDevice(device);
      }
    }
  }

  private void getAllChunkMetadataByDevice(String deviceID) {
    for (TsFileResource resource : seqFiles) {
      TsFileSequenceReader seqReader =
          readerCacheMap.computeIfAbsent(
              resource,
              tsFileResource -> {
                try {
                  return new TsFileSequenceReader(tsFileResource.getTsFilePath());
                } catch (IOException e) {
                  throw new RuntimeException(
                      String.format("Failed to construct sequence reader for %s", resource));
                }
              });
      Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
          measurementChunkMetadataListMapIteratorCache.computeIfAbsent(
              seqReader,
              (tsFileSequenceReader -> {
                try {
                  return tsFileSequenceReader.getMeasurementChunkMetadataListMapIterator(deviceID);
                } catch (IOException e) {
                  throw new RuntimeException(
                      "GetMeasurementChunkMetadataListMapIterator meets error. iterator create failed.");
                }
              }));
    }
  }

  private void compactAlignedSeries() {}

  private void compactNonAlignedSeries() {

  }

  private AbstractCompactionWriter getCompactionWriter(
      List<TsFileResource> seqFileResources,
      List<TsFileResource> unseqFileResources,
      List<TsFileResource> targetFileResources)
      throws IOException {
    if (!seqFileResources.isEmpty() && !unseqFileResources.isEmpty()) {
      // cross space
      return new CrossSpaceCompactionWriter(targetFileResources, seqFileResources);
    } else {
      // inner space
      return new InnerSpaceCompactionWriter(targetFileResources.get(0));
    }
  }

  @Override
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetFiles = targetFiles;
  }

  @Override
  public void setSummary(CompactionTaskSummary summary) {
    this.summary = summary;
  }

  @Override
  public void setSourceFiles(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  public IPointReader[] getUnseqReaders(List<PartialPath> paths)
          throws IOException, MetadataException {
    // 将所有待合并序列在所有乱序文件里的所有Chunk依次放入ret列表数组里（ret数组长度为待合并序列数量），如有s0 s1 s2,其中s2在第1 3 5个乱序文件里都有好几个Chunk，则在ret[2]列表里存放该序列分别在1 3 5乱序文件的所有Chunk
    List<Chunk>[] pathChunks = collectUnseqChunks(paths, unseqFiles);
    // 创建每个待合并序列的乱序数据点Reader,将某待合并序列在所有乱序文件的所有Chunk的第一个数据点放入heap优先级队列里（越后面的Chunk说明数据越新，因此优先级越高）
    IPointReader[] ret = new IPointReader[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = IoTDB.metaManager.getSeriesType(paths.get(i));
      ret[i] = new CachedUnseqResourceMergeReader(pathChunks[i], dataType);
    }
    return ret;
  }

  public List<Chunk>[] collectUnseqChunks(
          List<PartialPath> paths, List<TsFileResource> unseqResources)
          throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader unseqReader = readerCacheMap.computeIfAbsent(
              tsFileResource,
              resource -> {
                try {
                  return new TsFileSequenceReader(resource.getTsFilePath());
                } catch (IOException e) {
                  throw new RuntimeException(
                          String.format("Failed to construct sequence reader for %s", resource));
                }
              });
      // prepare metaDataList
      //将所有待合并序列在当前乱序文件里的ChunkMetadataList依次放入chunkMetaHeap队列，该队列元素为（待合并序列index,该序列在该乱序文件里的ChunkMetadataList）
      buildMetaHeap(paths, unseqReader, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      //将所有待合并序列在该乱序文件里的所有Chunk依次放入ret里，ret长度为待合并序列数量，每个元素存放该序列在该乱序文件里的所有Chunk（包含删除区间）
      // read all chunks of timeseries in the unseq file in order
      while (!chunkMetaHeap.isEmpty()) {
        MetaListEntry metaListEntry = chunkMetaHeap.poll();
        ChunkMetadata currMeta = metaListEntry.current();
        Chunk chunk = unseqReader.readMemChunk(currMeta);
        ret[metaListEntry.pathId].add(chunk);
        if (metaListEntry.hasNext()) {
          metaListEntry.next();
          chunkMetaHeap.add(metaListEntry);
        }
      }
    }
    return ret;
  }

  private void buildMetaHeap(
          List<PartialPath> paths,
          TsFileSequenceReader tsFileReader,
          TsFileResource tsFileResource,
          PriorityQueue<MetaListEntry> chunkMetaHeap)
          throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      List<ChunkMetadata> metaDataList = tsFileReader.getChunkMetadataList(path, true);
      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications = getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  public List<Modification> getModifications(TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
            modificationCache.computeIfAbsent(
                    tsFileResource, resource -> new ArrayList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  /**
   * MetaListEntry stores all chunkmetadatas of a timeseries in an unseq file.
   */
  public static class MetaListEntry implements Comparable<MetaListEntry> {

    private int pathId;
    private int listIdx;
    private List<ChunkMetadata> chunkMetadataList;

    public MetaListEntry(int pathId, List<ChunkMetadata> chunkMetadataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetadataList = chunkMetadataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(
              this.current().getOffsetOfChunkHeader(), o.current().getOffsetOfChunkHeader());
    }

    public ChunkMetadata current() {
      return chunkMetadataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetadataList.size();
    }

    public ChunkMetadata next() {
      return chunkMetadataList.get(++listIdx);
    }

    public int getPathId() {
      return pathId;
    }
  }

}
