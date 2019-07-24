/**
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

package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }
  
  public static void writeTVPair(TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case DOUBLE:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      case BOOLEAN:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i));
        break;
      case DOUBLE:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i));
        break;
      case BOOLEAN:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i));
        break;
      case INT64:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i));
        break;
      case INT32:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i));
        break;
      case FLOAT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i));
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader) throws IOException {
    TsFileMetaData metaData = sequenceReader.readFileMetadata();
    Set<String> deviceIds = metaData.getDeviceMap().keySet();
    Set<String> measurements = metaData.getMeasurementSchema().keySet();
    List<Path> paths = new ArrayList<>();
    for (String deviceId : deviceIds) {
      for (String measurement : measurements) {
        paths.add(new Path(deviceId, measurement));
      }
    }
    return paths;
  }

  /**
   * Collect all paths contained in the all SeqFiles and UnseqFiles in a merge and sort them
   * before return.
   * @param resource
   * @return all paths contained in the merge.
   * @throws IOException
   */
  public static List<Path> collectPaths(MergeResource resource)
      throws IOException {
    Set<Path> pathSet = new HashSet<>();
    for (TsFileResource tsFileResource : resource.getUnseqFiles()) {
      TsFileSequenceReader sequenceReader = resource.getFileReader(tsFileResource);
      resource.getMeasurementSchemaMap().putAll(sequenceReader.readFileMetadata().getMeasurementSchema());
      pathSet.addAll(collectFileSeries(sequenceReader));
    }
    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
      TsFileSequenceReader sequenceReader = resource.getFileReader(tsFileResource);
      resource.getMeasurementSchemaMap().putAll(sequenceReader.readFileMetadata().getMeasurementSchema());
      pathSet.addAll(collectFileSeries(sequenceReader));
    }
    List<Path> ret = new ArrayList<>(pathSet);
    ret.sort(Comparator.comparing(Path::getFullPath));
    return ret;
  }

  public static long collectFileSizes(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter) throws IOException {
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    int ptWritten = 0;
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
      ptWritten += batchData.length();
    }
    return ptWritten;
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findLargestSeriesChunkNum(TsFileResource tsFileResource,
      TsFileSequenceReader sequenceReader)
      throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetaData> chunkMetaDataList = sequenceReader.getChunkMetadata(path);
      totalChunkNum += chunkMetaDataList.size();
      maxChunkNum = chunkMetaDataList.size() > maxChunkNum ? chunkMetaDataList.size() : maxChunkNum;
    }
    logger.debug("In file {}, total chunk num {}, series max chunk num {}", tsFileResource,
        totalChunkNum, maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) throws IOException {
    long minPos = Long.MAX_VALUE;
    TsFileMetaData fileMetaData = sequenceReader.readFileMetadata();
    Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
    for (TsDeviceMetadataIndex metadataIndex : deviceMap.values()) {
      minPos = metadataIndex.getOffset() < minPos ? metadataIndex.getOffset() : minPos;
    }
    return seqFile.getFileSize() - minPos;
  }

  public static List<Chunk> collectUnseqChunks(Path seriesPath, List<TsFileResource> unseqResources,
      MergeResource resource)
      throws IOException {
    List<Chunk> chunks = new ArrayList<>();
    for (TsFileResource tsFileResource : unseqResources) {
      TsFileSequenceReader tsFileReader = resource.getFileReader(tsFileResource);

      // prepare metaDataList
      List<ChunkMetaData> metaDataList = tsFileReader.getChunkMetadata(seriesPath);
      List<Modification> pathModifications =
          resource.getModifications(tsFileResource, seriesPath);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }

      // create and add ChunkReader
      for (ChunkMetaData chunkMetaData : metaDataList) {
        Chunk chunk = tsFileReader.readMemChunk(chunkMetaData);
        chunks.add(chunk);
      }
    }
    return chunks;
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a
   * file, this method follows the order of positions of chunks instead of the order of
   * timeseries, which reduce disk seeks.
   * @param paths names of the timeseries
   * @param unseqResources
   * @param resource
   * @return
   * @throws IOException
   */
  public static List<Chunk>[] collectUnseqChunks(List<Path> paths,
      List<TsFileResource> unseqResources, MergeResource resource) throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();
    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = resource.getFileReader(tsFileResource);

      // prepare metaDataList
      for (int i = 0; i < paths.size(); i++) {
        Path path = paths.get(i);
        List<ChunkMetaData> metaDataList = tsFileReader.getChunkMetadata(path);
        List<Modification> pathModifications =
            resource.getModifications(tsFileResource, path);
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
        chunkMetaHeap.add(new MetaListEntry(i, metaDataList));
      }

      // read chunks order by their position
      while (!chunkMetaHeap.isEmpty()) {
        MetaListEntry metaListEntry = chunkMetaHeap.poll();
        Chunk chunk =
            tsFileReader.readMemChunk(metaListEntry.chunkMetaDataList.get(metaListEntry.listIdx));
        ret[metaListEntry.pathId].add(chunk);
        metaListEntry.listIdx ++;
        if (metaListEntry.listIdx < metaListEntry.chunkMetaDataList.size()) {
          chunkMetaHeap.add(metaListEntry);
        }
      }
    }
    return ret;
  }

  private static class MetaListEntry implements Comparable<MetaListEntry>{
    private int pathId;
    private int listIdx;
    private List<ChunkMetaData> chunkMetaDataList;

    private MetaListEntry(int pathId, List<ChunkMetaData> chunkMetaDataList) {
      this.pathId = pathId;
      this.listIdx = 0;
      this.chunkMetaDataList = chunkMetaDataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(chunkMetaDataList.get(listIdx).getOffsetOfChunkHeader(),
          o.chunkMetaDataList.get(o.listIdx).getOffsetOfChunkHeader());
    }
  }
}
