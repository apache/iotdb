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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.VectorChunkReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }

  // TODO pass datatype of value here if null
  public static void writeTVPair(long time, TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    if (timeValuePair == null) {
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(time, null, true);
          break;
        case DOUBLE:
        case FLOAT:
        case INT32:
          chunkWriter.write(time, 0, true);
          break;
        case BOOLEAN:
          chunkWriter.write(time, false, true);
          break;
        case INT64:
          chunkWriter.write(time, 0L, true);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      switch (timeValuePair.getValue().getDataType()) {
        case TEXT:
          chunkWriter.write(time, timeValuePair.getValue().getBinary(), false);
          break;
        case DOUBLE:
          chunkWriter.write(time, timeValuePair.getValue().getDouble(), false);
          break;
        case BOOLEAN:
          chunkWriter.write(time, timeValuePair.getValue().getBoolean(), false);
          break;
        case INT64:
          chunkWriter.write(time, timeValuePair.getValue().getLong(), false);
          break;
        case INT32:
          chunkWriter.write(time, timeValuePair.getValue().getInt(), false);
          break;
        case FLOAT:
          chunkWriter.write(time, timeValuePair.getValue().getFloat(), false);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    }
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader)
      throws IOException {
    return sequenceReader.getAllPaths();
  }

  public static long collectFileSizes(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(List<Chunk> chunk, IChunkWriter iChunkWriter)
      throws IOException {
    int ptWritten = 0;
    ChunkReader timeChunkReader = new ChunkReader(chunk.get(0), null);
    if (iChunkWriter instanceof ChunkWriterImpl) {
      ChunkWriterImpl chunkWriter = (ChunkWriterImpl) iChunkWriter;
      while (timeChunkReader.hasNextSatisfiedPage()) {
        BatchData batchData = timeChunkReader.nextPageData();
        for (int i = 0; i < batchData.length(); i++) {
          writeBatchPoint(batchData, i, chunkWriter);
        }
        ptWritten += batchData.length();
      }
    } else {
      // write by VectorChunkWriterImpl
      VectorChunkWriterImpl vectorChunkWriter = (VectorChunkWriterImpl) iChunkWriter;
      VectorChunkReader vectorChunkReader =
          new VectorChunkReader(chunk.get(0), chunk.subList(0, chunk.size()), null);
      while (vectorChunkReader.hasNextSatisfiedPage()) {
        BatchData batchData = vectorChunkReader.nextPageData();
        for (int i = 0; i < batchData.length(); i++) {
          writeBatchPoint(batchData, i, vectorChunkWriter);
        }
        ptWritten += batchData.length();
      }
    }
    return ptWritten;
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    if (chunkWriter instanceof ChunkWriterImpl) {
      switch (chunkWriter.getDataType()) {
        case TEXT:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i), false);
          break;
        case DOUBLE:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), false);
          break;
        case BOOLEAN:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i), false);
          break;
        case INT64:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i), false);
          break;
        case INT32:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i), false);
          break;
        case FLOAT:
          chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i), false);
          break;
        default:
          throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
      }
    } else {
      long time = batchData.getTimeByIndex(i);
      TsPrimitiveType[] values = batchData.getVectorByIndex(i);
      for (TsPrimitiveType value : values) {
        // TODO if value is null
        switch (value.getDataType()) {
          case TEXT:
            chunkWriter.write(time, value.getBinary(), false);
            break;
          case DOUBLE:
            chunkWriter.write(time, value.getDouble(), false);
            break;
          case BOOLEAN:
            chunkWriter.write(time, value.getBoolean(), false);
            break;
          case INT64:
            chunkWriter.write(time, value.getLong(), false);
            break;
          case INT32:
            chunkWriter.write(time, value.getInt(), false);
            break;
          case FLOAT:
            chunkWriter.write(time, value.getFloat(), false);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unknown data type " + chunkWriter.getDataType());
        }
      }
      chunkWriter.write(time);
    }
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = sequenceReader.getChunkMetadataList(path);
      totalChunkNum += chunkMetadataList.size();
      maxChunkNum = chunkMetadataList.size() > maxChunkNum ? chunkMetadataList.size() : maxChunkNum;
    }
    logger.debug(
        "In file {}, total chunk num {}, series max chunk num {}",
        tsFileResource,
        totalChunkNum,
        maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) {
    return seqFile.getTsFileSize() - sequenceReader.getFileMetadataPos();
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a file,
   * this method follows the order of positions of chunks instead of the order of timeseries, which
   * reduce disk seeks.
   *
   * @param paths names of the timeseries
   */
  public static List<List<Chunk>>[] collectUnseqChunks(
      String device,
      List<IMeasurementSchema> paths,
      List<TsFileResource> unseqResources,
      MergeResource mergeResource)
      throws IOException, IllegalPathException {
    List<List<Chunk>>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      buildMetaHeap(device, paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  private static void buildMetaHeap(
      String device,
      List<IMeasurementSchema> paths,
      TsFileSequenceReader tsFileReader,
      MergeResource resource,
      TsFileResource tsFileResource,
      PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException, IllegalPathException {
    for (int i = 0; i < paths.size(); i++) {
      IMeasurementSchema path = paths.get(i);
      List<IChunkMetadata> metaDataList = new ArrayList<>();
      if (path instanceof MeasurementSchema) {
        // pack ChunkMetadata
        metaDataList =
            new ArrayList<>(
                tsFileReader.getChunkMetadataList(
                    new PartialPath(device, path.getMeasurementId())));
      } else {
        // pack VectorChunkMetadata
        VectorMeasurementSchema vectorMeasurementSchema = (VectorMeasurementSchema) path;
        List<ChunkMetadata> timeChunkMetaList =
            tsFileReader.getChunkMetadataList(
                new PartialPath(device, vectorMeasurementSchema.getMeasurementId()));
        List<List<IChunkMetadata>> valueMetadataList = new ArrayList<>();
        for (String valueMeasurementId : vectorMeasurementSchema.getValueMeasurementIdList()) {
          List<ChunkMetadata> valueChunkMetadataList =
              tsFileReader.getChunkMetadataList(new PartialPath(device, valueMeasurementId));
          valueMetadataList.add(new ArrayList<>(valueChunkMetadataList));
        }
        for (int j = 0; j < timeChunkMetaList.size(); j++) {
          metaDataList.add(
              new VectorChunkMetadata(timeChunkMetaList.get(j), valueMetadataList.get(j)));
        }
      }

      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications =
          resource.getModifications(
              tsFileResource, new PartialPath(device, path.getMeasurementId()));
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      List<IChunkMetadata> iChunkMetadataList = new ArrayList<>(metaDataList);
      MetaListEntry entry = new MetaListEntry(i, iChunkMetadataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  private static void collectUnseqChunks(
      PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader tsFileReader,
      List<List<Chunk>>[] ret)
      throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      IChunkMetadata currMeta = metaListEntry.current();
      List<List<Chunk>> currChunkList = ret[metaListEntry.pathId];
      if (currMeta instanceof ChunkMetadata) {
        // pack Chunk
        ChunkMetadata currChunkMetadata = (ChunkMetadata) currMeta;
        Chunk chunk = tsFileReader.readMemChunk(currChunkMetadata);
        List<Chunk> chunkList;
        if (currChunkList.size() == 0) {
          chunkList = new ArrayList<>();
          currChunkList.add(chunkList);
        } else {
          chunkList = currChunkList.get(0);
        }
        chunkList.add(chunk);
      } else {
        // pack VectorChunk list
        VectorChunkMetadata currVectorChunkMetadata = (VectorChunkMetadata) currMeta;
        Chunk timeChunk = currVectorChunkMetadata.getTimeChunk();
        List<Chunk> valueChunkList = currVectorChunkMetadata.getValueChunkList();
        if (currChunkList.size() == 0) {
          for (int i = 0; i < 1 + valueChunkList.size(); i++) {
            currChunkList.add(new ArrayList<>());
          }
        }
        currChunkList.get(0).add(timeChunk);
        for (int i = 0; i < valueChunkList.size(); i++) {
          currChunkList.get(i + 1).add(valueChunkList.get(i));
        }
      }
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }

  public static boolean isChunkOverflowed(
      List<TimeValuePair> timeValuePair, IChunkMetadata metaData) {
    return timeValuePair != null && timeValuePair.get(0).getTimestamp() <= metaData.getEndTime();
  }

  public static boolean isChunkTooSmall(
      int ptWritten, IChunkMetadata chunkMetaData, boolean isLastChunk, int minChunkPointNum) {
    long numOfPoints = chunkMetaData.getStatistics().getCount();
    return ptWritten > 0
        || (minChunkPointNum >= 0 && numOfPoints < minChunkPointNum && !isLastChunk);
  }

  public static class MetaListEntry implements Comparable<MetaListEntry> {

    private int pathId;
    private int listIdx;
    private List<IChunkMetadata> chunkMetadataList;

    public MetaListEntry(int pathId, List<IChunkMetadata> chunkMetadataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetadataList = chunkMetadataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(
          this.current().getOffsetOfChunkHeader(), o.current().getOffsetOfChunkHeader());
    }

    public IChunkMetadata current() {
      return chunkMetadataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetadataList.size();
    }

    public IChunkMetadata next() {
      return chunkMetadataList.get(++listIdx);
    }

    public int getPathId() {
      return pathId;
    }
  }
}
