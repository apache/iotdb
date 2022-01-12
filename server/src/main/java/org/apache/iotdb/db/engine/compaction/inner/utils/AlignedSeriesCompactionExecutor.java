/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.inner.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AlignedSeriesCompactionExecutor {
  private final String device;
  private final LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  private final TsFileResource targetResource;
  private final TsFileIOWriter writer;

  private final AlignedChunkWriterImpl chunkWriter;
  private final List<IMeasurementSchema> iSchemaList = new ArrayList<>();
  private final Map<String, Integer> measurementIndexMap = new HashMap<>();
  private long remainingPointInChunkWriter = 0L;

  private final long chunkSizeThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize();
  private final long chunkPointNumThreshold =
      IoTDBDescriptor.getInstance().getConfig().getTargetChunkPointNum();

  public AlignedSeriesCompactionExecutor(
      String device,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter writer)
      throws IllegalPathException, PathNotExistException {
    this.device = device;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
    this.targetResource = targetResource;
    List<MeasurementPath> subPaths =
        MManager.getInstance().getAllMeasurementByDevicePath(new PartialPath(device));
    for (int i = 0; i < subPaths.size(); ++i) {
      iSchemaList.add(subPaths.get(i).getMeasurementSchema());
      measurementIndexMap.put(subPaths.get(i).getMeasurement(), i);
    }
    chunkWriter = new AlignedChunkWriterImpl(iSchemaList);
  }

  public void execute() throws IOException {
    while (readerAndChunkMetadataList.size() > 0) {
      Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerListPair =
          readerAndChunkMetadataList.removeFirst();
      TsFileSequenceReader reader = readerListPair.left;
      List<AlignedChunkMetadata> alignedChunkMetadataList = readerListPair.right;

      for (AlignedChunkMetadata alignedChunkMetadata : alignedChunkMetadataList) {
        AlignedChunkReaderByTimestamp chunkReader =
            constructAlignedChunkReader(reader, alignedChunkMetadata);
        compactOneAlignedChunk(chunkReader);
      }
    }

    if (remainingPointInChunkWriter != 0L) {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  /**
   * read the time chunk and value chunks separately, then use them to construct a
   * AlignedChunkReader
   *
   * @param reader
   * @param alignedChunkMetadata
   * @return
   * @throws IOException
   */
  private AlignedChunkReaderByTimestamp constructAlignedChunkReader(
      TsFileSequenceReader reader, AlignedChunkMetadata alignedChunkMetadata) throws IOException {
    IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
    List<IChunkMetadata> valueChunkMetadataList = alignedChunkMetadata.getValueChunkMetadataList();

    Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
    Chunk[] valueChunks = new Chunk[iSchemaList.size()];
    for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
      valueChunks[measurementIndexMap.get(valueChunkMetadata.getMeasurementUid())] =
          reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
    }

    AlignedChunkReaderByTimestamp chunkReader =
        new AlignedChunkReaderByTimestamp(timeChunk, Arrays.asList(valueChunks));
    return chunkReader;
  }

  private void compactOneAlignedChunk(AlignedChunkReaderByTimestamp chunkReader)
      throws IOException {
    while (chunkReader.hasNextSatisfiedPage()) {
      IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
      while (batchDataIterator.hasNext()) {
        TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
        long time = batchDataIterator.currentTime();
        for (int i = 0; i < pointsData.length; ++i) {
          writeOnePointToChunkWriter(time, pointsData[i], iSchemaList.get(i).getType());
        }
        chunkWriter.write(time);
        ++remainingPointInChunkWriter;

        targetResource.updateStartTime(device, time);
        targetResource.updateEndTime(device, time);

        batchDataIterator.next();
      }
    }
  }

  private void writeOnePointToChunkWriter(long time, TsPrimitiveType pointData, TSDataType type) {
    switch (type) {
      case TEXT:
        chunkWriter.write(
            time,
            pointData == null ? new Binary(new byte[] {}) : pointData.getBinary(),
            pointData == null);
        break;
      case FLOAT:
        chunkWriter.write(
            time, pointData == null ? Float.MIN_VALUE : pointData.getFloat(), pointData == null);
        break;
      case DOUBLE:
        chunkWriter.write(
            time, pointData == null ? Double.MIN_VALUE : pointData.getDouble(), pointData == null);
        break;
      case INT32:
        chunkWriter.write(
            time, pointData == null ? Integer.MIN_VALUE : pointData.getInt(), pointData == null);
        break;
      case INT64:
        chunkWriter.write(
            time, pointData == null ? Long.MIN_VALUE : pointData.getLong(), pointData == null);
        break;
      case BOOLEAN:
        chunkWriter.write(
            time, pointData == null ? false : pointData.getBoolean(), pointData == null);
        break;
    }
  }

  /**
   * if the avg size of each chunk is larger than the threshold, or the chunk point num is larger
   * than the threshold, flush it
   *
   * @throws IOException
   */
  private void flushChunkWriterIfLargeEnough() throws IOException {
    if (remainingPointInChunkWriter >= chunkPointNumThreshold
        || chunkWriter.estimateMaxSeriesMemSize() >= chunkSizeThreshold * iSchemaList.size()) {
      chunkWriter.writeToFileWriter(writer);
      remainingPointInChunkWriter = 0L;
    }
  }
}
