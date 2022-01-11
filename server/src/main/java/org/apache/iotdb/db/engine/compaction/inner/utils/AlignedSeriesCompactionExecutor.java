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

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReaderByTimestamp;
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
  private String device;
  private List<TsFileResource> tsFileResources;
  private LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      readerAndChunkMetadataList;
  private TsFileResource targetResource;
  private TsFileIOWriter writer;

  private AlignedChunkWriterImpl chunkWriter;
  private List<IMeasurementSchema> iSchemaList = new ArrayList<>();
  private Map<String, Integer> measurementIndexMap = new HashMap<>();

  public AlignedSeriesCompactionExecutor(
      String device,
      List<TsFileResource> tsFileResources,
      TsFileResource targetResource,
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList,
      TsFileIOWriter writer)
      throws IllegalPathException, PathNotExistException {
    this.device = device;
    this.tsFileResources = tsFileResources;
    this.targetResource = targetResource;
    this.readerAndChunkMetadataList = readerAndChunkMetadataList;
    this.writer = writer;
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
        IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
        List<IChunkMetadata> valueChunkMetadataList =
            alignedChunkMetadata.getValueChunkMetadataList();

        Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
        Chunk[] valueChunks = new Chunk[valueChunkMetadataList.size()];
        // TODO: if some chunk doesn't exist, fill it with null
        for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
          valueChunks[measurementIndexMap.get(valueChunkMetadata.getMeasurementUid())] =
              reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
        }

        AlignedChunkReaderByTimestamp chunkReader =
            new AlignedChunkReaderByTimestamp(timeChunk, Arrays.asList(valueChunks));

        while (chunkReader.hasNextSatisfiedPage()) {
          IBatchDataIterator batchDataIterator = chunkReader.nextPageData().getBatchDataIterator();
          while (batchDataIterator.hasNext()) {
            TsPrimitiveType[] pointsData = (TsPrimitiveType[]) batchDataIterator.currentValue();
            long time = batchDataIterator.currentTime();
            for (TsPrimitiveType pointData : pointsData) {
              switch (pointData.getDataType()) {
                case TEXT:
                  chunkWriter.write(time, pointData.getBinary(), false);
                  break;
                case FLOAT:
                  chunkWriter.write(time, pointData.getFloat(), false);
                  break;
                case DOUBLE:
                  chunkWriter.write(time, pointData.getDouble(), false);
                  break;
                case INT32:
                  chunkWriter.write(time, pointData.getInt(), false);
                  break;
                case INT64:
                  chunkWriter.write(time, pointData.getLong(), false);
                  break;
                case BOOLEAN:
                  chunkWriter.write(time, pointData.getBoolean(), false);
                  break;
              }
            }

            chunkWriter.write(time);
            targetResource.updateStartTime(device, time);
            targetResource.updateEndTime(device, time);
            batchDataIterator.next();
          }
        }
      }
    }

    chunkWriter.writeToFileWriter(writer);
  }
}
