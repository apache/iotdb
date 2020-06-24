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
package org.apache.iotdb.db.engine.flush;

import static org.apache.iotdb.db.conf.IoTDBConstant.UNSEQUENCE_FLODER_NAME;
import static org.apache.iotdb.db.utils.MergeUtils.writeTimeValuePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmMergeTask {

  private static final Logger logger = LoggerFactory.getLogger(VmMergeTask.class);

  private final RestorableTsFileIOWriter writer;
  private List<RestorableTsFileIOWriter> vmWriters;
  private Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
  private VmLogger vmLogger;
  private Set<String> devices;
  private boolean sequence;

  private String storageGroup;

  public VmMergeTask(RestorableTsFileIOWriter writer,
      List<RestorableTsFileIOWriter> vmWriters, String storageGroup, VmLogger vmLogger,
      Set<String> devices, boolean sequence) {
    this.writer = writer;
    this.vmWriters = vmWriters;
    this.storageGroup = storageGroup;
    this.vmLogger = vmLogger;
    this.devices = devices;
    this.sequence = sequence;
  }

  public void fullMerge() throws IOException {
    Map<String, Map<String, MeasurementSchema>> deviceMeasurementMap = new HashMap<>();

    for (RestorableTsFileIOWriter vmWriter : vmWriters) {
      Map<String, Map<String, List<ChunkMetadata>>> deviceMeasurementChunkMetadataMap = vmWriter
          .getMetadatasForQuery();
      // device, measurement -> chunk metadata list
      for (Entry<String, Map<String, List<ChunkMetadata>>> deviceEntry :
          deviceMeasurementChunkMetadataMap.entrySet()) {
        if (devices.contains(deviceEntry.getKey())) {
          continue;
        }
        Map<String, MeasurementSchema> measurementSchemaMap = deviceMeasurementMap
            .computeIfAbsent(deviceEntry.getKey(), k -> new HashMap<>());

        // measurement, chunk metadata list
        for (Entry<String, List<ChunkMetadata>> measurementEntry : deviceEntry.getValue()
            .entrySet()) {
          measurementSchemaMap.computeIfAbsent(measurementEntry.getKey(), k ->
              new MeasurementSchema(k, measurementEntry.getValue().get(0).getDataType()));
        }
      }
    }
    if (!sequence) {
      for (String deviceId : deviceMeasurementMap.keySet()) {
        writer.startChunkGroup(deviceId);
        for (String measurementId : deviceMeasurementMap.get(deviceId).keySet()) {
          MeasurementSchema measurementSchema = deviceMeasurementMap.get(deviceId)
              .get(measurementId);
          List<TimeValuePair> timeValuePairs = new ArrayList<>();
          for (RestorableTsFileIOWriter vmWriter : vmWriters) {
            TsFileSequenceReader reader = tsFileSequenceReaderMap
                .computeIfAbsent(vmWriter.getFile().getAbsolutePath(),
                    path -> {
                      try {
                        return new TsFileSequenceReader(path);
                      } catch (IOException e) {
                        logger.error(
                            "Storage group {} tsfile {}, flush recover meets error. reader create failed.",
                            storageGroup,
                            writer.getFile().getName(), e);
                        return null;
                      }
                    });
            if (reader == null) {
              continue;
            }
            List<ChunkMetadata> chunkMetadataList = vmWriter
                .getVisibleMetadataList(deviceId, measurementId,
                    measurementSchema.getType());
            for (ChunkMetadata chunkMetadata : chunkMetadataList) {
              IChunkReader chunkReader = new ChunkReaderByTimestamp(
                  reader.readMemChunk(chunkMetadata));
              while (chunkReader.hasNextSatisfiedPage()) {
                IPointReader iPointReader = new BatchDataIterator(
                    chunkReader.nextPageData());
                while (iPointReader.hasNextTimeValuePair()) {
                  timeValuePairs.add(iPointReader.nextTimeValuePair());
                }
              }
            }
          }
          timeValuePairs.sort((o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
          IChunkWriter chunkWriter = new ChunkWriterImpl(
              deviceMeasurementMap.get(deviceId).get(measurementId));
          for (TimeValuePair timeValuePair : timeValuePairs) {
            writeTimeValuePair(timeValuePair, chunkWriter);
          }
          chunkWriter.writeToFileWriter(writer);
        }
        writer.endChunkGroup();
        if (vmLogger != null) {
          vmLogger.logDevice(deviceId, writer.getPos());
        }
      }
    } else {
      for (String deviceId : deviceMeasurementMap.keySet()) {
        writer.startChunkGroup(deviceId);
        for (String measurementId : deviceMeasurementMap.get(deviceId).keySet()) {
          ChunkMetadata newChunkMetadata = null;
          Chunk newChunk = null;
          for (RestorableTsFileIOWriter vmWriter : vmWriters) {
            TsFileSequenceReader reader = tsFileSequenceReaderMap
                .computeIfAbsent(vmWriter.getFile().getAbsolutePath(),
                    path -> {
                      try {
                        return new TsFileSequenceReader(path);
                      } catch (IOException e) {
                        logger.error(
                            "Storage group {} tsfile {}, flush recover meets error. reader create failed.",
                            storageGroup,
                            writer.getFile().getName(), e);
                        return null;
                      }
                    });
            if (reader == null) {
              continue;
            }
            List<ChunkMetadata> chunkMetadataList = vmWriter.getMetadatasForQuery()
                .get(deviceId).get(measurementId);
            if (chunkMetadataList == null) {
              continue;
            }
            for (ChunkMetadata chunkMetadata : chunkMetadataList) {
              Chunk chunk = reader.readMemChunk(chunkMetadata);
              if (newChunkMetadata == null) {
                newChunkMetadata = chunkMetadata;
                newChunk = chunk;
              } else {
                newChunkMetadata.mergeChunkMetadata(chunkMetadata);
                newChunk.mergeChunk(chunk);
              }
            }
          }
          if (newChunkMetadata != null && newChunk != null) {
            writer.writeChunk(newChunk, newChunkMetadata);
          }
        }
        writer.endChunkGroup();
        if (vmLogger != null) {
          vmLogger.logDevice(deviceId, writer.getPos());
        }
      }
    }
  }
}