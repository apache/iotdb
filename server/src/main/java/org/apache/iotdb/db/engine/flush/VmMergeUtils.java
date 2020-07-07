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

import static org.apache.iotdb.db.utils.MergeUtils.writeTimeValuePair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
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

public class VmMergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(VmMergeUtils.class);

  public static void fullMerge(RestorableTsFileIOWriter writer,
      List<RestorableTsFileIOWriter> vmWriters, String storageGroup, VmLogger vmLogger,
      Set<String> devices, boolean sequence) throws IOException {
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    Map<String, Map<String, MeasurementSchema>> deviceMeasurementMap = new HashMap<>();

    for (RestorableTsFileIOWriter vmWriter : vmWriters) {
      vmWriter.makeMetadataVisible();
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
      for (Entry<String, Map<String, MeasurementSchema>> deviceMeasurementEntry : deviceMeasurementMap
          .entrySet()) {
        String deviceId = deviceMeasurementEntry.getKey();
        writer.startChunkGroup(deviceId);
        long maxVersion = Long.MIN_VALUE;
        for (Entry<String, MeasurementSchema> entry : deviceMeasurementEntry.getValue()
            .entrySet()) {
          String measurementId = entry.getKey();
          Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
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
            List<ChunkMetadata> chunkMetadataList = vmWriter.getVisibleMetadataList(deviceId,
                measurementId, entry.getValue().getType());
            for (ChunkMetadata chunkMetadata : chunkMetadataList) {
              maxVersion = Math.max(chunkMetadata.getVersion(), maxVersion);
              IChunkReader chunkReader = new ChunkReaderByTimestamp(
                  reader.readMemChunk(chunkMetadata));
              while (chunkReader.hasNextSatisfiedPage()) {
                IPointReader iPointReader = new BatchDataIterator(
                    chunkReader.nextPageData());
                while (iPointReader.hasNextTimeValuePair()) {
                  TimeValuePair timeValuePair = iPointReader.nextTimeValuePair();
                  timeValuePairMap.put(timeValuePair.getTimestamp(), timeValuePair);
                }
              }
            }
          }
          IChunkWriter chunkWriter = new ChunkWriterImpl(entry.getValue());
          for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
            writeTimeValuePair(timeValuePair, chunkWriter);
          }
          chunkWriter.writeToFileWriter(writer);
        }
        writer.writeVersion(maxVersion);
        writer.endChunkGroup();
        if (vmLogger != null) {
          vmLogger.logDevice(deviceId, writer.getPos());
        }
      }
    } else {
      for (Entry<String, Map<String, MeasurementSchema>> deviceMeasurementEntry : deviceMeasurementMap
          .entrySet()) {
        String deviceId = deviceMeasurementEntry.getKey();
        writer.startChunkGroup(deviceId);
        for (Entry<String, MeasurementSchema> entry : deviceMeasurementEntry.getValue()
            .entrySet()) {
          String measurementId = entry.getKey();
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

    for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
      reader.close();
    }
  }
}