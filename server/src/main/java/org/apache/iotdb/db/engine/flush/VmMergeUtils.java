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

import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;

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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmMergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(VmMergeUtils.class);

  private VmMergeUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static Pair<ChunkMetadata, Chunk> writeSeqChunk(RestorableTsFileIOWriter writer,
      String storageGroup,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String deviceId,
      String measurementId,
      List<RestorableTsFileIOWriter> vmWriters, ChunkMetadata lastChunkMetadata, Chunk lastChunk)
      throws IOException {
    ChunkMetadata newChunkMetadata = lastChunkMetadata;
    Chunk newChunk = lastChunk;
    for (RestorableTsFileIOWriter vmWriter : vmWriters) {
      TsFileSequenceReader reader = buildReaderFromVmWriter(vmWriter,
          writer, tsFileSequenceReaderMap, storageGroup);
      if (reader == null || !vmWriter.getMetadatasForQuery().containsKey(deviceId)) {
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
    return new Pair<>(newChunkMetadata, newChunk);
  }

  private static long writeUnseqChunk(RestorableTsFileIOWriter writer, String storageGroup,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String deviceId, long maxVersion,
      Entry<String, MeasurementSchema> entry, String measurementId,
      Map<Long, TimeValuePair> timeValuePairMap, List<RestorableTsFileIOWriter> vmWriters)
      throws IOException {
    for (RestorableTsFileIOWriter vmWriter : vmWriters) {
      TsFileSequenceReader reader = buildReaderFromVmWriter(vmWriter,
          writer, tsFileSequenceReaderMap, storageGroup);
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
    return maxVersion;
  }

  private static void fillDeviceMeasurementMap(Set<String> devices,
      Map<String, Map<String, MeasurementSchema>> deviceMeasurementMap,
      List<RestorableTsFileIOWriter> subVmWriters) {
    for (RestorableTsFileIOWriter vmWriter : subVmWriters) {
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
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void merge(RestorableTsFileIOWriter writer,
      List<RestorableTsFileIOWriter> vmWriters, String storageGroup, VmLogger vmLogger,
      Set<String> devices, boolean sequence) throws IOException {
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    Map<String, Map<String, MeasurementSchema>> deviceMeasurementMap = new HashMap<>();

    fillDeviceMeasurementMap(devices, deviceMeasurementMap, vmWriters);
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
          maxVersion = writeUnseqChunk(writer, storageGroup, tsFileSequenceReaderMap, deviceId,
              maxVersion, entry, measurementId, timeValuePairMap, vmWriters);
          IChunkWriter chunkWriter = new ChunkWriterImpl(entry.getValue());
          for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
            writeTVPair(timeValuePair, chunkWriter);
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
          Pair<ChunkMetadata, Chunk> chunkPair = writeSeqChunk(writer, storageGroup,
              tsFileSequenceReaderMap, deviceId, measurementId, vmWriters, null, null);
          ChunkMetadata newChunkMetadata = chunkPair.left;
          Chunk newChunk = chunkPair.right;
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
      logger.debug("{} vm file close a reader", reader.getFileName());
    }
  }

  private static TsFileSequenceReader buildReaderFromVmWriter(RestorableTsFileIOWriter vmWriter,
      RestorableTsFileIOWriter writer, Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup) {
    return tsFileSequenceReaderMap.computeIfAbsent(vmWriter.getFile().getAbsolutePath(),
        path -> {
          try {
            if (vmWriter.getFile().exists()) {
              logger.debug("{} vm file create a reader", path);
              return new TsFileSequenceReader(path);
            } else {
              logger.info("{} vm file does not exist", path);
              return null;
            }
          } catch (IOException e) {
            logger.error(
                "Storage group {} tsfile {}, flush recover meets error. reader create failed.",
                storageGroup, writer.getFile().getName(), e);
            return null;
          }
        });
  }
}