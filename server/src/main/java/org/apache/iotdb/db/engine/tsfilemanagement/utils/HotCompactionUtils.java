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

package org.apache.iotdb.db.engine.tsfilemanagement.utils;

import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;

import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
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

public class HotCompactionUtils {

  private static final Logger logger = LoggerFactory.getLogger(HotCompactionUtils.class);

  private HotCompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static Pair<ChunkMetadata, Chunk> writeSeqChunk(String storageGroup,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String deviceId,
      String measurementId,
      List<TsFileResource> levelResources)
      throws IOException {
    ChunkMetadata newChunkMetadata = null;
    Chunk newChunk = null;
    for (TsFileResource levelResource : levelResources) {
      TsFileSequenceReader reader = buildReaderFromTsFileResource(levelResource,
          tsFileSequenceReaderMap,
          storageGroup);
      if (reader == null || !reader.getAllDevices().contains(deviceId)) {
        continue;
      }
      List<ChunkMetadata> chunkMetadataList = reader
          .getChunkMetadataList(new Path(deviceId, measurementId));
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

  private static long readUnseqChunk(String storageGroup,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String deviceId, long maxVersion,
      String measurementId,
      Map<Long, TimeValuePair> timeValuePairMap, List<TsFileResource> levelResources)
      throws IOException {
    for (TsFileResource levelResource : levelResources) {
      TsFileSequenceReader reader = buildReaderFromTsFileResource(levelResource,
          tsFileSequenceReaderMap,
          storageGroup);
      if (reader == null) {
        continue;
      }
      List<ChunkMetadata> chunkMetadataList = reader
          .getChunkMetadataList(new Path(deviceId, measurementId));
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
      List<TsFileResource> subLevelResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String storageGroup)
      throws IOException {
    for (TsFileResource levelResource : subLevelResources) {
      TsFileSequenceReader reader = buildReaderFromTsFileResource(levelResource,
          tsFileSequenceReaderMap,
          storageGroup);
      if (reader == null) {
        continue;
      }
      List<Path> allPaths = reader.getAllPaths();
      Map<String, TSDataType> allMeasurements = reader.getAllMeasurements();
      // device, measurement -> chunk metadata list
      for (Path path : allPaths) {
        if (devices.contains(path.getDevice())) {
          continue;
        }
        Map<String, MeasurementSchema> measurementSchemaMap = deviceMeasurementMap
            .computeIfAbsent(path.getDevice(), k -> new HashMap<>());

        // measurement, chunk metadata list
        measurementSchemaMap.computeIfAbsent(path.getMeasurement(), k ->
            new MeasurementSchema(k, allMeasurements.get(path.getMeasurement())));
      }
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void merge(TsFileResource targetResource,
      List<TsFileResource> tsFileResources, String storageGroup,
      HotCompactionLogger hotCompactionLogger,
      Set<String> devices, boolean sequence) throws IOException {
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetResource.getTsFile());
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    Map<String, Map<String, MeasurementSchema>> deviceMeasurementMap = new HashMap<>();
    RateLimiter compactionRateLimiter = MergeManager.getINSTANCE().getMergeRateLimiter();
    fillDeviceMeasurementMap(devices, deviceMeasurementMap, tsFileResources,
        tsFileSequenceReaderMap, storageGroup);
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
          maxVersion = readUnseqChunk(storageGroup, tsFileSequenceReaderMap, deviceId,
              maxVersion, measurementId, timeValuePairMap, tsFileResources);
          IChunkWriter chunkWriter = new ChunkWriterImpl(entry.getValue());
          for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
            writeTVPair(timeValuePair, chunkWriter);
            targetResource.updateStartTime(deviceId, timeValuePair.getTimestamp());
            targetResource.updateEndTime(deviceId, timeValuePair.getTimestamp());
          }
          // wait for limit write
          MergeManager
              .mergeRateLimiterAcquire(compactionRateLimiter, chunkWriter.getCurrentChunkSize());
          chunkWriter.writeToFileWriter(writer);
        }
        writer.writeVersion(maxVersion);
        writer.endChunkGroup();
        if (hotCompactionLogger != null) {
          hotCompactionLogger.logDevice(deviceId, writer.getPos());
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
          Pair<ChunkMetadata, Chunk> chunkPair = writeSeqChunk(storageGroup,
              tsFileSequenceReaderMap, deviceId, measurementId, tsFileResources);
          ChunkMetadata newChunkMetadata = chunkPair.left;
          Chunk newChunk = chunkPair.right;
          if (newChunkMetadata != null && newChunk != null) {
            // wait for limit write
            MergeManager.mergeRateLimiterAcquire(compactionRateLimiter,
                (long)newChunk.getHeader().getDataSize() + newChunk.getData().position());
            writer.writeChunk(newChunk, newChunkMetadata);
            targetResource.updateStartTime(deviceId, newChunkMetadata.getStartTime());
            targetResource.updateEndTime(deviceId, newChunkMetadata.getEndTime());
          }
        }
        writer.endChunkGroup();
        if (hotCompactionLogger != null) {
          hotCompactionLogger.logDevice(deviceId, writer.getPos());
        }
      }
    }

    for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
      reader.close();
    }
    Set<Long> historicalVersions = new HashSet<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      historicalVersions.addAll(tsFileResource.getHistoricalVersions());
    }
    targetResource.setHistoricalVersions(historicalVersions);
    targetResource.serialize();
    writer.endFile();
    targetResource.close();
  }

  private static TsFileSequenceReader buildReaderFromTsFileResource(TsFileResource levelResource,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String storageGroup) {
    return tsFileSequenceReaderMap.computeIfAbsent(levelResource.getTsFile().getAbsolutePath(),
        path -> {
          try {
            if (levelResource.getTsFile().exists()) {
              return new TsFileSequenceReader(path);
            } else {
              logger.info("{} tsfile does not exist", path);
              return null;
            }
          } catch (IOException e) {
            logger.error(
                "Storage group {}, flush recover meets error. reader create failed.",
                storageGroup, e);
            return null;
          }
        });
  }
}