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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

  private static Pair<ChunkMetadata, Chunk> readSeqChunk(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap) throws IOException {
    ChunkMetadata newChunkMetadata = null;
    Chunk newChunk = null;
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry : readerChunkMetadataMap
        .entrySet()) {
      for (ChunkMetadata chunkMetadata : entry.getValue()) {
        Chunk chunk = entry.getKey().readMemChunk(chunkMetadata);
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

  private static long readUnseqChunk(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap, long maxVersion,
      Map<Long, TimeValuePair> timeValuePairMap)
      throws IOException {
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry : readerChunkMetadataMap
        .entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
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

  private static Set<String> getTsFileDevicesSet(List<TsFileResource> subLevelResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap, String storageGroup)
      throws IOException {
    Set<String> tsFileDevicesSet = new HashSet<>();
    for (TsFileResource levelResource : subLevelResources) {
      TsFileSequenceReader reader = buildReaderFromTsFileResource(levelResource,
          tsFileSequenceReaderMap,
          storageGroup);
      if (reader == null) {
        continue;
      }
      tsFileDevicesSet.addAll(reader.getAllDevices());
    }
    return tsFileDevicesSet;
  }

  /**
   * @param targetResource the target resource to be merged to
   * @param tsFileResources the source resource to be merged
   * @param storageGroup the storage group name
   * @param hotCompactionLogger the logger
   * @param devices the devices to be skipped(used by recover)
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void merge(TsFileResource targetResource,
      List<TsFileResource> tsFileResources, String storageGroup,
      HotCompactionLogger hotCompactionLogger,
      Set<String> devices, boolean sequence) throws IOException {
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetResource.getTsFile());
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    RateLimiter compactionRateLimiter = MergeManager.getINSTANCE().getMergeRateLimiter();
    Set<String> tsFileDevicesMap = getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap,
        storageGroup);
    for (String device : tsFileDevicesMap) {
      if (devices.contains(device)) {
        continue;
      }
      writer.startChunkGroup(device);
      // sort chunkMeta by measurement
      Map<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> measurementChunkMetadataMap = new HashMap<>();
      for (TsFileResource levelResource : tsFileResources) {
        TsFileSequenceReader reader = buildReaderFromTsFileResource(levelResource,
            tsFileSequenceReaderMap, storageGroup);
        Map<String, List<ChunkMetadata>> chunkMetadataMap = reader
            .readChunkMetadataInDevice(device);
        for (Entry<String, List<ChunkMetadata>> entry : chunkMetadataMap.entrySet()) {
          for (ChunkMetadata chunkMetadata : entry.getValue()) {
            Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap;
            String measurementUid = chunkMetadata.getMeasurementUid();
            if (measurementChunkMetadataMap.containsKey(measurementUid)) {
              readerChunkMetadataMap = measurementChunkMetadataMap.get(measurementUid);
            } else {
              readerChunkMetadataMap = new LinkedHashMap<>();
            }
            List<ChunkMetadata> chunkMetadataList;
            if (readerChunkMetadataMap.containsKey(reader)) {
              chunkMetadataList = readerChunkMetadataMap.get(reader);
            } else {
              chunkMetadataList = new ArrayList<>();
            }
            chunkMetadataList.add(chunkMetadata);
            readerChunkMetadataMap.put(reader, chunkMetadataList);
            measurementChunkMetadataMap
                .put(chunkMetadata.getMeasurementUid(), readerChunkMetadataMap);
          }
        }
      }
      if (!sequence) {
        long maxVersion = Long.MIN_VALUE;
        for (Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry : measurementChunkMetadataMap
            .entrySet()) {
          Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
          maxVersion = readUnseqChunk(entry.getValue(), maxVersion, timeValuePairMap);
          Iterator<List<ChunkMetadata>> chunkMetadataListIterator = entry.getValue().values()
              .iterator();
          if (!chunkMetadataListIterator.hasNext()) {
            continue;
          }
          List<ChunkMetadata> chunkMetadataList = chunkMetadataListIterator.next();
          if (chunkMetadataList.size() <= 0) {
            continue;
          }
          IChunkWriter chunkWriter = new ChunkWriterImpl(
              new MeasurementSchema(entry.getKey(), chunkMetadataList.get(0).getDataType()));
          for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
            writeTVPair(timeValuePair, chunkWriter);
            targetResource.updateStartTime(device, timeValuePair.getTimestamp());
            targetResource.updateEndTime(device, timeValuePair.getTimestamp());
          }
          // wait for limit write
          MergeManager
              .mergeRateLimiterAcquire(compactionRateLimiter, chunkWriter.getCurrentChunkSize());
          chunkWriter.writeToFileWriter(writer);
          if (hotCompactionLogger != null) {
            hotCompactionLogger.logDevice(device, writer.getPos());
          }
        }
        writer.endChunkGroup();
        writer.writeVersion(maxVersion);
      } else {
        for (Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry : measurementChunkMetadataMap
            .entrySet()) {
          Pair<ChunkMetadata, Chunk> chunkPair = readSeqChunk(
              entry.getValue());
          ChunkMetadata newChunkMetadata = chunkPair.left;
          Chunk newChunk = chunkPair.right;
          if (newChunkMetadata != null && newChunk != null) {
            // wait for limit write
            MergeManager.mergeRateLimiterAcquire(compactionRateLimiter,
                newChunk.getHeader().getDataSize() + newChunk.getData().position());
            writer.writeChunk(newChunk, newChunkMetadata);
            targetResource.updateStartTime(device, newChunkMetadata.getStartTime());
            targetResource.updateEndTime(device, newChunkMetadata.getEndTime());
          }
        }
        writer.endChunkGroup();
        if (hotCompactionLogger != null) {
          hotCompactionLogger.logDevice(device, writer.getPos());
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