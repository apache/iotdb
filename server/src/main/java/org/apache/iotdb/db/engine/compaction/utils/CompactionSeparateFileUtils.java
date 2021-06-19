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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.createNewTsFileNameByOffset;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;

public class CompactionSeparateFileUtils {

  private static final Logger logger = LoggerFactory.getLogger(CompactionSeparateFileUtils.class);
  private static final int AVG_UNSEQ_SERIES_POINT_NUMBER =
      IoTDBDescriptor.getInstance().getConfig().getAvgUnseqSeriesPointNumberThreshold();

  private CompactionSeparateFileUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static void readByDeserializeMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap,
      Map<Long, TimeValuePair> timeValuePairMap)
      throws IOException {
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        IChunkReader chunkReader = new ChunkReaderByTimestamp(reader.readMemChunk(chunkMetadata));
        while (chunkReader.hasNextSatisfiedPage()) {
          IPointReader iPointReader = new BatchDataIterator(chunkReader.nextPageData());
          while (iPointReader.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = iPointReader.nextTimeValuePair();
            timeValuePairMap.put(timeValuePair.getTimestamp(), timeValuePair);
          }
        }
      }
    }
  }

  private static Set<String> getTsFileDevicesSet(
      List<TsFileResource> subLevelResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup)
      throws IOException {
    Set<String> tsFileDevicesSet = new HashSet<>();
    for (TsFileResource levelResource : subLevelResources) {
      TsFileSequenceReader reader =
          buildReaderFromTsFileResource(levelResource, tsFileSequenceReaderMap, storageGroup);
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
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static List<TsFileResource> mergeWithFileSeparate(
      TsFileResource targetResource, List<TsFileResource> tsFileResources, String storageGroup)
      throws IOException {
    List<TsFileResource> returnTsFileResources = new ArrayList<>();
    returnTsFileResources.add(targetResource);
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    RateLimiter compactionWriteRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    Set<String> tsFileDevicesMap =
        getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap, storageGroup);
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetResource.getTsFile());

    for (String device : tsFileDevicesMap) {
      writer.startChunkGroup(device);
      Map<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> sensorReaderCmlMap =
          new HashMap<>();
      for (TsFileResource tsFileResource : tsFileResources) {
        TsFileSequenceReader reader =
            tsFileSequenceReaderMap.get(tsFileResource.getTsFile().getAbsolutePath());
        Map<String, List<ChunkMetadata>> sensorChunkMetaListMap =
            reader.readChunkMetadataInDevice(device);
        for (Entry<String, List<ChunkMetadata>> sensorCmlEntry :
            sensorChunkMetaListMap.entrySet()) {
          Map<TsFileSequenceReader, List<ChunkMetadata>> readerCmlMap =
              sensorReaderCmlMap.computeIfAbsent(sensorCmlEntry.getKey(), (k) -> new HashMap<>());
          readerCmlMap.put(reader, sensorCmlEntry.getValue());
        }
      }

      for (Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> sensorReaderCmlEntry :
          sensorReaderCmlMap.entrySet()) {
        String sensor = sensorReaderCmlEntry.getKey();
        Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
        Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap =
            sensorReaderCmlEntry.getValue();
        readByDeserializeMerge(readerChunkMetadataMap, timeValuePairMap);
        IChunkWriter chunkWriter;
        MeasurementSchema measurementSchema;
        try {
          measurementSchema = IoTDB.metaManager.getSeriesSchema(new PartialPath(device), sensor);
        } catch (MetadataException e) {
          logger.error("{} get schema {} error, skip this sensor", device, sensor, e);
          continue;
        }
        chunkWriter = new ChunkWriterImpl(measurementSchema, true);

        long currentWrittenPointNum = 0;
        for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
          writeTVPair(timeValuePair, chunkWriter);
          returnTsFileResources
              .get(returnTsFileResources.size() - 1)
              .updateStartTime(device, timeValuePair.getTimestamp());
          returnTsFileResources
              .get(returnTsFileResources.size() - 1)
              .updateEndTime(device, timeValuePair.getTimestamp());

          currentWrittenPointNum++;
          if (currentWrittenPointNum >= AVG_UNSEQ_SERIES_POINT_NUMBER) {
            chunkWriter.writeToFileWriter(writer);
            writer.endChunkGroup();
            for (TsFileResource tsFileResource : tsFileResources) {
              returnTsFileResources
                  .get(returnTsFileResources.size() - 1)
                  .updatePlanIndexes(tsFileResource);
            }
            returnTsFileResources.get(returnTsFileResources.size() - 1).serialize();
            writer.endFile();
            returnTsFileResources.get(returnTsFileResources.size() - 1).close();
            writer = buildNewWriter(returnTsFileResources);
            writer.startChunkGroup(device);
            chunkWriter = new ChunkWriterImpl(measurementSchema, true);
            currentWrittenPointNum = 0;
          }
        }
        // wait for limit write
        MergeManager.mergeRateLimiterAcquire(
            compactionWriteRateLimiter, chunkWriter.getCurrentChunkSize());
        chunkWriter.writeToFileWriter(writer);
      }

      writer.endChunkGroup();
    }

    for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
      reader.close();
    }

    for (TsFileResource tsFileResource : tsFileResources) {
      returnTsFileResources.get(returnTsFileResources.size() - 1).updatePlanIndexes(tsFileResource);
    }
    returnTsFileResources.get(returnTsFileResources.size() - 1).serialize();
    writer.endFile();
    returnTsFileResources.get(returnTsFileResources.size() - 1).close();

    return returnTsFileResources;
  }

  private static RestorableTsFileIOWriter buildNewWriter(List<TsFileResource> tsFileResources)
      throws IOException {
    TsFileResource tsFileResource = tsFileResources.get(0);
    File newFile = createNewTsFileNameByOffset(tsFileResource.getTsFile(), tsFileResources.size());
    TsFileResource newResource = new TsFileResource(newFile);
    tsFileResources.add(newResource);
    return new RestorableTsFileIOWriter(newFile);
  }

  private static TsFileSequenceReader buildReaderFromTsFileResource(
      TsFileResource levelResource,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup) {
    return tsFileSequenceReaderMap.computeIfAbsent(
        levelResource.getTsFile().getAbsolutePath(),
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
                storageGroup,
                e);
            return null;
          }
        });
  }
}
