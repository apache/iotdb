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
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
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
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.chunk.VectorChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

public class CompactionUtils {

  private static final Logger logger = LoggerFactory.getLogger(CompactionUtils.class);
  private static final int MERGE_PAGE_POINT_NUM =
      IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();

  private CompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static Pair<List<ChunkMetadata>, List<Chunk>> readByAppendMerge(
      Map<TsFileSequenceReader, List<IChunkMetadata>> readerChunkMetadataMap,
      Map<String, List<Modification>> modificationCache,
      String device,
      IMeasurementSchema iMeasurementSchema,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    List<ChunkMetadata> newChunkMetadataList = new ArrayList<>();
    List<Chunk> newChunkList = new ArrayList<>();
    for (Entry<TsFileSequenceReader, List<IChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<IChunkMetadata> chunkMetadataList = entry.getValue();
      modifyChunkMetaDataWithCache(
          reader,
          chunkMetadataList,
          modificationCache,
          new PartialPath(device, iMeasurementSchema.getMeasurementId()),
          modifications);
      for (IChunkMetadata iChunkMetadata : chunkMetadataList) {
        if (iChunkMetadata instanceof ChunkMetadata) {
          ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          if (newChunkMetadataList.size() == 0) {
            newChunkMetadataList.add(chunkMetadata);
            newChunkList.add(chunk);
          } else {
            newChunkList.get(0).mergeChunk(chunk);
            newChunkMetadataList.get(0).mergeChunkMetadata(chunkMetadata);
          }
        } else {
          // read by vector chunkMetadata, and merge by list
          VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) iChunkMetadata;
          IChunkMetadata timeChunkMetadata = vectorChunkMetadata.getTimeChunkMetadata();
          Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
          List<IChunkMetadata> valueChunkMetadataList =
              vectorChunkMetadata.getValueChunkMetadataList();
          List<Chunk> valueChunkList = new ArrayList<>();
          for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
            valueChunkList.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
          }
          if (newChunkMetadataList.size() == 0) {
            // first chunk, we do not merge
            newChunkMetadataList.add((ChunkMetadata) timeChunkMetadata);
            newChunkList.add(timeChunk);
            for (int i = 0; i < valueChunkMetadataList.size(); i++) {
              newChunkMetadataList.add((ChunkMetadata) valueChunkMetadataList.get(i));
              newChunkList.add(valueChunkList.get(i));
            }
          } else {
            // more chunk, we have to merge them with previous chunk
            newChunkList.get(0).mergeChunk(timeChunk);
            newChunkMetadataList.get(0).mergeChunkMetadata((ChunkMetadata) timeChunkMetadata);
            for (int i = 0; i < valueChunkMetadataList.size(); i++) {
              newChunkList.get(i + 1).mergeChunk(valueChunkList.get(i));
              newChunkMetadataList
                  .get(i + 1)
                  .mergeChunkMetadata((ChunkMetadata) valueChunkMetadataList.get(i));
            }
          }
        }
      }
    }
    return new Pair<>(newChunkMetadataList, newChunkList);
  }

  private static void readByDeserializeMerge(
      Map<TsFileSequenceReader, List<IChunkMetadata>> readerChunkMetadataMap,
      Map<Long, List<TimeValuePair>> timeValuePairMap,
      Map<String, List<Modification>> modificationCache,
      String device,
      IMeasurementSchema iMeasurementSchema,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    for (Entry<TsFileSequenceReader, List<IChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      modifyChunkMetaDataWithCache(
          reader,
          iChunkMetadataList,
          modificationCache,
          new PartialPath(device, iMeasurementSchema.getMeasurementId()),
          modifications);
      if (iMeasurementSchema instanceof MeasurementSchema) {
        for (IChunkMetadata iChunkMetadata : iChunkMetadataList) {
          ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
          IChunkReader chunkReader = new ChunkReaderByTimestamp(reader.readMemChunk(chunkMetadata));
          while (chunkReader.hasNextSatisfiedPage()) {
            IPointReader iPointReader = new BatchDataIterator(chunkReader.nextPageData());
            while (iPointReader.hasNextTimeValuePair()) {
              TimeValuePair timeValuePair = iPointReader.nextTimeValuePair();
              List<TimeValuePair> timeValuePairList = new ArrayList<>();
              timeValuePairList.add(timeValuePair);
              timeValuePairMap.put(timeValuePair.getTimestamp(), timeValuePairList);
            }
          }
        }
      } else {
        if (iChunkMetadataList.size() > 0) {
          // read vectorChunkMetadata by loop
          for (IChunkMetadata iChunkMetadata : iChunkMetadataList) {
            VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) iChunkMetadata;
            Chunk timeChunk =
                reader.readMemChunk((ChunkMetadata) vectorChunkMetadata.getTimeChunkMetadata());
            // prepare for value chunks
            List<Chunk> valueChunks = new ArrayList<>();
            for (IChunkMetadata valueChunkMetadata :
                vectorChunkMetadata.getValueChunkMetadataList()) {
              valueChunks.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
            }
            VectorChunkReader vectorChunkReader =
                new VectorChunkReader(timeChunk, valueChunks, null);
            while (vectorChunkReader.hasNextSatisfiedPage()) {
              BatchData batchData = vectorChunkReader.nextPageData();
              for (int i = 0; i < batchData.length(); i++) {
                long time = batchData.getTimeByIndex(i);
                TsPrimitiveType[] values = batchData.getVectorByIndex(i);
                List<TimeValuePair> timeValuePairList =
                    timeValuePairMap.computeIfAbsent(time, k -> new ArrayList<>());
                for (TsPrimitiveType value : values) {
                  if (value != null) {
                    timeValuePairList.add(new TimeValuePair(time, value));
                  } else {
                    timeValuePairList.add(null);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static void writeByAppendMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<IMeasurementSchema, Map<TsFileSequenceReader, List<IChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer,
      Map<String, List<Modification>> modificationCache,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    Pair<List<ChunkMetadata>, List<Chunk>> chunkPair =
        readByAppendMerge(
            entry.getValue(), modificationCache, device, entry.getKey(), modifications);
    List<ChunkMetadata> newChunkMetadataList = chunkPair.left;
    List<Chunk> newChunkList = chunkPair.right;
    if (newChunkMetadataList.size() > 0 && newChunkList.size() > 0) {
      for (int i = 0; i < newChunkMetadataList.size(); i++) {
        ChunkMetadata newChunkMetadata = newChunkMetadataList.get(i);
        Chunk newChunk = newChunkList.get(i);
        // wait for limit write
        MergeManager.mergeRateLimiterAcquire(
            compactionWriteRateLimiter,
            (long) newChunk.getHeader().getDataSize() + newChunk.getData().position());
        writer.writeChunk(newChunk, newChunkMetadata);
        targetResource.updateStartTime(device, newChunkMetadata.getStartTime());
        targetResource.updateEndTime(device, newChunkMetadata.getEndTime());
      }
    }
  }

  public static void writeByDeserializeMerge(
      String device,
      RateLimiter compactionRateLimiter,
      Entry<IMeasurementSchema, Map<TsFileSequenceReader, List<IChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer,
      Map<String, List<Modification>> modificationCache,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    Map<Long, List<TimeValuePair>> timeValuePairMap = new TreeMap<>();
    Map<TsFileSequenceReader, List<IChunkMetadata>> readerChunkMetadataMap = entry.getValue();
    readByDeserializeMerge(
        readerChunkMetadataMap,
        timeValuePairMap,
        modificationCache,
        device,
        entry.getKey(),
        modifications);
    boolean isChunkMetadataEmpty = true;
    for (List<IChunkMetadata> iChunkMetadataList : readerChunkMetadataMap.values()) {
      if (!iChunkMetadataList.isEmpty()) {
        isChunkMetadataEmpty = false;
        break;
      }
    }
    if (isChunkMetadataEmpty) {
      return;
    }

    IChunkWriter chunkWriter;
    if (entry.getKey() instanceof MeasurementSchema) {
      // write with chunk writer
      chunkWriter = new ChunkWriterImpl(entry.getKey());
      for (Entry<Long, List<TimeValuePair>> timeValuePair : timeValuePairMap.entrySet()) {
        writeTVPair(timeValuePair.getKey(), timeValuePair.getValue().get(0), chunkWriter);
        targetResource.updateStartTime(device, timeValuePair.getKey());
        targetResource.updateEndTime(device, timeValuePair.getKey());
      }
    } else {
      // write with vector chunk writer
      chunkWriter = new VectorChunkWriterImpl(entry.getKey());
      for (Entry<Long, List<TimeValuePair>> timeValuePairsEntry : timeValuePairMap.entrySet()) {
        long time = timeValuePairsEntry.getKey();
        for (TimeValuePair timeValuePair : timeValuePairsEntry.getValue()) {
          if (timeValuePair == null) {
            writeTVPair(time, null, chunkWriter);
          } else {
            time = timeValuePair.getTimestamp();
            writeTVPair(time, timeValuePair, chunkWriter);
          }
        }
        chunkWriter.write(time);
        targetResource.updateStartTime(device, time);
        targetResource.updateEndTime(device, time);
      }
    }
    // wait for limit write
    MergeManager.mergeRateLimiterAcquire(compactionRateLimiter, chunkWriter.getCurrentChunkSize());
    chunkWriter.writeToFileWriter(writer);
  }

  /**
   * get add devices in the tsResources and update the tsFileSequenceReaderCache cache
   *
   * @param tsResources the resources to be merged
   * @param tsFileSequenceReaderCache the cache for all tsResourceReader
   * @param storageGroup the storage group to be merged
   * @return all devices in the tsResources
   * @throws IOException
   */
  private static Set<String> getTsFileDevicesSet(
      List<TsFileResource> tsResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderCache,
      String storageGroup)
      throws IOException {
    Set<String> tsFileDevicesSet = new HashSet<>();
    for (TsFileResource tsFileResource : tsResources) {
      TsFileSequenceReader reader =
          buildReaderFromTsFileResource(tsFileResource, tsFileSequenceReaderCache, storageGroup);
      if (reader == null) {
        continue;
      }
      tsFileDevicesSet.addAll(reader.getAllDevices());
    }
    return tsFileDevicesSet;
  }

  private static boolean hasNextChunkMetadataList(
      Collection<Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>> iteratorSet) {
    for (Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>> iterator : iteratorSet) {
      if (iterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param targetResource the target resource to be merged to
   * @param tsFileResources the source resource to be merged
   * @param storageGroup the storage group name
   * @param compactionLogger the logger
   * @param devicesForSkip the devices to be skipped(used by recover)
   * @param sequence the files to be merged are sequence or unsequence
   * @param modifications the modifications already used in merge process, for return
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void merge(
      TsFileResource targetResource,
      List<TsFileResource> tsFileResources,
      String storageGroup,
      CompactionLogger compactionLogger,
      Set<String> devicesForSkip,
      boolean sequence,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    // Cml : ChunkMetadataList
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetResource.getTsFile());
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    Map<String, List<Modification>> modificationCache = new HashMap<>();
    RateLimiter compactionWriteRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    Set<String> allDevices =
        getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap, storageGroup);
    for (String device : allDevices) {
      if (devicesForSkip.contains(device)) {
        continue;
      }
      writer.startChunkGroup(device);

      Map<TsFileSequenceReader, LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>
          cmlCacheForMerge =
              new TreeMap<>(
                  (o1, o2) ->
                      TsFileManagement.compareFileName(
                          new File(o1.getFileName()), new File(o2.getFileName())));
      Map<TsFileSequenceReader, Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>>
          cmlIteratorCache =
              new TreeMap<>(
                  (o1, o2) ->
                      TsFileManagement.compareFileName(
                          new File(o1.getFileName()), new File(o2.getFileName())));
      // init chunkMetadataListIterator of all tsfileResources
      for (TsFileResource tsFileResource : tsFileResources) {
        TsFileSequenceReader reader =
            buildReaderFromTsFileResource(tsFileResource, tsFileSequenceReaderMap, storageGroup);
        if (reader == null) {
          throw new IOException();
        }
        Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>> iterator =
            new MeasurementChunkMetadataListMapIterator(reader, device);
        cmlIteratorCache.put(reader, iterator);
        cmlCacheForMerge.put(reader, new LinkedHashMap<>());
      }

      // if the device has unmerged chunkMetadataList in any file
      while (hasNextChunkMetadataList(cmlIteratorCache.values())) {
        String lastMeasurementSchema = null;
        Set<IMeasurementSchema> allMeasurementSchemas = new HashSet<>();
        // find the last sensor to be merged in this task.
        // And, for each file, get a bulk of sensors (256 by default) and union them into
        // allMeasurementSchemas
        for (Entry<TsFileSequenceReader, LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>
            cmlCacheForMergeEntry : cmlCacheForMerge.entrySet()) {
          TsFileSequenceReader reader = cmlCacheForMergeEntry.getKey();
          LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementCmlMapInFile =
              cmlCacheForMergeEntry.getValue();
          if (measurementCmlMapInFile.isEmpty()) {
            if (cmlIteratorCache.get(reader).hasNext()) {
              measurementCmlMapInFile = cmlIteratorCache.get(reader).next();
              cmlCacheForMerge.put(reader, measurementCmlMapInFile);
            } else {
              continue;
            }
          }
          // decide the last sensor to be merged in this task
          // it is the smaller sensor between the lastMeasurementSchema and the last measurement in
          // measurementSchemaChunkMetadataListMap
          IMeasurementSchema maxMeasurementSchema =
              getLastKeyOfLinkedHashMap(measurementCmlMapInFile);
          if (lastMeasurementSchema == null) {
            lastMeasurementSchema = maxMeasurementSchema.getMeasurementId();
          } else {
            if (maxMeasurementSchema.getMeasurementId().compareTo(lastMeasurementSchema) < 0) {
              lastMeasurementSchema = maxMeasurementSchema.getMeasurementId();
            }
          }
          // get all sensor used later
          allMeasurementSchemas.addAll(measurementCmlMapInFile.keySet());
        }

        for (IMeasurementSchema iMeasurementSchema : allMeasurementSchemas) {
          if (iMeasurementSchema.getMeasurementId().compareTo(lastMeasurementSchema) <= 0) {
            Map<TsFileSequenceReader, List<IChunkMetadata>> readerCmlMap =
                new TreeMap<>(
                    (o1, o2) ->
                        TsFileManagement.compareFileName(
                            new File(o1.getFileName()), new File(o2.getFileName())));
            // get all chunkMetadata of the iMeasurementSchema from cmlCacheForMerge,
            // and save into readerCmlMap
            for (Entry<
                    TsFileSequenceReader, LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>
                cmlCacheForMergeEntry : cmlCacheForMerge.entrySet()) {
              TsFileSequenceReader reader = cmlCacheForMergeEntry.getKey();
              LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementCmlMap =
                  cmlCacheForMergeEntry.getValue();
              if (measurementCmlMap.containsKey(iMeasurementSchema)) {
                readerCmlMap.put(reader, measurementCmlMap.get(iMeasurementSchema));
                measurementCmlMap.remove(iMeasurementSchema);
              }
            }

            Entry<IMeasurementSchema, Map<TsFileSequenceReader, List<IChunkMetadata>>>
                measurementReaderCmlEntry = new DefaultMapEntry<>(iMeasurementSchema, readerCmlMap);
            if (!sequence) {
              writeByDeserializeMerge(
                  device,
                  compactionWriteRateLimiter,
                  measurementReaderCmlEntry,
                  targetResource,
                  writer,
                  modificationCache,
                  modifications);
            } else {
              boolean isPageEnoughLarge = true;
              // check if there is at least a chunk whose points number is smaller than
              // MERGE_PAGE_POINT_NUM
              for (List<IChunkMetadata> chunkMetadatas : readerCmlMap.values()) {
                for (IChunkMetadata iChunkMetadata : chunkMetadatas) {
                  if (iChunkMetadata instanceof ChunkMetadata) {
                    ChunkMetadata chunkMetadata = (ChunkMetadata) iChunkMetadata;
                    if (chunkMetadata.getNumOfPoints() < MERGE_PAGE_POINT_NUM) {
                      isPageEnoughLarge = false;
                      break;
                    }
                  } else {
                    VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) iChunkMetadata;
                    if (vectorChunkMetadata.getStatistics().getCount() < MERGE_PAGE_POINT_NUM) {
                      isPageEnoughLarge = false;
                      break;
                    }
                  }
                }
              }
              if (isPageEnoughLarge) {
                logger.debug("{} [Compaction] page enough large, use append merge", storageGroup);
                // append page in chunks, so we do not have to deserialize a chunk
                writeByAppendMerge(
                    device,
                    compactionWriteRateLimiter,
                    measurementReaderCmlEntry,
                    targetResource,
                    writer,
                    modificationCache,
                    modifications);
              } else {
                logger.debug("{} [Compaction] page too small, use deserialize merge", storageGroup);
                // we have to deserialize chunks to merge pages
                writeByDeserializeMerge(
                    device,
                    compactionWriteRateLimiter,
                    measurementReaderCmlEntry,
                    targetResource,
                    writer,
                    modificationCache,
                    modifications);
              }
            }
          }
        }
      }
      writer.endChunkGroup();
      if (compactionLogger != null) {
        compactionLogger.logDevice(device, writer.getPos());
      }
    }

    for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
      reader.close();
    }

    for (TsFileResource tsFileResource : tsFileResources) {
      targetResource.updatePlanIndexes(tsFileResource);
    }
    targetResource.serialize();
    writer.endFile();
    targetResource.close();
  }

  /**
   * VectorMeasurementSchema is always smaller then MeasurementSchema, otherwise we compare the
   * measurementId
   *
   * @param measurementSchema1
   * @param measurementSchema2
   */
  private static boolean compareIMeasurementSchema(
      IMeasurementSchema measurementSchema1, IMeasurementSchema measurementSchema2) {
    if (measurementSchema1 instanceof VectorMeasurementSchema
        && measurementSchema2 instanceof MeasurementSchema) {
      return false;
    } else if (measurementSchema1 instanceof MeasurementSchema
        && measurementSchema2 instanceof VectorMeasurementSchema) {
      return true;
    } else {
      String timeMeasurementId1 = measurementSchema1.getMeasurementId();
      String timeMeasurementId2 = measurementSchema2.getMeasurementId();
      return timeMeasurementId1.compareTo(timeMeasurementId2) > 0;
    }
  }

  private static IMeasurementSchema getLastKeyOfLinkedHashMap(
      LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> sensorChunkMetadataListMap) {
    return new LinkedList<>(sensorChunkMetadataListMap.keySet()).getLast();
  }

  /**
   * get the tefileReader of the tsfileResource from the cache, or create a new one and put it into
   * the cache
   *
   * @param tsFileResource
   * @param tsFileSequenceReaderCache
   * @param storageGroup
   * @return
   */
  private static TsFileSequenceReader buildReaderFromTsFileResource(
      TsFileResource tsFileResource,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderCache,
      String storageGroup) {
    return tsFileSequenceReaderCache.computeIfAbsent(
        tsFileResource.getTsFile().getAbsolutePath(),
        path -> {
          try {
            if (tsFileResource.getTsFile().exists()) {
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

  private static void modifyChunkMetaDataWithCache(
      TsFileSequenceReader reader,
      List<IChunkMetadata> chunkMetadataList,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath,
      List<Modification> usedModifications) {
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            reader.getFileName(),
            fileName ->
                new LinkedList<>(
                    new ModificationFile(fileName + ModificationFile.FILE_SUFFIX)
                        .getModifications()));
    List<Modification> seriesModifications = new LinkedList<>();
    for (Modification modification : modifications) {
      if (modification.getPath().matchFullPath(seriesPath)) {
        seriesModifications.add(modification);
        usedModifications.add(modification);
      }
    }
    modifyChunkMetaData(chunkMetadataList, seriesModifications);
  }
}
