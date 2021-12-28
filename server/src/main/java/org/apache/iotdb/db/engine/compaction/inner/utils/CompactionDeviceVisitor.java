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

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CompactionDeviceVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private List<TsFileResource> tsFileResources;
  private Map<TsFileResource, TsFileSequenceReader> readerMap =
      new TreeMap<>((o1, o2) -> TsFileResource.compareFileName(o1.getTsFile(), o2.getTsFile()));
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();

  public CompactionDeviceVisitor(List<TsFileResource> tsFileResources) throws IOException {
    this.tsFileResources = new ArrayList<>(tsFileResources);
    for (TsFileResource tsFileResource : tsFileResources) {
      tsFileResources.add(tsFileResource);
      TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
      readerMap.put(tsFileResource, reader);
    }
  }

  public Set<String> getDevices() throws IOException {
    Set<String> deviceSet = new HashSet<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      TsFileSequenceReader reader = readerMap.get(tsFileResource);
      deviceSet.addAll(reader.getAllDevices());
    }
    return deviceSet;
  }

  public CompactionSensorsIterator visit(String device) throws IOException {
    return new CompactionSensorsIterator(readerMap, device);
  }

  public class CompactionSensorsIterator {
    private Map<TsFileResource, TsFileSequenceReader> readerMap;
    private String device;
    private String currentCompactingSensor = null;
    private LinkedList<String> sensorsInThisIteration = new LinkedList<>();
    // tsfile sequence reader -> sensor -> list<ChunkMetadata>
    private Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>> chunkMetadataCacheMap =
        new TreeMap<>(new InnerSpaceCompactionUtils.TsFileNameComparator());
    // this map cache the chunk metadata list iterator for each tsfile
    // the iterator return a batch of sensors and all chunk metadata of these sensors in this tsfile
    private Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
        chunkMetadataIteratorMap =
            new TreeMap<>(new InnerSpaceCompactionUtils.TsFileNameComparator());

    private CompactionSensorsIterator(
        Map<TsFileResource, TsFileSequenceReader> readerMap, String device) throws IOException {
      this.readerMap = readerMap;
      this.device = device;
      for (TsFileSequenceReader reader : readerMap.values()) {
        chunkMetadataIteratorMap.put(
            reader, reader.getMeasurementChunkMetadataListMapIterator(device));
        chunkMetadataCacheMap.put(reader, new TreeMap<>());
      }
    }

    /**
     * Collect sensors from files using iterator, and the collected sensors will be store in
     * sensorInThisIteration. To ensure that each sensor is compacted once, when iterator of each
     * file returns a batch of sensors, we will find the max of it, and find the min sensor marked
     * as `last sensor` among the max sensors in each batch.
     *
     * <p>That is, lastSensor = min([max(sensors return in file 1),..., max(sensors return in file
     * n)]). Only the sensors that are greater than the lastSensor in lexicographical order will be
     * collected.
     *
     * @return true if there is any sensor is collected, else false.
     */
    private boolean collectSensors() {
      String lastSensor = null;
      List<String> tempCollectedSensors = new ArrayList<>();
      for (Map.Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
          chunkMetadataListCacheForMergeEntry : chunkMetadataCacheMap.entrySet()) {
        TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
        Map<String, List<ChunkMetadata>> chunkMetadataListMap =
            chunkMetadataListCacheForMergeEntry.getValue();
        if (chunkMetadataListMap.size() == 0) {
          if (chunkMetadataIteratorMap.get(reader).hasNext()) {
            chunkMetadataListMap = chunkMetadataIteratorMap.get(reader).next();
            chunkMetadataCacheMap.put(reader, chunkMetadataListMap);
          } else {
            continue;
          }
        }
        // get the min last sensor in the current chunk metadata
        String maxSensor = Collections.max(chunkMetadataListMap.keySet());
        if (lastSensor == null) {
          lastSensor = maxSensor;
        } else {
          if (maxSensor.compareTo(lastSensor) < 0) {
            lastSensor = maxSensor;
          }
        }
        tempCollectedSensors.addAll(chunkMetadataListMap.keySet());
      }
      if (tempCollectedSensors.size() > 0) {
        if (!hasRemainingSensors()) {
          lastSensor = Collections.max(tempCollectedSensors);
        }
        String finalLastSensor = lastSensor;
        List<String> finalCollectedSensorsInThisIteration =
            tempCollectedSensors.stream()
                .filter(sensor -> sensor.compareTo(finalLastSensor) <= 0)
                .collect(Collectors.toList());
        sensorsInThisIteration.addAll(finalCollectedSensorsInThisIteration);
        return true;
      } else {
        return false;
      }
    }

    private boolean hasRemainingSensors() {
      boolean remaining = false;
      for (Iterator<Map<String, List<ChunkMetadata>>> iterator :
          chunkMetadataIteratorMap.values()) {
        remaining = remaining || iterator.hasNext();
      }
      return remaining;
    }

    public boolean hasNextSensor() {
      if (sensorsInThisIteration.size() == 0 && !collectSensors()) {
        return false;
      } else {
        return true;
      }
    }

    public String nextSensor() {
      if (!hasNextSensor()) {
        return null;
      } else {
        currentCompactingSensor = sensorsInThisIteration.removeFirst();
        return currentCompactingSensor;
      }
    }

    /**
     * Collect all the chunk metadata of current sensor from the source files.
     *
     * <p>If there are any modifications for these chunk, we will apply them to the metadata. Use
     * `ChunkMetadata.getDeleteIntervalList() == null` to judge if the chunk is modified.
     *
     * @return
     * @throws IllegalPathException
     */
    public LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
        getMetadataListForCurrentSensor() throws IllegalPathException {
      if (currentCompactingSensor == null) {
        return null;
      }

      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
          readerAndChunkMetadataForThisSensor = new LinkedList<>();
      PartialPath path = new PartialPath(device, currentCompactingSensor);

      for (TsFileResource resource : tsFileResources) {
        TsFileSequenceReader reader = readerMap.get(resource);
        Map<String, List<ChunkMetadata>> chunkMetadataListMap = chunkMetadataCacheMap.get(reader);

        if (chunkMetadataListMap.containsKey(currentCompactingSensor)) {
          // get the chunk metadata list and modification list of current sensor in this tsfile
          List<ChunkMetadata> chunkMetadataListInThisResource =
              chunkMetadataListMap.get(currentCompactingSensor);
          chunkMetadataListMap.remove(currentCompactingSensor);

          List<Modification> modificationsInThisResource =
              modificationCache.computeIfAbsent(
                  resource,
                  r -> new LinkedList<>(ModificationFile.getNormalMods(r).getModifications()));
          LinkedList<Modification> modificationForCurrentSensor = new LinkedList<>();
          // collect the modifications for current sensor
          for (Modification modification : modificationsInThisResource) {
            if (modification.getPath().matchFullPath(path)) {
              modificationForCurrentSensor.add(modification);
            }
          }

          // if there are modifications of current sensor, apply them to the chunk metadata
          if (modificationForCurrentSensor.size() != 0) {
            QueryUtils.modifyChunkMetaData(
                chunkMetadataListInThisResource, modificationForCurrentSensor);
          }

          readerAndChunkMetadataForThisSensor.add(
              new Pair<>(reader, chunkMetadataListInThisResource));
        }
      }
      return readerAndChunkMetadataForThisSensor;
    }
  }
}
