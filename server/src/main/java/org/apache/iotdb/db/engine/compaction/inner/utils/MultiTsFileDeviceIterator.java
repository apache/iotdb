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

public class MultiTsFileDeviceIterator implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger("COMPACTION");
  private List<TsFileResource> tsFileResources;
  private Map<TsFileResource, TsFileSequenceReader> readerMap =
      new TreeMap<>((o1, o2) -> TsFileResource.compareFileName(o1.getTsFile(), o2.getTsFile()));
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();

  public MultiTsFileDeviceIterator(List<TsFileResource> tsFileResources) throws IOException {
    this.tsFileResources = new ArrayList<>(tsFileResources);
    try {
      for (TsFileResource tsFileResource : this.tsFileResources) {
        TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath());
        readerMap.put(tsFileResource, reader);
      }
    } catch (Throwable throwable) {
      // if there is any exception occurs
      // existing readers should be closed
      for (TsFileSequenceReader reader : readerMap.values()) {
        reader.close();
      }
      throw throwable;
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

  public MeasurementIterator iterateOneSeries(String device) throws IOException {
    return new MeasurementIterator(readerMap, device);
  }

  @Override
  public void close() throws IOException {
    for (TsFileSequenceReader reader : readerMap.values()) {
      reader.close();
    }
  }

  public class MeasurementIterator {
    private Map<TsFileResource, TsFileSequenceReader> readerMap;
    private String device;
    private String currentCompactingSeries = null;
    private LinkedList<String> seriesInThisIteration = new LinkedList<>();
    // tsfile sequence reader -> series -> list<ChunkMetadata>
    private Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>> chunkMetadataCacheMap =
        new TreeMap<>(new InnerSpaceCompactionUtils.TsFileNameComparator());
    // this map cache the chunk metadata list iterator for each tsfile
    // the iterator return a batch of series and all chunk metadata of these series in this tsfile
    private Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
        chunkMetadataIteratorMap =
            new TreeMap<>(new InnerSpaceCompactionUtils.TsFileNameComparator());

    private MeasurementIterator(Map<TsFileResource, TsFileSequenceReader> readerMap, String device)
        throws IOException {
      this.readerMap = readerMap;
      this.device = device;
      for (TsFileSequenceReader reader : readerMap.values()) {
        chunkMetadataIteratorMap.put(
            reader, reader.getMeasurementChunkMetadataListMapIterator(device));
        chunkMetadataCacheMap.put(reader, new TreeMap<>());
      }
    }

    /**
     * Collect series from files using iterator, and the collected series will be store in
     * seriesInThisIteration. To ensure that each serie is compacted once, when iterator of each
     * file returns a batch of series, we will find the max of it, and find the min series marked as
     * `last series` among the max series in each batch.
     *
     * <p>That is, lastSeries = min([max(series return in file 1),..., max(series return in file
     * n)]). Only the series that are greater than the lastSeries in lexicographical order will be
     * collected.
     *
     * @return true if there is any series is collected, else false.
     */
    private boolean collectSeries() {
      String lastSeries = null;
      List<String> tempCollectedSeries = new ArrayList<>();
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
        // get the min last series in the current chunk metadata
        String maxSeries = Collections.max(chunkMetadataListMap.keySet());
        if (lastSeries == null) {
          lastSeries = maxSeries;
        } else {
          if (maxSeries.compareTo(lastSeries) < 0) {
            lastSeries = maxSeries;
          }
        }
        tempCollectedSeries.addAll(chunkMetadataListMap.keySet());
      }
      if (tempCollectedSeries.size() > 0) {
        if (!hasRemainingSeries()) {
          lastSeries = Collections.max(tempCollectedSeries);
        }
        String finalLastSeries = lastSeries;
        List<String> finalCollectedSeriesInThisIteration =
            tempCollectedSeries.stream()
                .filter(series -> series.compareTo(finalLastSeries) <= 0)
                .collect(Collectors.toList());
        seriesInThisIteration.addAll(finalCollectedSeriesInThisIteration);
        return true;
      } else {
        return false;
      }
    }

    private boolean hasRemainingSeries() {
      boolean remaining = false;
      for (Iterator<Map<String, List<ChunkMetadata>>> iterator :
          chunkMetadataIteratorMap.values()) {
        remaining = remaining || iterator.hasNext();
      }
      return remaining;
    }

    public boolean hasNextSeries() {
      if (seriesInThisIteration.size() == 0 && !collectSeries()) {
        return false;
      } else {
        return true;
      }
    }

    public String nextSeries() {
      if (!hasNextSeries()) {
        return null;
      } else {
        currentCompactingSeries = seriesInThisIteration.removeFirst();
        return currentCompactingSeries;
      }
    }

    /**
     * Collect all the chunk metadata of current series from the source files.
     *
     * <p>If there are any modifications for these chunk, we will apply them to the metadata. Use
     * `ChunkMetadata.getDeleteIntervalList() == null` to judge if the chunk is modified.
     *
     * @return
     * @throws IllegalPathException
     */
    public LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
        getMetadataListForCurrentSeries() throws IllegalPathException {
      if (currentCompactingSeries == null) {
        return null;
      }

      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
          readerAndChunkMetadataForThisSeries = new LinkedList<>();
      PartialPath path = new PartialPath(device, currentCompactingSeries);

      for (TsFileResource resource : tsFileResources) {
        TsFileSequenceReader reader = readerMap.get(resource);
        Map<String, List<ChunkMetadata>> chunkMetadataListMap = chunkMetadataCacheMap.get(reader);

        if (chunkMetadataListMap.containsKey(currentCompactingSeries)) {
          // get the chunk metadata list and modification list of current series in this tsfile
          List<ChunkMetadata> chunkMetadataListInThisResource =
              chunkMetadataListMap.get(currentCompactingSeries);
          chunkMetadataListMap.remove(currentCompactingSeries);

          List<Modification> modificationsInThisResource =
              modificationCache.computeIfAbsent(
                  resource,
                  r -> new LinkedList<>(ModificationFile.getNormalMods(r).getModifications()));
          LinkedList<Modification> modificationForCurrentSeries = new LinkedList<>();
          // collect the modifications for current series
          for (Modification modification : modificationsInThisResource) {
            if (modification.getPath().matchFullPath(path)) {
              modificationForCurrentSeries.add(modification);
            }
          }

          // if there are modifications of current series, apply them to the chunk metadata
          if (modificationForCurrentSeries.size() != 0) {
            QueryUtils.modifyChunkMetaData(
                chunkMetadataListInThisResource, modificationForCurrentSeries);
          }

          readerAndChunkMetadataForThisSeries.add(
              new Pair<>(reader, chunkMetadataListInThisResource));
        }
      }
      return readerAndChunkMetadataForThisSeries;
    }
  }
}
