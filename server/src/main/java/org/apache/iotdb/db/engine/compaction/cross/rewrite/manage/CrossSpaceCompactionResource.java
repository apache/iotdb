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

package org.apache.iotdb.db.engine.compaction.cross.rewrite.manage;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * CrossSpaceMergeResource manages files and caches of readers, writers, MeasurementSchemas and
 * modifications to avoid unnecessary object creations and file openings.
 */
public class CrossSpaceCompactionResource {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles = new ArrayList<>();

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  private Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  private Map<TsFileResource, Map<String, Pair<Long, Long>>> startEndTimeCache =
      new HashMap<>(); // pair<startTime, endTime>
  private Map<IMeasurementSchema, ChunkWriterImpl> chunkWriterCache = new ConcurrentHashMap<>();

  private long ttlLowerBound = Long.MIN_VALUE;

  private boolean cacheDeviceMeta = false;

  public CrossSpaceCompactionResource(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles.stream().filter(this::filterSeqResource).collect(Collectors.toList());
    filterUnseqResource(unseqFiles);
  }

  /** Fitler the seq files into the compaction. Seq files should be not deleted or over ttl. */
  private boolean filterSeqResource(TsFileResource res) {
    return !res.isDeleted() && res.stillLives(ttlLowerBound);
  }

  /**
   * Filter the unseq files into the compaction. Unseq files should be not deleted or over ttl. To
   * ensure that the compaction is correct, return as soon as it encounters the file being compacted
   * or compaction candidate. Therefore, a cross space compaction can only be performed serially
   * under a time partition in a VSG.
   */
  private void filterUnseqResource(List<TsFileResource> unseqResources) {
    for (TsFileResource resource : unseqResources) {
      if (resource.getStatus() != TsFileResourceStatus.CLOSED) {
        return;
      } else if (!resource.isDeleted() && resource.stillLives(ttlLowerBound)) {
        this.unseqFiles.add(resource);
      }
    }
  }

  public CrossSpaceCompactionResource(
      Collection<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long ttlLowerBound) {
    this.ttlLowerBound = ttlLowerBound;
    this.seqFiles = seqFiles.stream().filter(this::filterSeqResource).collect(Collectors.toList());
    filterUnseqResource(unseqFiles);
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }
    for (RestorableTsFileIOWriter writer : fileWriterCache.values()) {
      writer.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    chunkWriterCache.clear();
  }

  /**
   * Construct the a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @return a TsFileSequenceReader
   */
  public TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFilePath(), true, cacheDeviceMeta);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  public List<Modification> getModifications(TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            tsFileResource, resource -> new LinkedList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public void setSeqFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public void setUnseqFiles(List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }

  public void removeOutdatedSeqReaders() throws IOException {
    Iterator<Entry<TsFileResource, TsFileSequenceReader>> entryIterator =
        fileReaderCache.entrySet().iterator();
    HashSet<TsFileResource> fileSet = new HashSet<>(seqFiles);
    while (entryIterator.hasNext()) {
      Entry<TsFileResource, TsFileSequenceReader> entry = entryIterator.next();
      TsFileResource tsFile = entry.getKey();
      if (!fileSet.contains(tsFile)) {
        TsFileSequenceReader reader = entry.getValue();
        reader.close();
        entryIterator.remove();
      }
    }
  }

  public void setCacheDeviceMeta(boolean cacheDeviceMeta) {
    this.cacheDeviceMeta = cacheDeviceMeta;
  }

  public void updateStartTime(TsFileResource tsFileResource, String device, long startTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newStartTime = startEndTimePair.left > startTime ? startTime : startEndTimePair.left;
    deviceStartEndTimePairMap.put(device, new Pair<>(newStartTime, startEndTimePair.right));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public void updateEndTime(TsFileResource tsFileResource, String device, long endTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newEndTime = startEndTimePair.right < endTime ? endTime : startEndTimePair.right;
    deviceStartEndTimePairMap.put(device, new Pair<>(startEndTimePair.left, newEndTime));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public Map<String, Pair<Long, Long>> getStartEndTime(TsFileResource tsFileResource) {
    return startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
  }
}
