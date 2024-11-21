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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.constant.CompactionType;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MultiTsFileDeviceIterator implements AutoCloseable {

  // sort from the newest to the oldest by version (Used by FastPerformer and ReadPointPerformer)
  private final List<TsFileResource> tsFileResourcesSortedByDesc;

  // sort from the oldest to the newest by version (Used by ReadChunkPerformer)
  private List<TsFileResource> tsFileResourcesSortedByAsc;
  private Map<TsFileResource, TsFileSequenceReader> readerMap = new HashMap<>();
  private final Map<TsFileResource, TsFileDeviceIterator> deviceIteratorMap = new HashMap<>();
  private final Map<TsFileResource, List<ModEntry>> modificationCache = new HashMap<>();
  private Pair<IDeviceID, Boolean> currentDevice = null;
  private boolean ignoreAllNullRows;
  private long ttlForCurrentDevice;
  private long timeLowerBoundForCurrentDevice;
  private final String databaseName;

  /**
   * Used for compaction with read chunk performer.
   *
   * @throws IOException if io error occurred
   */
  public MultiTsFileDeviceIterator(List<TsFileResource> tsFileResources) throws IOException {
    this.databaseName = tsFileResources.get(0).getDatabaseName();
    this.tsFileResourcesSortedByDesc = new ArrayList<>(tsFileResources);
    this.tsFileResourcesSortedByAsc = new ArrayList<>(tsFileResources);
    // sort the files from the oldest to the newest
    Collections.sort(this.tsFileResourcesSortedByAsc, TsFileResource::compareFileName);
    // sort the files from the newest to the oldest
    Collections.sort(
        this.tsFileResourcesSortedByDesc, TsFileResource::compareFileCreationOrderByDesc);
    try {
      for (TsFileResource tsFileResource : this.tsFileResourcesSortedByDesc) {
        CompactionTsFileReader reader =
            new CompactionTsFileReader(
                tsFileResource.getTsFilePath(), CompactionType.INNER_SEQ_COMPACTION);
        readerMap.put(tsFileResource, reader);
        deviceIteratorMap.put(tsFileResource, reader.getAllDevicesIteratorWithIsAligned());
      }
    } catch (Exception e) {
      // if there is any exception occurs
      // existing readers should be closed
      for (TsFileSequenceReader reader : readerMap.values()) {
        reader.close();
      }
      throw e;
    }
  }

  /**
   * Used for compaction with read point performer.
   *
   * @throws IOException if io errors occurred
   */
  public MultiTsFileDeviceIterator(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    this.tsFileResourcesSortedByDesc = new ArrayList<>(seqResources);
    tsFileResourcesSortedByDesc.addAll(unseqResources);
    this.databaseName = tsFileResourcesSortedByDesc.get(0).getDatabaseName();
    // sort the files from the newest to the oldest
    Collections.sort(
        this.tsFileResourcesSortedByDesc, TsFileResource::compareFileCreationOrderByDesc);
    for (TsFileResource tsFileResource : tsFileResourcesSortedByDesc) {
      TsFileSequenceReader reader =
          FileReaderManager.getInstance().get(tsFileResource.getTsFilePath(), true);
      readerMap.put(tsFileResource, reader);
      deviceIteratorMap.put(tsFileResource, reader.getAllDevicesIteratorWithIsAligned());
    }
  }

  /**
   * Used for compaction with fast performer.
   *
   * @throws IOException if io errors occurred
   */
  public MultiTsFileDeviceIterator(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    this.tsFileResourcesSortedByDesc = new ArrayList<>(seqResources);
    tsFileResourcesSortedByDesc.addAll(unseqResources);
    this.databaseName = tsFileResourcesSortedByDesc.get(0).getDatabaseName();
    // sort tsfiles from the newest to the oldest
    Collections.sort(
        this.tsFileResourcesSortedByDesc, TsFileResource::compareFileCreationOrderByDesc);
    this.readerMap = readerMap;

    CompactionType type = null;
    if (!seqResources.isEmpty() && !unseqResources.isEmpty()) {
      type = CompactionType.CROSS_COMPACTION;
    } else if (seqResources.isEmpty()) {
      type = CompactionType.INNER_UNSEQ_COMPACTION;
    } else {
      type = CompactionType.INNER_SEQ_COMPACTION;
    }

    for (TsFileResource tsFileResource : tsFileResourcesSortedByDesc) {
      TsFileSequenceReader reader =
          new CompactionTsFileReader(tsFileResource.getTsFilePath(), type);
      readerMap.put(tsFileResource, reader);
      deviceIteratorMap.put(tsFileResource, reader.getAllDevicesIteratorWithIsAligned());
    }
  }

  public boolean hasNextDevice() {
    boolean hasNext = false;
    for (TsFileDeviceIterator iterator : deviceIteratorMap.values()) {
      hasNext =
          hasNext
              || iterator.hasNext()
              || (iterator.current() != null
                  && !iterator.current().left.equals(currentDevice.left));
    }
    return hasNext;
  }

  /**
   * Return next device that is minimal in lexicographical order.
   *
   * @return Pair of device full path and whether this device is aligned
   */
  @SuppressWarnings({"squid:S135", "java:S2259"})
  public Pair<IDeviceID, Boolean> nextDevice() throws IllegalPathException {
    List<TsFileResource> toBeRemovedResources = new LinkedList<>();
    Pair<IDeviceID, Boolean> minDevice = null;
    // get the device from source files sorted from the newest to the oldest by version
    for (TsFileResource resource : tsFileResourcesSortedByDesc) {
      if (!deviceIteratorMap.containsKey(resource)) {
        continue;
      }
      TsFileDeviceIterator deviceIterator = deviceIteratorMap.get(resource);
      if (deviceIterator.current() == null
          || deviceIterator.current().left.equals(currentDevice.left)) {
        // if current file has same device with current device, then get its next device
        if (deviceIterator.hasNext()) {
          deviceIterator.next();
        } else {
          // this iterator does not have next device
          // remove them after the loop
          toBeRemovedResources.add(resource);
          continue;
        }
      }
      if (minDevice == null || minDevice.left.compareTo(deviceIterator.current().left) > 0) {
        // get the device that is minimal in lexicographical order according to the all files
        minDevice = deviceIterator.current();
      }
    }
    currentDevice = minDevice;
    // remove the iterator with no device remaining
    for (TsFileResource resource : toBeRemovedResources) {
      deviceIteratorMap.remove(resource);
    }

    IDeviceID deviceID = currentDevice.left;
    boolean isAligned = currentDevice.right;
    ignoreAllNullRows = !isAligned || deviceID.getTableName().startsWith("root.");
    if (!ignoreAllNullRows) {
      ttlForCurrentDevice =
          DataNodeTTLCache.getInstance().getTTLForTable(databaseName, deviceID.getTableName());
    } else {
      ttlForCurrentDevice = DataNodeTTLCache.getInstance().getTTLForTree(deviceID);
    }
    timeLowerBoundForCurrentDevice = CommonDateTimeUtils.currentTime() - ttlForCurrentDevice;
    return currentDevice;
  }

  public long getTTLForCurrentDevice() {
    return ttlForCurrentDevice;
  }

  public long getTimeLowerBoundForCurrentDevice() {
    return timeLowerBoundForCurrentDevice;
  }

  /**
   * Get all measurements and schemas of the current device from source files. Traverse all the
   * files from the newest to the oldest in turn and start traversing the index tree from the
   * firstMeasurementNode node to get all the measurements under the current device.
   *
   * @throws IOException if io errors occurred
   */
  public Map<String, MeasurementSchema> getAllSchemasOfCurrentDevice() throws IOException {
    Map<String, MeasurementSchema> schemaMap = new ConcurrentHashMap<>();
    // get schemas from the newest file to the oldest file
    for (TsFileResource resource : tsFileResourcesSortedByDesc) {
      if (!deviceIteratorMap.containsKey(resource)
          || !deviceIteratorMap.get(resource).current().equals(currentDevice)) {
        // if this tsfile has no more device or next device is not equals to the current device,
        // which means this tsfile does not contain the current device, then skip it.
        continue;
      }
      TsFileSequenceReader reader = readerMap.get(resource);
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      reader.getDeviceTimeseriesMetadata(
          timeseriesMetadataList,
          deviceIteratorMap.get(resource).getFirstMeasurementNodeOfCurrentDevice(),
          schemaMap.keySet(),
          true);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
        if (!schemaMap.containsKey(timeseriesMetadata.getMeasurementId())
            && !timeseriesMetadata.getChunkMetadataList().isEmpty()) {
          schemaMap.put(
              timeseriesMetadata.getMeasurementId(),
              reader.getMeasurementSchema(timeseriesMetadata.getChunkMetadataList()));
        }
      }
    }
    return schemaMap;
  }

  /**
   * Get all measurements and their timeseries metadata offset in each source file. It is used for
   * new fast compaction to compact nonAligned timeseries.
   *
   * @return measurement -> tsfile resource -> timeseries metadata <startOffset, endOffset>
   * @throws IOException if io errors occurred
   */
  public Map<String, Map<TsFileResource, Pair<Long, Long>>>
      getTimeseriesMetadataOffsetOfCurrentDevice() throws IOException {
    Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap =
        new HashMap<>();
    Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
    for (TsFileResource resource : tsFileResourcesSortedByDesc) {
      if (!deviceIteratorMap.containsKey(resource)
          || !deviceIteratorMap.get(resource).current().equals(currentDevice)) {
        // if this tsfile has no more device or next device is not equals to the current device,
        // which means this tsfile does not contain the current device, then skip it.
        continue;
      }
      TsFileSequenceReader reader = readerMap.get(resource);
      for (Map.Entry<String, Pair<TimeseriesMetadata, Pair<Long, Long>>> entrySet :
          ((CompactionTsFileReader) reader)
              .getTimeseriesMetadataAndOffsetByDevice(
                  deviceIteratorMap.get(resource).getFirstMeasurementNodeOfCurrentDevice(),
                  Collections.emptySet(),
                  false)
              .entrySet()) {
        String measurementId = entrySet.getKey();
        // skip the TimeseriesMetadata whose data type is not consistent
        TSDataType dataTypeOfCurrentTimeseriesMetadata = entrySet.getValue().left.getTsDataType();
        TSDataType correctDataTypeOfCurrentMeasurement =
            measurementDataTypeMap.putIfAbsent(measurementId, dataTypeOfCurrentTimeseriesMetadata);
        if (correctDataTypeOfCurrentMeasurement != null
            && correctDataTypeOfCurrentMeasurement != dataTypeOfCurrentTimeseriesMetadata) {
          continue;
        }
        timeseriesMetadataOffsetMap.putIfAbsent(measurementId, new HashMap<>());
        timeseriesMetadataOffsetMap.get(measurementId).put(resource, entrySet.getValue().right);
      }
    }
    return timeseriesMetadataOffsetMap;
  }

  /**
   * Get all measurements and their schemas of the current device and the timeseries metadata offset
   * of each timeseries in each source file. It is used for new fast compaction to compact aligned
   * timeseries.
   *
   * @return measurement -> metadata -> tsfile resource -> timeseries metadata <startOffset,
   *     endOffset>
   * @throws IOException if io errors occurred
   */
  @SuppressWarnings({"checkstyle:AtclauseOrderCheck", "squid:S3824"})
  public Map<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>>
      getTimeseriesSchemaAndMetadataOffsetOfCurrentDevice() throws IOException {
    Map<String, Pair<MeasurementSchema, Map<TsFileResource, Pair<Long, Long>>>>
        timeseriesMetadataOffsetMap = new LinkedHashMap<>();
    for (TsFileResource resource : tsFileResourcesSortedByDesc) {
      if (!deviceIteratorMap.containsKey(resource)
          || !deviceIteratorMap.get(resource).current().equals(currentDevice)) {
        // if this tsfile has no more device or next device is not equals to the current device,
        // which means this tsfile does not contain the current device, then skip it.
        continue;
      }

      TsFileSequenceReader reader = readerMap.get(resource);
      for (Map.Entry<String, Pair<List<IChunkMetadata>, Pair<Long, Long>>> entrySet :
          reader
              .getTimeseriesMetadataOffsetByDevice(
                  deviceIteratorMap.get(resource).getFirstMeasurementNodeOfCurrentDevice(),
                  timeseriesMetadataOffsetMap.keySet(),
                  true)
              .entrySet()) {
        String measurementId = entrySet.getKey();
        if (!timeseriesMetadataOffsetMap.containsKey(measurementId)) {
          MeasurementSchema schema = reader.getMeasurementSchema(entrySet.getValue().left);
          timeseriesMetadataOffsetMap.put(measurementId, new Pair<>(schema, new HashMap<>()));
        }
        timeseriesMetadataOffsetMap
            .get(measurementId)
            .right
            .put(resource, entrySet.getValue().right);
      }
    }
    return timeseriesMetadataOffsetMap;
  }

  /**
   * return MultiTsFileNonAlignedMeasurementMetadataListIterator, who iterates the measurements of
   * not aligned device
   *
   * @return measurement iterator of not aligned device
   * @throws IOException if io errors occurred
   */
  public MultiTsFileNonAlignedMeasurementMetadataListIterator
      iterateNotAlignedSeriesAndChunkMetadataListOfCurrentDevice() throws IOException {
    return new MultiTsFileNonAlignedMeasurementMetadataListIterator();
  }

  /**
   * return a list of the tsfile reader and its aligned chunk metadata list for the aligned device
   * which this iterator is visiting. If there is any modification for this device, it will be
   * applied to the AlignedChunkMetadata, so that the user of this function can reader Chunk
   * directly using the reader and the chunkMetadata returned. Notice, if the TsFile corresponding
   * to a TsFileSequenceReader does not contain the current device, the TsFileSequenceReader will
   * not appear in the return list.
   *
   * @return a list of pair(TsFileSequenceReader, the list of AlignedChunkMetadata for current
   *     device)
   * @throws IOException if io errors occurred
   */
  @SuppressWarnings({"squid:S1319", "squid:S135"})
  public LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
      getReaderAndChunkMetadataForCurrentAlignedSeries() throws IOException, IllegalPathException {
    if (currentDevice == null || !currentDevice.right) {
      return new LinkedList<>();
    }

    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        new LinkedList<>();
    for (TsFileResource tsFileResource : tsFileResourcesSortedByAsc) {
      if (!deviceIteratorMap.containsKey(tsFileResource)) {
        continue;
      }
      TsFileDeviceIterator iterator = deviceIteratorMap.get(tsFileResource);
      if (!currentDevice.equals(iterator.current())) {
        continue;
      }
      MetadataIndexNode firstMeasurementNodeOfCurrentDevice =
          iterator.getFirstMeasurementNodeOfCurrentDevice();
      TsFileSequenceReader reader = readerMap.get(tsFileResource);
      List<AlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadataByMetadataIndexNode(
              currentDevice.left, firstMeasurementNodeOfCurrentDevice, ignoreAllNullRows);
      applyModificationForAlignedChunkMetadataList(tsFileResource, alignedChunkMetadataList);
      readerAndChunkMetadataList.add(new Pair<>(reader, alignedChunkMetadataList));
    }

    return readerAndChunkMetadataList;
  }

  /**
   * collect the modification for current device and apply it to the alignedChunkMetadataList.
   *
   * @param tsFileResource tsfile resource
   * @param alignedChunkMetadataList list of aligned chunk metadata
   */
  private void applyModificationForAlignedChunkMetadataList(
      TsFileResource tsFileResource, List<AlignedChunkMetadata> alignedChunkMetadataList)
      throws IllegalPathException {
    if (alignedChunkMetadataList.isEmpty()) {
      // all the value chunks is empty chunk
      return;
    }
    IDeviceID device = currentDevice.getLeft();
    ModEntry ttlDeletion = null;
    if (tsFileResource.getStartTime(device) < timeLowerBoundForCurrentDevice) {
      ttlDeletion = CompactionUtils.convertTtlToDeletion(device, timeLowerBoundForCurrentDevice);
    }

    List<ModEntry> modifications =
        modificationCache.computeIfAbsent(
            tsFileResource, r -> new ArrayList<>(tsFileResource.getAllModEntries()));

    // construct the input params List<List<Modification>> for QueryUtils.modifyAlignedChunkMetaData
    AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
    List<IChunkMetadata> valueChunkMetadataList = alignedChunkMetadata.getValueChunkMetadataList();

    // match time column modifications
    List<ModEntry> modificationForTimeColumn = new ArrayList<>();
    for (ModEntry modification : modifications) {
      if (modification.affectsAll(device)) {
        modificationForTimeColumn.add(modification);
      }
    }
    if (ttlDeletion != null) {
      modificationForTimeColumn.add(ttlDeletion);
    }

    // match value column modifications
    List<List<ModEntry>> modificationForValueColumns = new ArrayList<>();
    for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
      if (valueChunkMetadata == null) {
        modificationForValueColumns.add(Collections.emptyList());
        continue;
      }
      List<ModEntry> modificationList = new ArrayList<>();
      PartialPath path =
          CompactionPathUtils.getPath(
              currentDevice.getLeft(), valueChunkMetadata.getMeasurementUid());
      for (ModEntry modification : modifications) {
        if (modification.matches(path)) {
          modificationList.add(modification);
        }
      }
      if (ttlDeletion != null) {
        modificationList.add(ttlDeletion);
      }
      modificationForValueColumns.add(
          modificationList.isEmpty() ? Collections.emptyList() : modificationList);
    }

    ModificationUtils.modifyAlignedChunkMetaData(
        alignedChunkMetadataList,
        modificationForTimeColumn,
        modificationForValueColumns,
        ignoreAllNullRows);
  }

  public Map<TsFileResource, TsFileSequenceReader> getReaderMap() {
    return readerMap;
  }

  @Override
  public void close() throws IOException {
    for (TsFileSequenceReader reader : readerMap.values()) {
      reader.close();
    }
  }

  /*
  NonAligned measurement iterator.
   */
  public class MultiTsFileNonAlignedMeasurementMetadataListIterator {
    private final LinkedList<String> seriesInThisIteration = new LinkedList<>();
    // tsfile sequence reader -> series -> list<ChunkMetadata>
    private final Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
        chunkMetadataCacheMap = new HashMap<>();
    // this map cache the chunk metadata list iterator for each tsfile
    // the iterator return a batch of series and all chunk metadata of these series in this tsfile
    private final Map<TsFileResource, Iterator<Map<String, List<ChunkMetadata>>>>
        chunkMetadataIteratorMap = new HashMap<>();
    private String currentCompactingSeries = null;
    private LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
        chunkMetadataMetadataListOfCurrentCompactingSeries;

    private MultiTsFileNonAlignedMeasurementMetadataListIterator() throws IOException {
      IDeviceID device = currentDevice.getLeft();
      for (TsFileResource resource : tsFileResourcesSortedByAsc) {
        TsFileDeviceIterator deviceIterator = deviceIteratorMap.get(resource);
        TsFileSequenceReader reader = readerMap.get(resource);
        if (deviceIterator == null || !device.equals(deviceIterator.current().getLeft())) {
          chunkMetadataIteratorMap.put(
              resource,
              new Iterator<Map<String, List<ChunkMetadata>>>() {
                @Override
                public boolean hasNext() {
                  return false;
                }

                @Override
                @SuppressWarnings("java:S2272")
                public Map<String, List<ChunkMetadata>> next() {
                  return Collections.emptyMap();
                }
              });
        } else {
          chunkMetadataIteratorMap.put(
              resource,
              reader.getMeasurementChunkMetadataListMapIterator(
                  deviceIterator.getFirstMeasurementNodeOfCurrentDevice()));
        }
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
    @SuppressWarnings("squid:S3776")
    private boolean collectSeries() {
      String lastSeries = null;
      List<String> collectedSeries = new ArrayList<>();
      for (TsFileResource resource : tsFileResourcesSortedByAsc) {
        Map<String, List<ChunkMetadata>> batchMeasurementChunkMetadataList =
            getBatchMeasurementChunkMetadataListFromCache(resource);
        if (batchMeasurementChunkMetadataList.isEmpty()) {
          continue;
        }

        // get the min last series in the current chunk metadata
        String maxSeriesOfCurrentBatch =
            Collections.max(batchMeasurementChunkMetadataList.keySet());
        if (lastSeries == null) {
          lastSeries = maxSeriesOfCurrentBatch;
        } else if (maxSeriesOfCurrentBatch.compareTo(lastSeries) < 0) {
          lastSeries = maxSeriesOfCurrentBatch;
        }
        collectedSeries.addAll(batchMeasurementChunkMetadataList.keySet());
      }

      if (collectedSeries.isEmpty()) {
        return false;
      }
      if (!hasRemainingSeries()) {
        lastSeries = Collections.max(collectedSeries);
      }

      String finalLastSeries = lastSeries;
      seriesInThisIteration.addAll(
          collectedSeries.stream()
              .filter(series -> series.compareTo(finalLastSeries) <= 0)
              .sorted()
              .distinct()
              .collect(Collectors.toList()));
      return true;
    }

    private Map<String, List<ChunkMetadata>> getBatchMeasurementChunkMetadataListFromCache(
        TsFileResource resource) {
      TsFileSequenceReader reader = readerMap.get(resource);
      Map<String, List<ChunkMetadata>> cachedBatchMeasurementChunkMetadataListMap =
          chunkMetadataCacheMap.get(reader);
      if (!cachedBatchMeasurementChunkMetadataListMap.isEmpty()) {
        return cachedBatchMeasurementChunkMetadataListMap;
      }
      Iterator<Map<String, List<ChunkMetadata>>> batchMeasurementChunkMetadataListIterator =
          chunkMetadataIteratorMap.get(resource);
      if (!batchMeasurementChunkMetadataListIterator.hasNext()) {
        return cachedBatchMeasurementChunkMetadataListMap;
      }
      Map<String, List<ChunkMetadata>> newBatchMeasurementChunkMetadataListMap =
          batchMeasurementChunkMetadataListIterator.next();
      // if encounter deleted aligned series, then remove it
      newBatchMeasurementChunkMetadataListMap.remove("");
      chunkMetadataCacheMap.put(reader, newBatchMeasurementChunkMetadataListMap);
      return newBatchMeasurementChunkMetadataListMap;
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
      return !seriesInThisIteration.isEmpty() || collectSeries();
    }

    public String nextSeries() throws IllegalPathException {
      if (!hasNextSeries()) {
        return null;
      } else {
        chunkMetadataMetadataListOfCurrentCompactingSeries =
            calculateMetadataListForCurrentSeries();
        return currentCompactingSeries;
      }
    }

    public LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
        getMetadataListForCurrentSeries() {
      return chunkMetadataMetadataListOfCurrentCompactingSeries;
    }

    /**
     * Collect all the chunk metadata of current series from the source files.
     *
     * <p>If there are any modifications for these chunk, we will apply them to the metadata. Use
     * `ChunkMetadata.getDeleteIntervalList() == null` to judge if the chunk is modified.
     *
     * @return all the chunk metadata of current series
     * @throws IllegalPathException if path is illegal
     */
    @SuppressWarnings("squid:S1319")
    private LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
        calculateMetadataListForCurrentSeries() throws IllegalPathException {
      if (seriesInThisIteration.isEmpty()) {
        return new LinkedList<>();
      }
      IDeviceID device = currentDevice.getLeft();
      currentCompactingSeries = seriesInThisIteration.removeFirst();

      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>>
          readerAndChunkMetadataForThisSeries = new LinkedList<>();
      PartialPath path = CompactionPathUtils.getPath(device, currentCompactingSeries);

      for (TsFileResource resource : tsFileResourcesSortedByAsc) {
        TsFileSequenceReader reader = readerMap.get(resource);
        Map<String, List<ChunkMetadata>> chunkMetadataListMap = chunkMetadataCacheMap.get(reader);

        ModEntry ttlDeletion = null;
        if (resource.getStartTime(device) < timeLowerBoundForCurrentDevice) {
          ttlDeletion =
              new TreeDeletionEntry(
                  new MeasurementPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD),
                  Long.MIN_VALUE,
                  timeLowerBoundForCurrentDevice);
        }

        if (chunkMetadataListMap.containsKey(currentCompactingSeries)) {
          // get the chunk metadata list and modification list of current series in this tsfile
          List<ChunkMetadata> chunkMetadataListInThisResource =
              chunkMetadataListMap.get(currentCompactingSeries);
          chunkMetadataListMap.remove(currentCompactingSeries);

          List<ModEntry> modificationsInThisResource =
              modificationCache.computeIfAbsent(
                  resource, r -> new ArrayList<>(r.getAllModEntries()));
          LinkedList<ModEntry> modificationForCurrentSeries = new LinkedList<>();
          // collect the modifications for current series
          for (ModEntry modification : modificationsInThisResource) {
            if (modification.matches(path)) {
              modificationForCurrentSeries.add(modification);
            }
          }
          // add ttl deletion for current series
          if (ttlDeletion != null) {
            modificationForCurrentSeries.add(ttlDeletion);
          }

          // if there are modifications of current series, apply them to the chunk metadata
          if (!modificationForCurrentSeries.isEmpty()) {
            ModificationUtils.modifyChunkMetaData(
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
