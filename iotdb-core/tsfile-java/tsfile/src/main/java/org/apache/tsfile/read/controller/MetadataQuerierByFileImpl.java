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

package org.apache.tsfile.read.controller;

import org.apache.tsfile.common.cache.LRUCache;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReader.LocateStatus;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class MetadataQuerierByFileImpl implements IMetadataQuerier {

  // number of cache entries (path -> List<ChunkMetadata>)
  private static final int CACHED_ENTRY_NUMBER = 1000;

  private TsFileMetadata fileMetaData;

  // (deviceId, measurementId) -> List<IChunkMetadata>
  private LRUCache<Pair<IDeviceID, String>, List<IChunkMetadata>> deviceIdChunkMetadataCache;

  private TsFileSequenceReader tsFileReader;

  /** Constructor of MetadataQuerierByFileImpl. */
  public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
    this.tsFileReader = tsFileReader;
    this.fileMetaData = tsFileReader.readFileMetadata();
    deviceIdChunkMetadataCache =
        new LRUCache<Pair<IDeviceID, String>, List<IChunkMetadata>>(CACHED_ENTRY_NUMBER) {
          @Override
          protected List<IChunkMetadata> loadObjectByKey(Pair<IDeviceID, String> key)
              throws IOException {
            return loadChunkMetadata(key);
          }
        };
  }

  @Override
  public List<IChunkMetadata> getChunkMetaDataList(Path timeseriesPath) throws IOException {
    return new ArrayList<>(
        deviceIdChunkMetadataCache.get(
            new Pair<>(timeseriesPath.getIDeviceID(), timeseriesPath.getMeasurement())));
  }

  public List<List<IChunkMetadata>> getChunkMetadataLists(
      IDeviceID deviceID, Set<String> measurementNames, MetadataIndexNode measurementNode)
      throws IOException {
    List<List<IChunkMetadata>> results = new ArrayList<>(measurementNames.size());
    final Iterator<String> iterator = measurementNames.iterator();
    // use cache when possible
    while (iterator.hasNext()) {
      final String measurementName = iterator.next();
      // check first to avoid loading
      final Pair<IDeviceID, String> key = new Pair<>(deviceID, measurementName);
      if (deviceIdChunkMetadataCache.containsKey(key)) {
        final List<IChunkMetadata> metadataList = deviceIdChunkMetadataCache.get(key);
        results.add(metadataList);
        iterator.remove();
      }
    }
    // the remaining is not in the cache, search them in file
    final List<List<IChunkMetadata>> iChunkMetadataList =
        tsFileReader.getIChunkMetadataList(deviceID, measurementNames, measurementNode);
    for (List<IChunkMetadata> metadataList : iChunkMetadataList) {
      final String measurementUid = metadataList.get(0).getMeasurementUid();
      // cache the result
      deviceIdChunkMetadataCache.put(new Pair<>(deviceID, measurementUid), metadataList);
      results.add(metadataList);
    }
    return results;
  }

  @Override
  public Map<Path, List<IChunkMetadata>> getChunkMetaDataMap(List<Path> paths) throws IOException {
    Map<Path, List<IChunkMetadata>> chunkMetaDatas = new HashMap<>();
    for (Path path : paths) {
      if (!chunkMetaDatas.containsKey(path)) {
        chunkMetaDatas.put(path, new ArrayList<>());
      }
      chunkMetaDatas.get(path).addAll(getChunkMetaDataList(path));
    }
    return chunkMetaDatas;
  }

  @Override
  public TsFileMetadata getWholeFileMetadata() {
    return fileMetaData;
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void loadChunkMetaDatas(List<Path> paths) throws IOException {
    // group measurements by device
    TreeMap<IDeviceID, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      if (!deviceMeasurementsMap.containsKey(path.getIDeviceID())) {
        deviceMeasurementsMap.put(path.getIDeviceID(), new HashSet<>());
      }
      deviceMeasurementsMap.get(path.getIDeviceID()).add(path.getMeasurement());
    }
    int count = 0;
    boolean enough = false;
    for (Map.Entry<IDeviceID, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {
      if (enough) {
        break;
      }
      IDeviceID selectedDevice = deviceMeasurements.getKey();
      Set<String> selectedMeasurements = deviceMeasurements.getValue();
      List<IDeviceID> devices = this.tsFileReader.getAllDevices();
      IDeviceID[] deviceNames = devices.toArray(new IDeviceID[0]);
      if (Arrays.binarySearch(deviceNames, selectedDevice) < 0) {
        continue;
      }

      List<ITimeSeriesMetadata> timeseriesMetaDataList =
          tsFileReader.readITimeseriesMetadata(selectedDevice, selectedMeasurements, null, false);
      for (ITimeSeriesMetadata timeseriesMetadata : timeseriesMetaDataList) {
        List<IChunkMetadata> chunkMetadataList =
            tsFileReader.readIChunkMetaDataList(timeseriesMetadata);
        String measurementId;
        if (timeseriesMetadata instanceof AlignedTimeSeriesMetadata) {
          measurementId =
              ((AlignedTimeSeriesMetadata) timeseriesMetadata)
                  .getValueTimeseriesMetadataList()
                  .get(0)
                  .getMeasurementId();
        } else {
          measurementId = ((TimeseriesMetadata) timeseriesMetadata).getMeasurementId();
        }
        this.deviceIdChunkMetadataCache.put(
            new Pair<>(selectedDevice, measurementId), chunkMetadataList);
        count += chunkMetadataList.size();
        if (count == CACHED_ENTRY_NUMBER) {
          enough = true;
          break;
        }
      }
    }
  }

  @Override
  public TSDataType getDataType(Path path) throws IOException {
    if (tsFileReader.getChunkMetadataList(path) == null
        || tsFileReader.getChunkMetadataList(path).isEmpty()) {
      return null;
    }
    return tsFileReader.getChunkMetadataList(path).get(0).getDataType();
  }

  private List<IChunkMetadata> loadChunkMetadata(Path path) throws IOException {
    return tsFileReader.getIChunkMetadataList(path);
  }

  private List<IChunkMetadata> loadChunkMetadata(Pair<IDeviceID, String> key) throws IOException {
    return tsFileReader.getIChunkMetadataList(key.getLeft(), key.right);
  }

  @Override
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public List<TimeRange> convertSpace2TimePartition(
      List<Path> paths, long spacePartitionStartPos, long spacePartitionEndPos) throws IOException {
    if (spacePartitionStartPos > spacePartitionEndPos) {
      throw new IllegalArgumentException(
          "'spacePartitionStartPos' should not be larger than 'spacePartitionEndPos'.");
    }

    // (1) get timeRangesInCandidates and timeRangesBeforeCandidates by iterating
    // through the metadata
    ArrayList<TimeRange> timeRangesInCandidates = new ArrayList<>();
    ArrayList<TimeRange> timeRangesBeforeCandidates = new ArrayList<>();

    // group measurements by device

    TreeMap<IDeviceID, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      deviceMeasurementsMap
          .computeIfAbsent(path.getIDeviceID(), key -> new HashSet<>())
          .add(path.getMeasurement());
    }
    for (Map.Entry<IDeviceID, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {
      IDeviceID selectedDevice = deviceMeasurements.getKey();
      Set<String> selectedMeasurements = deviceMeasurements.getValue();

      // measurement -> ChunkMetadata list
      Map<String, List<ChunkMetadata>> seriesMetadatas =
          tsFileReader.readChunkMetadataInDevice(selectedDevice);

      for (Entry<String, List<ChunkMetadata>> seriesMetadata : seriesMetadatas.entrySet()) {

        if (!selectedMeasurements.contains(seriesMetadata.getKey())) {
          continue;
        }

        for (IChunkMetadata chunkMetadata : seriesMetadata.getValue()) {
          LocateStatus location =
              checkLocateStatus(chunkMetadata, spacePartitionStartPos, spacePartitionEndPos);
          if (location == LocateStatus.AFTER) {
            break;
          }

          if (location == LocateStatus.IN) {
            timeRangesInCandidates.add(
                new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          } else {
            timeRangesBeforeCandidates.add(
                new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime()));
          }
        }
      }
    }

    // (2) sort and merge the timeRangesInCandidates
    ArrayList<TimeRange> timeRangesIn =
        new ArrayList<>(TimeRange.sortAndMerge(timeRangesInCandidates));
    if (timeRangesIn.isEmpty()) {
      return Collections.emptyList(); // return an empty list
    }

    // (3) sort and merge the timeRangesBeforeCandidates
    ArrayList<TimeRange> timeRangesBefore =
        new ArrayList<>(TimeRange.sortAndMerge(timeRangesBeforeCandidates));

    // (4) calculate the remaining time ranges
    List<TimeRange> resTimeRanges = new ArrayList<>();
    for (TimeRange in : timeRangesIn) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(timeRangesBefore));
      resTimeRanges.addAll(remains);
    }

    return resTimeRanges;
  }

  /**
   * Check the location of a given chunkGroupMetaData with respect to a space partition constraint.
   *
   * @param chunkMetaData the given chunkMetaData
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return LocateStatus
   */
  public static LocateStatus checkLocateStatus(
      IChunkMetadata chunkMetaData, long spacePartitionStartPos, long spacePartitionEndPos) {
    long startOffsetOfChunk = chunkMetaData.getOffsetOfChunkHeader();
    if (spacePartitionStartPos <= startOffsetOfChunk && startOffsetOfChunk < spacePartitionEndPos) {
      return LocateStatus.IN;
    } else if (startOffsetOfChunk < spacePartitionStartPos) {
      return LocateStatus.BEFORE;
    } else {
      return LocateStatus.AFTER;
    }
  }

  @Override
  public void clear() {
    deviceIdChunkMetadataCache.clear();
  }

  @Override
  public Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceIterator(
      MetadataIndexNode root, ExpressionTree idFilter) {
    return new DeviceMetaIterator(tsFileReader, root, idFilter);
  }
}
