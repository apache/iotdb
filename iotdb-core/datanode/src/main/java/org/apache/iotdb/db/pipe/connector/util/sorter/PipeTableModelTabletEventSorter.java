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

package org.apache.iotdb.db.pipe.connector.util.sorter;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeTableModelTabletEventSorter {

  private final Tablet tablet;

  private Integer[] index;
  private boolean isUnSorted = false;
  private boolean hasDuplicates = false;
  private int deduplicatedSize;
  private int initIndexSize;

  public PipeTableModelTabletEventSorter(final Tablet tablet) {
    this.tablet = tablet;
    deduplicatedSize = tablet == null ? 0 : tablet.getRowSize();
  }

  /**
   * For the sorting and deduplication needs of the table model tablet, it is done according to the
   * {@link IDeviceID}. For sorting, it is necessary to sort the {@link IDeviceID} first, and then
   * sort by time. Deduplication is to remove the same timestamp in the same {@link IDeviceID}, and
   * the same timestamp in different {@link IDeviceID} will not be processed.
   */
  public void sortAndDeduplicateByDevIdTimestamp() {
    if (tablet == null || tablet.getRowSize() < 1) {
      return;
    }

    HashMap<IDeviceID, List<Pair<Integer, Integer>>> deviceIDToIndexMap = new HashMap<>();
    final long[] timestamps = tablet.timestamps;

    IDeviceID lastDevice = tablet.getDeviceID(0);
    long previousTimestamp = tablet.timestamps[0];
    int lasIndex = 0;
    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final IDeviceID deviceID = tablet.getDeviceID(i);
      final long currentTimestamp = timestamps[i];
      final int deviceComparison = deviceID.compareTo(lastDevice);
      if (deviceComparison == 0) {
        if (previousTimestamp == currentTimestamp) {
          hasDuplicates = true;
          continue;
        }
        if (previousTimestamp > currentTimestamp) {
          isUnSorted = true;
        }
        previousTimestamp = currentTimestamp;
        continue;
      }
      if (deviceComparison < 0) {
        isUnSorted = true;
      }

      List<Pair<Integer, Integer>> list =
          deviceIDToIndexMap.computeIfAbsent(lastDevice, k -> new ArrayList<>());

      if (!list.isEmpty()) {
        isUnSorted = true;
      }
      list.add(new Pair<>(lasIndex, i));
      lastDevice = deviceID;
      lasIndex = i;
      previousTimestamp = currentTimestamp;
    }

    List<Pair<Integer, Integer>> list =
        deviceIDToIndexMap.computeIfAbsent(lastDevice, k -> new ArrayList<>());
    if (!list.isEmpty()) {
      isUnSorted = true;
    }
    list.add(new Pair<>(lasIndex, tablet.getRowSize()));

    if (!isUnSorted && !hasDuplicates) {
      return;
    }

    initIndexSize = 0;
    deduplicatedSize = 0;
    index = new Integer[tablet.getRowSize()];
    deviceIDToIndexMap.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              final int start = initIndexSize;
              int i = initIndexSize;
              for (Pair<Integer, Integer> pair : entry.getValue()) {
                for (int j = pair.left; j < pair.right; j++) {
                  index[i++] = j;
                }
              }
              if (isUnSorted) {
                sortTimestamps(start, i);
                deduplicateTimestamps(start, i);
                initIndexSize = i;
                return;
              }

              if (hasDuplicates) {
                deduplicateTimestamps(start, i);
              }
              initIndexSize = i;
            });

    sortAndDeduplicateValuesAndBitMaps();
  }

  private void sortAndDeduplicateValuesAndBitMaps() {
    int columnIndex = 0;
    tablet.timestamps =
        (long[])
            PipeTabletEventSorter.reorderValueList(
                deduplicatedSize, tablet.timestamps, TSDataType.TIMESTAMP, index);
    for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
      final IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        tablet.values[columnIndex] =
            PipeTabletEventSorter.reorderValueList(
                deduplicatedSize, tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] =
              PipeTabletEventSorter.reorderBitMap(
                  deduplicatedSize, tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      }
    }

    tablet.setRowSize(deduplicatedSize);
  }

  private void sortTimestamps(final int startIndex, final int endIndex) {
    Arrays.sort(
        this.index, startIndex, endIndex, Comparator.comparingLong(i -> tablet.timestamps[i]));
  }

  private void deduplicateTimestamps(final int startIndex, final int endIndex) {
    long lastTime = tablet.timestamps[index[startIndex]];
    index[deduplicatedSize++] = index[startIndex];
    for (int i = startIndex + 1; i < endIndex; i++) {
      if (lastTime != (lastTime = tablet.timestamps[index[i]])) {
        index[deduplicatedSize++] = index[i];
      }
    }
  }

  /** Sort by time only, and remove only rows with the same DeviceID and time. */
  public void sortAndDeduplicateByTimestampIfNecessary() {
    if (tablet == null || tablet.getRowSize() == 0) {
      return;
    }

    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final long currentTimestamp = tablet.timestamps[i];
      final long previousTimestamp = tablet.timestamps[i - 1];

      if (currentTimestamp < previousTimestamp) {
        isUnSorted = true;
        break;
      }
      if (currentTimestamp == previousTimestamp) {
        hasDuplicates = true;
      }
    }

    if (!isUnSorted && !hasDuplicates) {
      return;
    }

    index = new Integer[tablet.getRowSize()];
    for (int i = 0, size = tablet.getRowSize(); i < size; i++) {
      index[i] = i;
    }

    if (isUnSorted) {
      sortTimestamps();

      // Do deduplicate anyway.
      // isDeduplicated may be false positive when isUnSorted is true.
      deduplicateTimestamps();
      hasDuplicates = false;
    }

    if (hasDuplicates) {
      deduplicateTimestamps();
    }

    sortAndDeduplicateValuesAndBitMapsIgnoreTimestamp();
  }

  private void sortTimestamps() {
    Arrays.sort(this.index, Comparator.comparingLong(i -> tablet.timestamps[i]));
    Arrays.sort(tablet.timestamps, 0, tablet.getRowSize());
  }

  private void deduplicateTimestamps() {
    deduplicatedSize = 1;
    long lastTime = tablet.timestamps[0];
    IDeviceID deviceID = tablet.getDeviceID(index[0]);
    final Set<IDeviceID> deviceIDSet = new HashSet<>();
    deviceIDSet.add(deviceID);
    for (int i = 1, size = tablet.getRowSize(); i < size; i++) {
      deviceID = tablet.getDeviceID(index[i]);
      if ((lastTime == (lastTime = tablet.timestamps[i]))) {
        if (!deviceIDSet.contains(deviceID)) {
          tablet.timestamps[deduplicatedSize] = lastTime;
          index[deduplicatedSize++] = index[i];
          deviceIDSet.add(deviceID);
        }
      } else {
        tablet.timestamps[deduplicatedSize] = lastTime;
        index[deduplicatedSize++] = index[i];
        deviceIDSet.clear();
        deviceIDSet.add(deviceID);
      }
    }
  }

  private void sortAndDeduplicateValuesAndBitMapsIgnoreTimestamp() {
    int columnIndex = 0;
    for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
      final IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        tablet.values[columnIndex] =
            PipeTabletEventSorter.reorderValueList(
                deduplicatedSize, tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] =
              PipeTabletEventSorter.reorderBitMap(
                  deduplicatedSize, tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      }
    }

    tablet.setRowSize(deduplicatedSize);
  }
}
