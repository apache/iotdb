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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeTableModelTabletEventSorter extends PipeTabletEventSorter {
  private int initIndexSize;

  public PipeTableModelTabletEventSorter(final Tablet tablet) {
    super(tablet);
    deDuplicatedSize = tablet == null ? 0 : tablet.getRowSize();
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

    final HashMap<IDeviceID, List<Pair<Integer, Integer>>> deviceIDToIndexMap = new HashMap<>();
    final long[] timestamps = tablet.getTimestamps();

    IDeviceID lastDevice = tablet.getDeviceID(0);
    long previousTimestamp = tablet.getTimestamp(0);
    int lasIndex = 0;
    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final IDeviceID deviceID = tablet.getDeviceID(i);
      final long currentTimestamp = timestamps[i];
      final int deviceComparison = deviceID.compareTo(lastDevice);
      if (deviceComparison == 0) {
        if (previousTimestamp == currentTimestamp) {
          isDeduplicate = false;
          continue;
        }
        if (previousTimestamp > currentTimestamp) {
          isSorted = false;
        }
        previousTimestamp = currentTimestamp;
        continue;
      }
      if (deviceComparison < 0) {
        isSorted = false;
      }

      final List<Pair<Integer, Integer>> list =
          deviceIDToIndexMap.computeIfAbsent(lastDevice, k -> new ArrayList<>());

      if (!list.isEmpty()) {
        isSorted = false;
      }
      list.add(new Pair<>(lasIndex, i));
      lastDevice = deviceID;
      lasIndex = i;
      previousTimestamp = currentTimestamp;
    }

    final List<Pair<Integer, Integer>> list =
        deviceIDToIndexMap.computeIfAbsent(lastDevice, k -> new ArrayList<>());
    if (!list.isEmpty()) {
      isSorted = false;
    }
    list.add(new Pair<>(lasIndex, tablet.getRowSize()));

    if (isSorted && isDeduplicate) {
      return;
    }

    initIndexSize = 0;
    deDuplicatedSize = 0;
    index = new Integer[tablet.getRowSize()];
    deDuplicatedIndex = new int[tablet.getRowSize()];
    deviceIDToIndexMap.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              int i = initIndexSize;
              for (Pair<Integer, Integer> pair : entry.getValue()) {
                for (int j = pair.left; j < pair.right; j++) {
                  index[i++] = j;
                }
              }
              if (!isSorted) {
                sortTimestamps(initIndexSize, i);
                deDuplicateTimestamps(initIndexSize, i);
                initIndexSize = i;
                return;
              }

              if (!isDeduplicate) {
                deDuplicateTimestamps(initIndexSize, i);
              }
              initIndexSize = i;
            });

    sortAndDeduplicateValuesAndBitMapsWithTimestamp();
  }

  private void sortAndDeduplicateValuesAndBitMapsWithTimestamp() {
    tablet.setTimestamps(
        (long[])
            reorderValueListAndBitMap(tablet.getTimestamps(), TSDataType.TIMESTAMP, null, null));
    sortAndDeduplicateValuesAndBitMaps();
    tablet.setRowSize(deDuplicatedSize);
  }

  private void sortTimestamps(final int startIndex, final int endIndex) {
    Arrays.sort(this.index, startIndex, endIndex, Comparator.comparingLong(tablet::getTimestamp));
  }

  private void deDuplicateTimestamps(final int startIndex, final int endIndex) {
    final long[] timestamps = tablet.getTimestamps();
    long lastTime = timestamps[index[startIndex]];
    for (int i = startIndex + 1; i < endIndex; i++) {
      if (lastTime != (lastTime = timestamps[index[i]])) {
        deDuplicatedIndex[deDuplicatedSize++] = i - 1;
      }
    }
    deDuplicatedIndex[deDuplicatedSize++] = endIndex - 1;
  }

  /** Sort by time only, and remove only rows with the same DeviceID and time. */
  public void sortByTimestampIfNecessary() {
    if (tablet == null || tablet.getRowSize() == 0) {
      return;
    }

    final long[] timestamps = tablet.getTimestamps();
    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final long currentTimestamp = timestamps[i];
      final long previousTimestamp = timestamps[i - 1];

      if (currentTimestamp < previousTimestamp) {
        isSorted = false;
        break;
      }
    }

    if (isSorted) {
      return;
    }

    index = new Integer[tablet.getRowSize()];
    for (int i = 0, size = tablet.getRowSize(); i < size; i++) {
      index[i] = i;
    }

    if (!isSorted) {
      sortTimestamps();
    }

    sortAndDeduplicateValuesAndBitMaps();
  }

  private void sortTimestamps() {
    Arrays.sort(this.index, Comparator.comparingLong(tablet::getTimestamp));
    Arrays.sort(tablet.getTimestamps(), 0, tablet.getRowSize());
  }
}
