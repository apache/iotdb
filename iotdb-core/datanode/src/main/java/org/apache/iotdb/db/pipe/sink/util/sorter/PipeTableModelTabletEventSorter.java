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

package org.apache.iotdb.db.pipe.sink.util.sorter;

import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

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

public class PipeTableModelTabletEventSorter extends PipeInsertEventSorter {
  private int initIndexSize;

  /**
   * Constructor for Tablet.
   *
   * @param tablet the tablet to sort
   */
  public PipeTableModelTabletEventSorter(final Tablet tablet) {
    super(tablet);
    deDuplicatedSize = tablet == null ? 0 : tablet.getRowSize();
  }

  /**
   * Constructor for InsertTabletStatement.
   *
   * @param statement the insert tablet statement to sort
   */
  public PipeTableModelTabletEventSorter(final InsertTabletStatement statement) {
    super(statement);
    deDuplicatedSize = statement == null ? 0 : statement.getRowCount();
  }

  /**
   * For the sorting and deduplication needs of the table model tablet, it is done according to the
   * {@link IDeviceID}. For sorting, it is necessary to sort the {@link IDeviceID} first, and then
   * sort by time. Deduplication is to remove the same timestamp in the same {@link IDeviceID}, and
   * the same timestamp in different {@link IDeviceID} will not be processed.
   */
  public void sortAndDeduplicateByDevIdTimestamp() {
    if (dataAdapter == null || dataAdapter.getRowSize() < 1) {
      return;
    }

    final HashMap<IDeviceID, List<Pair<Integer, Integer>>> deviceIDToIndexMap = new HashMap<>();
    final long[] timestamps = dataAdapter.getTimestamps();
    final int rowSize = dataAdapter.getRowSize();

    IDeviceID lastDevice = dataAdapter.getDeviceID(0);
    long previousTimestamp = dataAdapter.getTimestamp(0);
    int lasIndex = 0;
    for (int i = 1; i < rowSize; ++i) {
      final IDeviceID deviceID = dataAdapter.getDeviceID(i);
      final long currentTimestamp = timestamps[i];
      final int deviceComparison = deviceID.compareTo(lastDevice);
      if (deviceComparison == 0) {
        if (previousTimestamp == currentTimestamp) {
          isDeDuplicated = false;
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
    list.add(new Pair<>(lasIndex, rowSize));

    if (isSorted && isDeDuplicated) {
      return;
    }

    initIndexSize = 0;
    deDuplicatedSize = 0;
    index = new Integer[rowSize];
    deDuplicatedIndex = new int[rowSize];
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

              if (!isDeDuplicated) {
                deDuplicateTimestamps(initIndexSize, i);
              }
              initIndexSize = i;
            });

    sortAndDeduplicateValuesAndBitMapsWithTimestamp();
  }

  private void sortAndDeduplicateValuesAndBitMapsWithTimestamp() {
    // TIMESTAMP is not a DATE type, so columnIndex is not relevant here, use -1
    dataAdapter.setTimestamps(
        (long[])
            reorderValueListAndBitMap(
                dataAdapter.getTimestamps(), TSDataType.TIMESTAMP, -1, null, null));
    sortAndMayDeduplicateValuesAndBitMaps();
    dataAdapter.setRowSize(deDuplicatedSize);
  }

  private void sortTimestamps(final int startIndex, final int endIndex) {
    Arrays.sort(
        this.index, startIndex, endIndex, Comparator.comparingLong(dataAdapter::getTimestamp));
  }

  private void deDuplicateTimestamps(final int startIndex, final int endIndex) {
    final long[] timestamps = dataAdapter.getTimestamps();
    long lastTime = timestamps[index[startIndex]];
    for (int i = startIndex + 1; i < endIndex; i++) {
      if (lastTime != (lastTime = timestamps[index[i]])) {
        deDuplicatedIndex[deDuplicatedSize++] = i - 1;
      }
    }
    deDuplicatedIndex[deDuplicatedSize++] = endIndex - 1;
  }

  /** Sort by time only. */
  public void sortByTimestampIfNecessary() {
    if (dataAdapter == null || dataAdapter.getRowSize() == 0) {
      return;
    }

    final long[] timestamps = dataAdapter.getTimestamps();
    final int rowSize = dataAdapter.getRowSize();
    for (int i = 1; i < rowSize; ++i) {
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

    index = new Integer[rowSize];
    for (int i = 0; i < rowSize; i++) {
      index[i] = i;
    }

    if (!isSorted) {
      sortTimestamps();
    }

    sortAndMayDeduplicateValuesAndBitMaps();
  }

  private void sortTimestamps() {
    Arrays.sort(this.index, Comparator.comparingLong(dataAdapter::getTimestamp));
    final long[] timestamps = dataAdapter.getTimestamps();
    Arrays.sort(timestamps, 0, dataAdapter.getRowSize());
  }
}
