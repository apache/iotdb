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

package org.apache.iotdb.db.pipe.connector.util;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class PipeTableModelTabletEventSorter {

  private final Tablet tablet;

  private Integer[] index;
  private boolean isUnSorted = false;
  private boolean hasDuplicates = false;
  private int deduplicatedSize;

  private List<Pair<IDeviceID, Integer>> deviceIDTimeIndexList;

  public PipeTableModelTabletEventSorter(final Tablet tablet) {
    this.tablet = tablet;
    deduplicatedSize = tablet == null ? 0 : tablet.getRowSize();
  }

  /**
   * For the sorting and deduplication needs of the table model tablet, it is done according to the
   * {@link IDeviceID}. For sorting, it is necessary to sort the {@link IDeviceID} first, and then
   * sort by time. Deduplication is to remove the same timestamp in the same {@link IDeviceID}, and
   * the same timestamp in different {@link IDeviceID} will not be processed.
   *
   * @return A list of pairs, each containing an instance of {@link IDeviceID} and the corresponding
   *     last index in the Tablet.
   */
  public List<Pair<IDeviceID, Integer>> sortAndDeduplicateByDevIdTimestamp() {
    if (tablet == null || tablet.getRowSize() < 1) {
      return null;
    }

    // The deviceIDTimeIndexList stores the last displacement of each DeviceID + 1, such as
    // [A1,A1,B1,B1,A2,A2,A3], then the deviceIDTimeIndexList is [(A1,2),(B1,4),(A2,6),(A3,7)]
    deviceIDTimeIndexList = new ArrayList<>();

    // The deviceIDToIndexMap stores the index of each DeviceID value in the List.
    HashMap<IDeviceID, Integer> deviceIDToIndexMap = new HashMap<>();
    final long[] timestamps = tablet.timestamps;

    IDeviceID lastDevice = tablet.getDeviceID(0);
    long previousTimestamp = tablet.timestamps[0];
    int lasIndex = 0;
    // It is necessary to determine whether reordering and deduplication are required, and after the
    // loop is completed, it is necessary to ensure that the displacement of each DeviceID is
    // continuous on the Index. For example, the Index array obtained from
    // [A1,A2,A3,A4,A2,A2,A4,A4],[0,1,2,3,4,5,6,7] becomes
    // [A1,A2,A2,A2,A3,A4,A4,A4],[0,1,4,5,2,3,6,7],
    //
    // Tablets need to be sorted under the following conditions: 1. The timestamps of the same
    // DeviceID are out of order 2. The same DeviceID is not continuous 3. The DeviceID is out of
    // order
    //
    // Tablets need to be deduplicated under the following conditions:
    // If Tablets need to be sorted, they must be deduplicated. The same DeviceID with the same time
    // needs to be deduplicated
    for (int i = 1, size = tablet.getRowSize(); i < size; ++i) {
      final IDeviceID deviceID = tablet.getDeviceID(i);
      final long currentTimestamp = timestamps[i];
      final int deviceComparison = deviceID.compareTo(lastDevice);
      if (deviceComparison == 0) {
        if (previousTimestamp > currentTimestamp) {
          isUnSorted = true;
          continue;
        }
        if (previousTimestamp == currentTimestamp) {
          hasDuplicates = true;
        }
        previousTimestamp = currentTimestamp;
        continue;
      }
      if (deviceComparison < 0) {
        isUnSorted = true;
      }
      updateDeviceIDIndex(deviceIDToIndexMap, lastDevice, lasIndex, i);
      lastDevice = deviceID;
      lasIndex = i;
      previousTimestamp = currentTimestamp;
    }
    updateDeviceIDIndex(deviceIDToIndexMap, lastDevice, lasIndex, tablet.getRowSize());

    if (!isUnSorted && !hasDuplicates) {
      return deviceIDTimeIndexList;
    }

    initIndex();

    if (isUnSorted) {
      sortAndDeduplicateTimestamps();
      hasDuplicates = false;
      isUnSorted = false;
    }

    if (hasDuplicates) {
      deduplicateTimestamps();
      hasDuplicates = false;
    }

    sortAndDeduplicateValuesAndBitMaps();
    return deviceIDTimeIndexList;
  }

  // This function sorts the tablets. It sorts the time under each IDeviceID first, then sorts each
  // IDevice, and then removes duplicates.
  private void sortAndDeduplicateTimestamps() {
    // Sorting the time of the same IDevice
    int startIndex = 0;
    final Comparator<Integer> comparator = Comparator.comparingLong(i -> tablet.timestamps[i]);
    List<Pair<IDeviceID, Pair<Integer, Integer>>> deviceIndexRange =
        new ArrayList<>(deviceIDTimeIndexList.size());
    for (Pair<IDeviceID, Integer> pair : deviceIDTimeIndexList) {
      Arrays.sort(this.index, startIndex, pair.right, comparator);
      deviceIndexRange.add(new Pair<>(pair.left, new Pair<>(startIndex, pair.right - 1)));
      startIndex = pair.right;
    }

    // Sort IDevices
    deviceIDTimeIndexList.clear();
    deviceIndexRange.sort(Comparator.comparing(a -> a.left));

    // Deduplication and update Index array
    final long[] timestamps = new long[tablet.getRowSize()];
    final long[] tabletTimestamps = tablet.timestamps;
    final Integer[] copyIndex = new Integer[index.length];

    deduplicatedSize = 0;
    for (Pair<IDeviceID, Pair<Integer, Integer>> deviceRange : deviceIndexRange) {
      startIndex = deviceRange.right.left;
      long lastTimestamps = timestamps[deduplicatedSize] = tabletTimestamps[index[startIndex]];
      copyIndex[deduplicatedSize++] = index[startIndex++];
      for (final int end = deviceRange.right.right; startIndex <= end; startIndex++) {
        final long curTimestamps = tabletTimestamps[index[startIndex]];
        if (lastTimestamps == curTimestamps) {
          continue;
        }
        lastTimestamps = timestamps[deduplicatedSize] = curTimestamps;
        copyIndex[deduplicatedSize++] = index[startIndex];
      }
      deviceIDTimeIndexList.add(new Pair<>(deviceRange.left, deduplicatedSize));
    }
    index = copyIndex;
    tablet.timestamps = timestamps;
  }

  private void deduplicateTimestamps() {
    int startIndex = 0;
    deduplicatedSize = 0;
    final long[] timestamps = tablet.timestamps;
    for (Pair<IDeviceID, Integer> pair : deviceIDTimeIndexList) {
      final int endIndex = pair.right;
      long lastTimestamps = timestamps[startIndex];
      timestamps[deduplicatedSize] = lastTimestamps;
      index[deduplicatedSize++] = index[startIndex++];
      for (; startIndex < endIndex; startIndex++) {
        final long curTimestamp = timestamps[startIndex];
        if (curTimestamp == lastTimestamps) {
          continue;
        }
        index[deduplicatedSize] = index[startIndex];
        timestamps[deduplicatedSize++] = curTimestamp;
        lastTimestamps = curTimestamp;
      }
      pair.right = deduplicatedSize;
    }
  }

  private void sortAndDeduplicateValuesAndBitMaps() {
    int columnIndex = 0;
    for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
      final IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        tablet.values[columnIndex] =
            TabletSortUtil.reorderValueList(
                deduplicatedSize, tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] =
              TabletSortUtil.reorderBitMap(deduplicatedSize, tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      }
    }
    tablet.setRowSize(deduplicatedSize);
  }

  private void updateDeviceIDIndex(
      HashMap<IDeviceID, Integer> deviceID2ListIndex, IDeviceID device, int start, int end) {
    final Integer index = deviceID2ListIndex.putIfAbsent(device, deviceIDTimeIndexList.size());
    if (Objects.isNull(index)) {
      deviceIDTimeIndexList.add(new Pair<>(device, end));
      return;
    }
    final Pair<IDeviceID, Integer> pair = deviceIDTimeIndexList.get(index);
    initIndex();
    shiftElements(this.index, start, end, pair.right);
    final int length = end - start;
    for (int j = index; j < deviceIDTimeIndexList.size(); j++) {
      final Pair<IDeviceID, Integer> var = deviceIDTimeIndexList.get(j);
      var.right = var.right + length;
    }
    isUnSorted = true;
  }

  private static void shiftElements(Integer[] arr, int start, int end, int index) {
    final int moveCount = end - start;
    Integer[] temp = new Integer[moveCount];
    System.arraycopy(arr, start, temp, 0, moveCount);
    for (int j = end - 1, first = index + moveCount; j >= first; j--) {
      arr[j] = arr[j - moveCount];
    }
    System.arraycopy(temp, 0, arr, index, moveCount);
  }

  private void initIndex() {
    if (Objects.nonNull(index)) {
      return;
    }
    index = new Integer[tablet.getRowSize()];
    for (int i = 0, size = tablet.getRowSize(); i < size; i++) {
      index[i] = i;
    }
  }
}
