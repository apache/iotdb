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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;
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
    deduplicatedSize = tablet == null ? 0 : tablet.rowSize;
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
  public List<Pair<IDeviceID, Integer>> deduplicateAndSortTimestampsIfNecessary() {
    if (tablet == null || tablet.rowSize < 1) {
      return null;
    }

    deviceIDTimeIndexList = new ArrayList<>();
    HashMap<IDeviceID, Integer> deviceIDToIndexMap = new HashMap<>();
    final long[] timestamps = tablet.timestamps;

    IDeviceID lastDevice = tablet.getDeviceID(0);
    long previousTimestamp = tablet.timestamps[0];
    int lasIndex = 0;
    for (int i = 1, size = tablet.rowSize; i < size; ++i) {
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
    updateDeviceIDIndex(deviceIDToIndexMap, lastDevice, lasIndex, tablet.rowSize);

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
    final long[] timestamps = new long[tablet.rowSize];
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
            reorderValueList(deduplicatedSize, tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] =
              reorderBitMap(deduplicatedSize, tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      }
    }
    tablet.rowSize = deduplicatedSize;
  }

  private static Object reorderValueList(
      final int deduplicatedSize,
      final Object valueList,
      final TSDataType dataType,
      final Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        final boolean[] boolValues = (boolean[]) valueList;
        final boolean[] deduplicatedBoolValues = new boolean[boolValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedBoolValues[i] = boolValues[index[i]];
        }
        return deduplicatedBoolValues;
      case INT32:
        final int[] intValues = (int[]) valueList;
        final int[] deduplicatedIntValues = new int[intValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedIntValues[i] = intValues[index[i]];
        }
        return deduplicatedIntValues;
      case DATE:
        final LocalDate[] dateValues = (LocalDate[]) valueList;
        final LocalDate[] deduplicatedDateValues = new LocalDate[dateValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedDateValues[i] = dateValues[index[i]];
        }
        return deduplicatedDateValues;
      case INT64:
      case TIMESTAMP:
        final long[] longValues = (long[]) valueList;
        final long[] deduplicatedLongValues = new long[longValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedLongValues[i] = longValues[index[i]];
        }
        return deduplicatedLongValues;
      case FLOAT:
        final float[] floatValues = (float[]) valueList;
        final float[] deduplicatedFloatValues = new float[floatValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedFloatValues[i] = floatValues[index[i]];
        }
        return deduplicatedFloatValues;
      case DOUBLE:
        final double[] doubleValues = (double[]) valueList;
        final double[] deduplicatedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedDoubleValues[i] = doubleValues[index[i]];
        }
        return deduplicatedDoubleValues;
      case TEXT:
      case BLOB:
      case STRING:
        final Binary[] binaryValues = (Binary[]) valueList;
        final Binary[] deduplicatedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedBinaryValues[i] = binaryValues[index[i]];
        }
        return deduplicatedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  private static BitMap reorderBitMap(
      final int deduplicatedSize, final BitMap bitMap, final Integer[] index) {
    final BitMap deduplicatedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < deduplicatedSize; i++) {
      if (bitMap.isMarked(index[i])) {
        deduplicatedBitMap.mark(i);
      }
    }
    return deduplicatedBitMap;
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
    index = new Integer[tablet.rowSize];
    for (int i = 0, size = tablet.rowSize; i < size; i++) {
      index[i] = i;
    }
  }
}
