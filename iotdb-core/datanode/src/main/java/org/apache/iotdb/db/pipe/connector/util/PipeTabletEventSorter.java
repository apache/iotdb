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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;

public class PipeTabletEventSorter {

  private final Tablet tablet;

  private boolean isSorted = true;
  private boolean isDeDuplicated = true;

  private Integer[] index;
  private int[] deDuplicatedIndex;
  private int deDuplicatedSize;

  public PipeTabletEventSorter(final Tablet tablet) {
    this.tablet = tablet;
    deDuplicatedSize = tablet == null ? 0 : tablet.rowSize;
  }

  public void deduplicateAndSortTimestampsIfNecessary() {
    if (tablet == null || tablet.rowSize == 0) {
      return;
    }

    for (int i = 1, size = tablet.rowSize; i < size; ++i) {
      final long currentTimestamp = tablet.timestamps[i];
      final long previousTimestamp = tablet.timestamps[i - 1];

      if (currentTimestamp < previousTimestamp) {
        isSorted = false;
      }
      if (currentTimestamp == previousTimestamp) {
        isDeDuplicated = false;
      }

      if (!isSorted && !isDeDuplicated) {
        break;
      }
    }

    if (isSorted && isDeDuplicated) {
      return;
    }

    index = new Integer[tablet.rowSize];
    deDuplicatedIndex = new int[tablet.rowSize];
    for (int i = 0, size = tablet.rowSize; i < size; i++) {
      index[i] = i;
    }

    if (!isSorted) {
      sortTimestamps();

      // Do deduplicate anyway.
      // isDeDuplicated may be false positive when isSorted is false.
      deduplicateTimestamps();
      isDeDuplicated = true;
    }

    if (!isDeDuplicated) {
      deduplicateTimestamps();
    }

    sortAndMayDeduplicateValuesAndBitMaps();
  }

  private void sortTimestamps() {
    Arrays.sort(index, Comparator.comparingLong(i -> tablet.timestamps[i]));
    Arrays.sort(tablet.timestamps, 0, tablet.rowSize);
  }

  private void deduplicateTimestamps() {
    deDuplicatedSize = 0;
    long[] timestamps = tablet.timestamps;
    for (int i = 1, size = tablet.rowSize; i < size; i++) {
      if (timestamps[i] != timestamps[i - 1]) {
        deDuplicatedIndex[deDuplicatedSize] = i - 1;
        timestamps[deDuplicatedSize] = timestamps[i - 1];

        ++deDuplicatedSize;
      }
    }

    deDuplicatedIndex[deDuplicatedSize] = tablet.rowSize - 1;
    timestamps[deDuplicatedSize] = timestamps[tablet.rowSize - 1];
    tablet.rowSize = ++deDuplicatedSize;
  }

  // Input:
  // Col: [1, null, 3, 6, null]
  // Timestamp: [2, 1, 1, 1, 1]
  // Intermediate:
  // Index: [1, 2, 3, 4, 0]
  // SortedTimestamp: [1, 2]
  // DeduplicateIndex: [3, 4]
  // Output:
  // (Used index: [2(3), 4(0)])
  // Col: [6, 1]
  private void sortAndMayDeduplicateValuesAndBitMaps() {
    int columnIndex = 0;
    for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
      final IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        BitMap deDuplicatedBitMap = null;
        BitMap originalBitMap = null;
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          originalBitMap = tablet.bitMaps[columnIndex];
          deDuplicatedBitMap = new BitMap(originalBitMap.getSize());
        }

        tablet.values[columnIndex] =
            reorderValueListAndBitMap(
                tablet.values[columnIndex], schema.getType(), originalBitMap, deDuplicatedBitMap);

        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] = deDuplicatedBitMap;
        }
        columnIndex++;
      }
    }
  }

  private Object reorderValueListAndBitMap(
      final Object valueList,
      final TSDataType dataType,
      final BitMap originalBitMap,
      final BitMap deDuplicatedBitMap) {
    switch (dataType) {
      case BOOLEAN:
        final boolean[] boolValues = (boolean[]) valueList;
        final boolean[] deDuplicatedBoolValues = new boolean[boolValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedBoolValues[i] =
              boolValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedBoolValues;
      case INT32:
        final int[] intValues = (int[]) valueList;
        final int[] deDuplicatedIntValues = new int[intValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedIntValues[i] =
              intValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedIntValues;
      case DATE:
        final LocalDate[] dateValues = (LocalDate[]) valueList;
        final LocalDate[] deDuplicatedDateValues = new LocalDate[dateValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedDateValues[i] =
              dateValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedDateValues;
      case INT64:
      case TIMESTAMP:
        final long[] longValues = (long[]) valueList;
        final long[] deDuplicatedLongValues = new long[longValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedLongValues[i] =
              longValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedLongValues;
      case FLOAT:
        final float[] floatValues = (float[]) valueList;
        final float[] deDuplicatedFloatValues = new float[floatValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedFloatValues[i] =
              floatValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedFloatValues;
      case DOUBLE:
        final double[] doubleValues = (double[]) valueList;
        final double[] deDuplicatedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedDoubleValues[i] =
              doubleValues[
                  getLastNonnullIndex(
                      i, index, deDuplicatedIndex, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedDoubleValues;
      case TEXT:
      case BLOB:
      case STRING:
        final Binary[] binaryValues = (Binary[]) valueList;
        final Binary[] deDuplicatedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedBinaryValues[i] =
              binaryValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  private int getLastNonnullIndex(
      final int i, final BitMap originalBitMap, final BitMap deDuplicatedBitMap) {
    if (originalBitMap == null) {
      return index[deDuplicatedIndex[i]];
    }
    int lastNonnullIndex = deDuplicatedIndex[i];
    int lastIndex = i > 0 ? deDuplicatedIndex[i - 1] : -1;
    while (originalBitMap.isMarked(index[lastNonnullIndex])) {
      --lastNonnullIndex;
      if (lastNonnullIndex == lastIndex) {
        deDuplicatedBitMap.mark(i);
        return index[lastNonnullIndex + 1];
      }
    }
    return index[lastNonnullIndex];
  }
}
