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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;

import java.time.LocalDate;
import java.util.Objects;

public class PipeInsertEventSorter {

  protected final InsertEventDataAdapter dataAdapter;

  protected Integer[] index;
  protected boolean isSorted = true;
  protected boolean isDeDuplicated = true;
  protected int[] deDuplicatedIndex;
  protected int deDuplicatedSize;

  /**
   * Constructor for Tablet.
   *
   * @param tablet the tablet to sort
   */
  public PipeInsertEventSorter(final Tablet tablet) {
    this.dataAdapter = new TabletAdapter(tablet);
  }

  /**
   * Constructor for InsertTabletStatement.
   *
   * @param statement the insert tablet statement to sort
   */
  public PipeInsertEventSorter(final InsertTabletStatement statement) {
    this.dataAdapter = new InsertTabletStatementAdapter(statement);
  }

  /**
   * Constructor with adapter (for internal use or advanced scenarios).
   *
   * @param adapter the data adapter
   */
  protected PipeInsertEventSorter(final InsertEventDataAdapter adapter) {
    this.dataAdapter = adapter;
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
  protected void sortAndMayDeduplicateValuesAndBitMaps() {
    final int columnCount = dataAdapter.getColumnCount();
    BitMap[] bitMaps = dataAdapter.getBitMaps();
    boolean bitMapsModified = false;

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      final TSDataType dataType = dataAdapter.getDataType(columnIndex);
      if (dataType != null) {
        BitMap deDuplicatedBitMap = null;
        BitMap originalBitMap = null;
        if (bitMaps != null && columnIndex < bitMaps.length && bitMaps[columnIndex] != null) {
          originalBitMap = bitMaps[columnIndex];
          deDuplicatedBitMap = new BitMap(originalBitMap.getSize());
        }

        final Object[] values = dataAdapter.getValues();
        final Object reorderedValue =
            reorderValueListAndBitMap(
                values[columnIndex], dataType, columnIndex, originalBitMap, deDuplicatedBitMap);
        dataAdapter.setValue(columnIndex, reorderedValue);

        if (bitMaps != null && columnIndex < bitMaps.length && bitMaps[columnIndex] != null) {
          bitMaps[columnIndex] = deDuplicatedBitMap;
          bitMapsModified = true;
        }
      }
    }

    if (bitMapsModified) {
      dataAdapter.setBitMaps(bitMaps);
    }
  }

  protected Object reorderValueListAndBitMap(
      final Object valueList,
      final TSDataType dataType,
      final int columnIndex,
      final BitMap originalBitMap,
      final BitMap deDuplicatedBitMap) {
    // Older version's sender may contain null values, we need to cover this case
    if (Objects.isNull(valueList)) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        final boolean[] boolValues = (boolean[]) valueList;
        final boolean[] deDuplicatedBoolValues = new boolean[boolValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedBoolValues[i] =
              boolValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedBoolValues;
      case INT32:
        final int[] intValues = (int[]) valueList;
        final int[] deDuplicatedIntValues = new int[intValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedIntValues[i] =
              intValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedIntValues;
      case DATE:
        // DATE type: Tablet uses LocalDate[], InsertTabletStatement uses int[]
        if (dataAdapter.isDateStoredAsLocalDate(columnIndex)) {
          // Tablet: LocalDate[]
          final LocalDate[] dateValues = (LocalDate[]) valueList;
          final LocalDate[] deDuplicatedDateValues = new LocalDate[dateValues.length];
          for (int i = 0; i < deDuplicatedSize; i++) {
            deDuplicatedDateValues[i] =
                dateValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
          }
          return deDuplicatedDateValues;
        } else {
          // InsertTabletStatement: int[]
          final int[] intDateValues = (int[]) valueList;
          final int[] deDuplicatedIntDateValues = new int[intDateValues.length];
          for (int i = 0; i < deDuplicatedSize; i++) {
            deDuplicatedIntDateValues[i] =
                intDateValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
          }
          return deDuplicatedIntDateValues;
        }
      case INT64:
      case TIMESTAMP:
        final long[] longValues = (long[]) valueList;
        final long[] deDuplicatedLongValues = new long[longValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedLongValues[i] =
              longValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedLongValues;
      case FLOAT:
        final float[] floatValues = (float[]) valueList;
        final float[] deDuplicatedFloatValues = new float[floatValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedFloatValues[i] =
              floatValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedFloatValues;
      case DOUBLE:
        final double[] doubleValues = (double[]) valueList;
        final double[] deDuplicatedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < deDuplicatedSize; i++) {
          deDuplicatedDoubleValues[i] =
              doubleValues[getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap)];
        }
        return deDuplicatedDoubleValues;
      case TEXT:
      case BLOB:
      case OBJECT:
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
    if (deDuplicatedIndex == null) {
      if (originalBitMap != null && originalBitMap.isMarked(index[i])) {
        deDuplicatedBitMap.mark(i);
      }
      return index[i];
    }
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
