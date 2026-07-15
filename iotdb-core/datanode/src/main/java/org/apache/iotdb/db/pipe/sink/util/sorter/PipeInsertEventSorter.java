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

import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;

import java.lang.reflect.Array;
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
      dataAdapter.setBitMaps(PipeTabletUtils.compactBitMaps(bitMaps, deDuplicatedSize));
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
    final Type type =
        TypeServices.PIPE_INSERT_EVENT_VALUE_LIST_TYPE_SERVICE
            .call(Type.fromTsDataType(dataType))
            .apply(dataType != TSDataType.DATE || dataAdapter.isDateStoredAsLocalDate(columnIndex));
    return reorderValueList(valueList, type, originalBitMap, deDuplicatedBitMap);
  }

  private Object reorderValueList(
      final Object valueList,
      final Type type,
      final BitMap originalBitMap,
      final BitMap deDuplicatedBitMap) {
    final Object deDuplicatedValues = type.createArray(Array.getLength(valueList));
    for (int i = 0; i < deDuplicatedSize; i++) {
      type.copyArrayElement(
          valueList,
          getLastNonnullIndex(i, originalBitMap, deDuplicatedBitMap),
          deDuplicatedValues,
          i);
    }
    return deDuplicatedValues;
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
