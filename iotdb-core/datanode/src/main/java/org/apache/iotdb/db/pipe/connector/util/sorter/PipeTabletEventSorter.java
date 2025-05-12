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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.LocalDate;

public class PipeTabletEventSorter {

  public static void sortAndDeduplicateValuesAndBitMaps(
      final Tablet tablet, final int deduplicatedSize, final Integer[] index) {
    int columnIndex = 0;
    for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
      final IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema != null) {
        BitMap deduplicatedBitMap = null;
        BitMap originalBitMap = null;
        if (tablet.getBitMaps() != null && tablet.getBitMaps()[columnIndex] != null) {
          originalBitMap = tablet.getBitMaps()[columnIndex];
          deduplicatedBitMap = new BitMap(originalBitMap.getSize());
        }

        tablet.getValues()[columnIndex] =
            PipeTabletEventSorter.reorderValueListAndBitMap(
                deduplicatedSize,
                tablet.getValues()[columnIndex],
                schema.getType(),
                originalBitMap,
                deduplicatedBitMap,
                index);

        if (tablet.getBitMaps() != null && tablet.getBitMaps()[columnIndex] != null) {
          tablet.getBitMaps()[columnIndex] = deduplicatedBitMap;
        }
        columnIndex++;
      }
    }
  }

  public static Object reorderValueListAndBitMap(
      final int deduplicatedSize,
      final Object valueList,
      final TSDataType dataType,
      final BitMap originalBitMap,
      final BitMap deduplicatedBitMap,
      final Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        final boolean[] boolValues = (boolean[]) valueList;
        final boolean[] deduplicatedBoolValues = new boolean[boolValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedBoolValues[i] =
              boolValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedBoolValues;
      case INT32:
        final int[] intValues = (int[]) valueList;
        final int[] deduplicatedIntValues = new int[intValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedIntValues[i] =
              intValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedIntValues;
      case DATE:
        final LocalDate[] dateValues = (LocalDate[]) valueList;
        final LocalDate[] deduplicatedDateValues = new LocalDate[dateValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedDateValues[i] =
              dateValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedDateValues;
      case INT64:
      case TIMESTAMP:
        final long[] longValues = (long[]) valueList;
        final long[] deduplicatedLongValues = new long[longValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedLongValues[i] =
              longValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedLongValues;
      case FLOAT:
        final float[] floatValues = (float[]) valueList;
        final float[] deduplicatedFloatValues = new float[floatValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedFloatValues[i] =
              floatValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedFloatValues;
      case DOUBLE:
        final double[] doubleValues = (double[]) valueList;
        final double[] deduplicatedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedDoubleValues[i] =
              doubleValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedDoubleValues;
      case TEXT:
      case BLOB:
      case STRING:
        final Binary[] binaryValues = (Binary[]) valueList;
        final Binary[] deduplicatedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < deduplicatedSize; i++) {
          deduplicatedBinaryValues[i] =
              binaryValues[getLastNonnullIndex(i, index, originalBitMap, deduplicatedBitMap)];
        }
        return deduplicatedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  private static int getLastNonnullIndex(
      final int i,
      final Integer[] index,
      final BitMap originalBitMap,
      final BitMap deduplicatedBitMap) {
    if (originalBitMap == null) {
      return index[i];
    }
    int lastNonnullIndex = index[i];
    int lastIndex = i > 0 ? index[i - 1] : -1;
    while (originalBitMap.isMarked(lastNonnullIndex)) {
      --lastNonnullIndex;
      if (lastNonnullIndex == lastIndex) {
        deduplicatedBitMap.mark(i);
        break;
      }
    }
    return lastNonnullIndex;
  }
}
