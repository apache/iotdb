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

import java.time.LocalDate;

public class PipeTabletEventSorter {

  public static Object reorderValueList(
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

  public static BitMap reorderBitMap(
      final int deduplicatedSize, final BitMap bitMap, final Integer[] index) {
    final BitMap deduplicatedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < deduplicatedSize; i++) {
      if (bitMap.isMarked(index[i])) {
        deduplicatedBitMap.mark(i);
      }
    }
    return deduplicatedBitMap;
  }
}
