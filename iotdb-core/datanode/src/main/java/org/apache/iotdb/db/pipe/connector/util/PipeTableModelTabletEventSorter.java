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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipeTableModelTabletEventSorter {

  private final Tablet tablet;

  private Integer[] index;
  private boolean isUnSorted = false;
  private boolean hasDuplicates = false;
  private int deduplicatedSize;
  private List<Pair<IDeviceID, Integer>> deviceID2TimeIndex;

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

    deviceID2TimeIndex = new ArrayList<>();
    final Set<IDeviceID> deviceIDSet = new HashSet<>();
    final long[] timestamps = tablet.timestamps;

    IDeviceID lastDevice = tablet.getDeviceID(0);
    deviceIDSet.add(lastDevice);

    for (int i = 1, size = tablet.rowSize; i < size; ++i) {
      final IDeviceID deviceID = tablet.getDeviceID(i);
      final long currentTimestamp = timestamps[i];
      final int deviceComparison = deviceID.compareTo(lastDevice);
      if (deviceComparison == 0) {
        final long previousTimestamp = tablet.timestamps[i - 1];
        if (previousTimestamp > currentTimestamp) {
          isUnSorted = true;
          break;
        }
        if (previousTimestamp == currentTimestamp) {
          hasDuplicates = true;
        }
        continue;
      }

      if (deviceComparison < 0 || deviceIDSet.contains(deviceID)) {
        isUnSorted = true;
        break;
      }
      deviceIDSet.add(deviceID);
      deviceID2TimeIndex.add(new Pair<>(lastDevice, i));
      lastDevice = deviceID;
    }

    if (!isUnSorted && !hasDuplicates) {
      deviceID2TimeIndex.add(new Pair<>(lastDevice, tablet.rowSize));
      return deviceID2TimeIndex;
    }

    deviceID2TimeIndex.clear();
    index = new Integer[tablet.rowSize];
    for (int i = 0, size = tablet.rowSize; i < size; i++) {
      index[i] = i;
    }

    if (!isUnSorted) {
      sortAndDeduplicateTimestamps();
      hasDuplicates = true;
    }

    if (!hasDuplicates) {
      deduplicateTimestamps();
    }

    sortAndDeduplicateValuesAndBitMaps();
    return deviceID2TimeIndex;
  }

  private void sortAndDeduplicateTimestamps() {
    Arrays.sort(
        index,
        (a, b) -> {
          final int deviceComparison = tablet.getDeviceID(a).compareTo(tablet.getDeviceID(b));
          return deviceComparison == 0
              ? Long.compare(tablet.timestamps[a], tablet.timestamps[b])
              : deviceComparison;
        });
    final long[] timestamps = new long[tablet.rowSize];
    final long[] tabletTimestamps = tablet.timestamps;
    timestamps[0] = tabletTimestamps[index[0]];
    IDeviceID lastDevice = tablet.getDeviceID(index[0]);
    deduplicatedSize = 1;

    for (int i = 1; i < timestamps.length; i++) {
      final int timeIndex = index[i];
      final IDeviceID deviceId = tablet.getDeviceID(timeIndex);
      if (!lastDevice.equals(deviceId)) {
        timestamps[deduplicatedSize] = tabletTimestamps[timeIndex];
        index[deduplicatedSize] = timeIndex;
        deviceID2TimeIndex.add(new Pair<>(lastDevice, deduplicatedSize));
        lastDevice = deviceId;
        deduplicatedSize++;
        continue;
      }

      if (timestamps[deduplicatedSize - 1] != timestamps[timeIndex]) {
        timestamps[deduplicatedSize] = tabletTimestamps[timeIndex];
        index[deduplicatedSize] = timeIndex;
        lastDevice = deviceId;
        deduplicatedSize++;
      }
    }
    deviceID2TimeIndex.add(new Pair<>(lastDevice, tablet.rowSize));
    tablet.timestamps = timestamps;
  }

  private void deduplicateTimestamps() {
    final long[] timestamps = tablet.timestamps;
    IDeviceID lastDevice = tablet.getDeviceID(0);
    deduplicatedSize = 1;
    for (int i = 1; i < timestamps.length; i++) {
      final IDeviceID deviceId = tablet.getDeviceID(i);
      if (!lastDevice.equals(deviceId)) {
        timestamps[deduplicatedSize] = timestamps[i];
        index[deduplicatedSize] = i;
        deviceID2TimeIndex.add(new Pair<>(lastDevice, i));
        lastDevice = deviceId;
        deduplicatedSize++;
        continue;
      }

      if (timestamps[deduplicatedSize - 1] != timestamps[i]) {
        timestamps[deduplicatedSize] = timestamps[i];
        index[deduplicatedSize] = i;
        lastDevice = deviceId;
        deduplicatedSize++;
      }
    }
    deviceID2TimeIndex.add(new Pair<>(lastDevice, tablet.rowSize));
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
  }

  private static Object reorderValueList(
      int deduplicatedSize,
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
      int deduplicatedSize, final BitMap bitMap, final Integer[] index) {
    final BitMap deduplicatedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < deduplicatedSize; i++) {
      if (bitMap.isMarked(index[i])) {
        deduplicatedBitMap.mark(i);
      }
    }
    return deduplicatedBitMap;
  }
}
