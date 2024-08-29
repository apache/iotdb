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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.utils.MemUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

public class PipeMemoryWeightUtil {

  /** Estimates memory usage of a {@link Map}<{@link IDeviceID}, {@link Boolean}>. */
  public static long memoryOfIDeviceId2Bool(Map<IDeviceID, Boolean> map) {
    long usageInBytes = 0L;
    for (Map.Entry<IDeviceID, Boolean> entry : map.entrySet()) {
      usageInBytes = usageInBytes + entry.getKey().ramBytesUsed() + 1L;
    }
    return usageInBytes + 16L; // add the overhead of map
  }

  /** Estimates memory usage of a {@link Map}<{@link String}, {@link TSDataType}>. */
  public static long memoryOfStr2TSDataType(Map<String, TSDataType> map) {
    long usageInBytes = 0L;
    for (Map.Entry<String, TSDataType> entry : map.entrySet()) {
      usageInBytes = usageInBytes + MemUtils.getStringMem(entry.getKey()) + 4L;
    }
    return usageInBytes + 16L; // add the overhead of map
  }

  /** Estimates memory usage of a {@link Map}<{@link IDeviceID}, {@link List}<{@link String}>>. */
  public static long memoryOfIDeviceID2StrList(Map<IDeviceID, List<String>> map) {
    long usageInBytes = 0L;
    for (Map.Entry<IDeviceID, List<String>> entry : map.entrySet()) {
      usageInBytes += entry.getKey().ramBytesUsed();
      for (String str : entry.getValue()) {
        usageInBytes += MemUtils.getStringMem(str);
      }
    }
    return usageInBytes + 16L; // add the overhead of map
  }

  /**
   * Given a row of a tablet, calculate the row count and memory cost of the pipe tablet that will
   * be constructed according to config.
   *
   * @return left is the row count of tablet, right is the memory cost of tablet in bytes
   */
  public static Pair<Integer, Integer> calculateTabletRowCountAndMemory(RowRecord row) {
    int totalSizeInBytes = 0;

    // timestamp
    totalSizeInBytes += 8L;

    // values
    final List<Field> fields = row.getFields();
    int schemaCount = 0;
    if (fields != null) {
      schemaCount = fields.size();
      for (final Field field : fields) {
        if (field == null) {
          continue;
        }

        final TSDataType tsDataType = field.getDataType();
        if (tsDataType == null) {
          continue;
        }

        if (tsDataType.isBinary()) {
          final Binary binary = field.getBinaryV();
          totalSizeInBytes += binary == null ? 0 : binary.getLength();
        } else {
          totalSizeInBytes += tsDataType.getDataTypeSize();
        }
      }
    }

    return calculateTabletRowCountAndMemoryBySize(totalSizeInBytes, schemaCount);
  }

  /**
   * Given a BatchData, calculate the row count and memory cost of the pipe tablet that will be
   * constructed according to config.
   *
   * @return left is the row count of tablet, right is the memory cost of tablet in bytes
   */
  public static Pair<Integer, Integer> calculateTabletRowCountAndMemory(BatchData batchData) {
    int totalSizeInBytes = 0;
    int schemaCount = 0;

    // timestamp
    totalSizeInBytes += 8L;

    // values
    final TSDataType type = batchData.getDataType();
    if (type != null) {
      if (type == TSDataType.VECTOR && batchData.getVector() != null) {
        schemaCount = batchData.getVector().length;
        for (int i = 0; i < schemaCount; ++i) {
          final TsPrimitiveType primitiveType = batchData.getVector()[i];
          if (primitiveType == null || primitiveType.getDataType() == null) {
            continue;
          }

          if (primitiveType.getDataType().isBinary()) {
            final Binary binary = primitiveType.getBinary();
            totalSizeInBytes += binary == null ? 0 : binary.getLength();
          } else {
            totalSizeInBytes += primitiveType.getDataType().getDataTypeSize();
          }
        }
      } else {
        schemaCount = 1;
        if (type.isBinary()) {
          final Binary binary = batchData.getBinary();
          totalSizeInBytes += binary == null ? 0 : binary.getLength();
        } else {
          totalSizeInBytes += type.getDataTypeSize();
        }
      }
    }

    return calculateTabletRowCountAndMemoryBySize(totalSizeInBytes, schemaCount);
  }

  /**
   * Given a row of a tablet, calculate the row count and memory cost of the pipe tablet that will
   * be constructed according to config.
   *
   * @return left is the row count of tablet, right is the memory cost of tablet in bytes
   */
  public static Pair<Integer, Integer> calculateTabletRowCountAndMemory(PipeRow row) {
    return calculateTabletRowCountAndMemoryBySize(row.getCurrentRowSize(), row.size());
  }

  private static Pair<Integer, Integer> calculateTabletRowCountAndMemoryBySize(
      int rowSize, int schemaCount) {
    if (rowSize <= 0) {
      return new Pair<>(1, 0);
    }

    // Calculate row number according to the max size of a pipe tablet.
    // "-100" is the estimated size of other data structures in a pipe tablet.
    // "*8" converts bytes to bits, because the bitmap size is 1 bit per schema.
    int rowNumber =
        8
            * (PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes() - 100)
            / (8 * rowSize + schemaCount);
    rowNumber = Math.max(1, rowNumber);

    if ( // This means the row number is larger than the max row count of a pipe tablet
    rowNumber > PipeConfig.getInstance().getPipeDataStructureTabletRowSize()) {
      // Bound the row number, the memory cost is rowSize * rowNumber
      return new Pair<>(
          PipeConfig.getInstance().getPipeDataStructureTabletRowSize(),
          rowSize * PipeConfig.getInstance().getPipeDataStructureTabletRowSize());
    } else {
      return new Pair<>(
          rowNumber, PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes());
    }
  }

  public static long calculateTabletSizeInBytes(Tablet tablet) {
    long totalSizeInBytes = 0;

    if (tablet == null) {
      return totalSizeInBytes;
    }

    // timestamps
    if (tablet.timestamps != null) {
      totalSizeInBytes += tablet.timestamps.length * 8L;
    }

    // values
    final List<IMeasurementSchema> timeseries = tablet.getSchemas();
    if (timeseries != null) {
      for (int column = 0; column < timeseries.size(); column++) {
        final IMeasurementSchema measurementSchema = timeseries.get(column);
        if (measurementSchema == null) {
          continue;
        }

        final TSDataType tsDataType = measurementSchema.getType();
        if (tsDataType == null) {
          continue;
        }

        if (tsDataType.isBinary()) {
          if (tablet.values == null || tablet.values.length <= column) {
            continue;
          }
          final Binary[] values = ((Binary[]) tablet.values[column]);
          if (values == null) {
            continue;
          }
          for (Binary value : values) {
            totalSizeInBytes +=
                value == null ? 0 : (value.getLength() == -1 ? 0 : value.getLength());
          }
        } else {
          totalSizeInBytes += (long) tablet.timestamps.length * tsDataType.getDataTypeSize();
        }
      }
    }

    // bitMaps
    if (tablet.bitMaps != null) {
      for (int i = 0; i < tablet.bitMaps.length; i++) {
        totalSizeInBytes += tablet.bitMaps[i] == null ? 0 : tablet.bitMaps[i].getSize();
      }
    }

    // estimate other dataStructures size
    totalSizeInBytes += 100;

    return totalSizeInBytes;
  }
}
