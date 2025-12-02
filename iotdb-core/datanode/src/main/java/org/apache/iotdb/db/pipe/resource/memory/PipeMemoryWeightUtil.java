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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.tsfile.utils.RamUsageEstimator.alignObjectSize;

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
          totalSizeInBytes += binary == null ? 8 : binary.ramBytesUsed();
        } else {
          totalSizeInBytes +=
              roundUpToMultiple(TsPrimitiveType.getByType(tsDataType).getSize() + 8, 8);
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
            totalSizeInBytes += binary == null ? 8 : binary.ramBytesUsed();
          } else {
            totalSizeInBytes += roundUpToMultiple(primitiveType.getSize() + 8, 8);
          }
        }
      } else {
        schemaCount = 1;
        if (type.isBinary()) {
          final Binary binary = batchData.getBinary();
          totalSizeInBytes += binary == null ? 8 : binary.ramBytesUsed();
        } else {
          totalSizeInBytes += roundUpToMultiple(TsPrimitiveType.getByType(type).getSize() + 8, 8);
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

  public static long calculateTabletSizeInBytes(final Tablet tablet) {
    return Objects.nonNull(tablet) ? tablet.ramBytesUsed() : 0L;
  }

  public static long calculateTableSchemaBytesUsed(TableSchema tableSchema) {
    long totalSizeInBytes = 0;

    final String tableName = tableSchema.getTableName();
    if (tableName != null) {
      totalSizeInBytes += tableName.length();
    }

    final List<IMeasurementSchema> measurementSchemas = tableSchema.getColumnSchemas();
    if (measurementSchemas != null) {
      totalSizeInBytes +=
          NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * measurementSchemas.size();
      for (IMeasurementSchema measurementSchema : measurementSchemas) {
        InsertNodeMemoryEstimator.sizeOfMeasurementSchema((MeasurementSchema) measurementSchema);
      }
    }

    final List<ColumnCategory> categories = tableSchema.getColumnTypes();
    if (categories != null) {
      totalSizeInBytes +=
          alignObjectSize(
              (long) NUM_BYTES_ARRAY_HEADER
                  + (long) NUM_BYTES_OBJECT_REF * (long) categories.size());
    }

    return totalSizeInBytes;
  }

  public static long calculateBatchDataRamBytesUsed(BatchData batchData) {
    long totalSizeInBytes = 0;

    // timestamp
    totalSizeInBytes += 8;

    // values
    final TSDataType type = batchData.getDataType();
    if (type != null) {
      if (type == TSDataType.VECTOR && batchData.getVector() != null) {
        for (int i = 0; i < batchData.getVector().length; ++i) {
          final TsPrimitiveType primitiveType = batchData.getVector()[i];
          if (primitiveType == null || primitiveType.getDataType() == null) {
            continue;
          }
          // consider variable references (plus 8) and memory alignment (round up to 8)
          totalSizeInBytes += roundUpToMultiple(primitiveType.getSize() + 8, 8);
        }
      } else {
        if (type.isBinary()) {
          final Binary binary = batchData.getBinary();
          // refer to org.apache.tsfile.utils.TsPrimitiveType.TsBinary.getSize
          totalSizeInBytes +=
              roundUpToMultiple((binary == null ? 8 : binary.getLength() + 8) + 8, 8);
        } else {
          totalSizeInBytes += roundUpToMultiple(TsPrimitiveType.getByType(type).getSize() + 8, 8);
        }
      }
    }

    return batchData.length() * totalSizeInBytes;
  }

  public static long calculateChunkRamBytesUsed(Chunk chunk) {
    return chunk != null ? chunk.getRetainedSizeInBytes() : 0L;
  }

  public static long calculateAlignedChunkMetaBytesUsed(
      AbstractAlignedChunkMetadata alignedChunkMetadata) {
    if (alignedChunkMetadata == null) {
      return 0L;
    }

    final ChunkMetadata timeChunkMetadata =
        (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
    final List<IChunkMetadata> valueChunkMetadataList =
        alignedChunkMetadata.getValueChunkMetadataList();

    long size = timeChunkMetadata != null ? timeChunkMetadata.getRetainedSizeInBytes() : 0;
    if (valueChunkMetadataList != null && !valueChunkMetadataList.isEmpty()) {
      for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
        if (valueChunkMetadata != null) {
          size += ((ChunkMetadata) valueChunkMetadata).getRetainedSizeInBytes();
        }
      }
    }

    return size;
  }

  /**
   * Rounds up the given integer num to the nearest multiple of n.
   *
   * @param num The integer to be rounded up.
   * @param n The specified multiple.
   * @return The nearest multiple of n greater than or equal to num.
   */
  private static int roundUpToMultiple(int num, int n) {
    if (n == 0) {
      throw new IllegalArgumentException("The multiple n must be greater than 0");
    }
    // Calculate the rounded up value to the nearest multiple of n
    return ((num + n - 1) / n) * n;
  }
}
