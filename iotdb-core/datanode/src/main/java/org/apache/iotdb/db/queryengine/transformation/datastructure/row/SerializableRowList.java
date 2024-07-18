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

package org.apache.iotdb.db.queryengine.transformation.datastructure.row;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;

public class SerializableRowList implements SerializableList {
  private final SerializationRecorder serializationRecorder;
  private final TsBlockSerde serde;
  private final TSDataType[] dataTypes;
  private final int valueColumnCount;

  private List<Column[]> blocks;
  private final List<Integer> blockSizes;

  private int skipPrefixNullCount;

  private int prefixNullCount;

  public static SerializableRowList construct(String queryId, TSDataType[] dataTypes) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableRowList(recorder, dataTypes);
  }

  private SerializableRowList(SerializationRecorder serializationRecorder, TSDataType[] dataTypes) {
    this.serializationRecorder = serializationRecorder;
    this.dataTypes = dataTypes;
    serde = new TsBlockSerde();
    blockSizes = new ArrayList<>();

    valueColumnCount = dataTypes.length;
    prefixNullCount = 0;
    skipPrefixNullCount = 0;

    init();
  }

  /**
   * Calculate the number of rows that can be cached given the memory limit.
   *
   * @param dataTypes Data types of columns.
   * @param memoryLimitInMB Memory limit.
   * @param byteArrayLengthForMemoryControl Max memory usage for a {@link TSDataType#TEXT}.
   * @return Number of rows that can be cached.
   * @throws QueryProcessException if the result capacity <= 0
   * @throws UnSupportedDataTypeException if the input datatype can not be handled by the given
   *     branches.
   */
  protected static int calculateCapacity(
      TSDataType[] dataTypes, float memoryLimitInMB, int byteArrayLengthForMemoryControl)
      throws QueryProcessException {
    int rowLength = ReadWriteIOUtils.LONG_LEN; // timestamp
    for (TSDataType dataType : dataTypes) { // fields
      switch (dataType) {
        case INT32:
        case DATE:
          rowLength += ReadWriteIOUtils.INT_LEN;
          break;
        case INT64:
        case TIMESTAMP:
          rowLength += ReadWriteIOUtils.LONG_LEN;
          break;
        case FLOAT:
          rowLength += ReadWriteIOUtils.FLOAT_LEN;
          break;
        case DOUBLE:
          rowLength += ReadWriteIOUtils.DOUBLE_LEN;
          break;
        case BOOLEAN:
          rowLength += ReadWriteIOUtils.BOOLEAN_LEN;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          rowLength +=
              MIN_OBJECT_HEADER_SIZE + MIN_ARRAY_HEADER_SIZE + byteArrayLengthForMemoryControl;
          break;
        default:
          throw new UnSupportedDataTypeException(dataType.toString());
      }
    }
    rowLength += ReadWriteIOUtils.BIT_LEN; // null field

    int capacity = (int) (memoryLimitInMB * MB / 2 / rowLength);
    if (capacity <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return capacity;
  }

  public int size() {
    return prefixNullCount + skipPrefixNullCount;
  }

  public int getBlockCount() {
    // Threat prefix null values as one column
    int additional_null_block = prefixNullCount == 0 ? 0 : 1;
    return blockSizes.size() + additional_null_block;
  }

  public int getBlockSize(int index) {
    if (prefixNullCount != 0) {
      return index == 0 ? prefixNullCount : blockSizes.get(index - 1);
    } else {
      return blockSizes.get(index);
    }
  }

  // region single data methods
  public long getTime(int index) {
    // Never access null row's time
    assert index >= prefixNullCount;

    return getTimeSkipPrefixNulls(index - prefixNullCount);
  }

  private long getTimeSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    int total = 0;
    long time = -1;
    for (Column[] block : blocks) {
      int length = block[0].getPositionCount();
      if (index < total + length) {
        int offset = index - total;

        // Last column is always time column
        time = block[valueColumnCount].getLong(offset);
        break;
      }
      total += length;
    }

    return time;
  }

  public Object[] getRow(int index) {
    // Fall into prefix null values
    if (index < prefixNullCount) {
      return null;
    }

    return getRowSkipPrefixNulls(index - prefixNullCount);
  }

  private Object[] getRowSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    // Value columns + time column
    Object[] row = new Object[valueColumnCount + 1];

    int total = 0;
    for (Column[] block : blocks) {
      // Find position
      int length = block[0].getPositionCount();
      if (index < total + length) {
        int offset = index - total;

        // Fill value columns
        for (int i = 0; i < block.length - 1; i++) {
          switch (dataTypes[i]) {
            case INT32:
            case DATE:
              row[i] = block[i].getInt(offset);
              break;
            case INT64:
            case TIMESTAMP:
              row[i] = block[i].getLong(offset);
              break;
            case FLOAT:
              row[i] = block[i].getFloat(offset);
              break;
            case DOUBLE:
              row[i] = block[i].getDouble(offset);
              break;
            case BOOLEAN:
              row[i] = block[i].getBoolean(offset);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              row[i] = block[i].getBinary(offset);
              break;
            default:
              throw new UnSupportedDataTypeException(dataTypes[i].toString());
          }
        }
        // Fill time column
        row[block.length - 1] = block[block.length - 1].getLong(offset);

        break;
      }
      total += length;
    }

    return row;
  }

  // endregion

  // region batch data methods
  public void putColumns(Column[] columns) {
    blocks.add(columns);

    int size = columns[0].getPositionCount();
    blockSizes.add(size);
    skipPrefixNullCount += size;
  }

  public void putNulls(int count) {
    prefixNullCount += count;
  }

  public Column[] getColumns(int index) {
    // Skip all null columns at first
    if (prefixNullCount != 0) {
      index--;
    }
    // Forbid to fetch first null column
    assert index >= 0;
    return blocks.get(index);
  }

  // endregion

  public int getColumnIndex(int index) {
    // Fall into first null column
    if (index < prefixNullCount) {
      return 0;
    }
    int addition = prefixNullCount == 0 ? 0 : 1;
    return addition + getColumnIndexSkipPrefixNulls(index - prefixNullCount);
  }

  private int getColumnIndexSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    int ret = -1;
    int total = 0;
    for (int i = 0; i < blockSizes.size(); i++) {
      int length = blockSizes.get(i);
      if (index < total + length) {
        ret = i;
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getRowOffsetInColumns(int index) {
    assert index >= prefixNullCount;

    return getRowOffsetInColumnsSkipPrefixNulls(index - prefixNullCount);
  }

  private int getRowOffsetInColumnsSkipPrefixNulls(int index) {
    assert index < skipPrefixNullCount;

    int ret = -1;
    int total = 0;
    for (int length : blockSizes) {
      if (index < total + length) {
        ret = index - total;
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getLastRowIndex(int blockIndex) {
    int total = prefixNullCount;
    for (int i = 0; i <= blockIndex; i++) {
      total += blockSizes.get(i);
    }

    return total;
  }

  @Override
  public void release() {
    blocks = null;
  }

  @Override
  public void init() {
    blocks = new ArrayList<>();
  }

  // Only blocks data are serialized to disk
  // Other field are kept in memory
  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int bufferSize = 0;

    // Write TsBlocks count
    bufferSize += ReadWriteIOUtils.write(blocks.size(), outputStream);

    for (Column[] block : blocks) {
      Column timeColumn = block[block.length - 1];
      Column[] valueColumns = new Column[block.length - 1];
      // Only references are copied
      System.arraycopy(block, 0, valueColumns, 0, block.length - 1);
      TsBlock tsBlock = new TsBlock(timeColumn, valueColumns);

      ByteBuffer buffer = serde.serialize(tsBlock);
      byte[] byteArray = buffer.array();
      // Write TsBlocks data
      outputStream.write(byteArray);
      bufferSize += byteArray.length;
    }

    serializationRecorder.setSerializedByteLength(bufferSize);
  }

  // Deserialized blocks from disk to memory
  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // Read TsBlocks count
    int blockCount = ReadWriteIOUtils.readInt(byteBuffer);
    blocks = new ArrayList<>(blockCount);

    for (int i = 0; i < blockCount; i++) {
      // Read TsBlocks data
      TsBlock tsBlock = serde.deserialize(byteBuffer);
      blocks.add(tsBlock.getAllColumns());
    }
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }
}
