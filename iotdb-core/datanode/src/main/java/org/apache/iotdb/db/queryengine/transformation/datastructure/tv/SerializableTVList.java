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

package org.apache.iotdb.db.queryengine.transformation.datastructure.tv;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;

public class SerializableTVList implements SerializableList {
  protected final SerializationRecorder serializationRecorder;

  private final TsBlockSerde serde;

  private List<Column> valueColumns;

  private List<Column> timeColumns;

  private final List<Integer> columnSizes;

  private int size;

  public static SerializableTVList construct(String queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableTVList(recorder);
  }

  protected SerializableTVList(SerializationRecorder serializationRecorder) {
    this.serializationRecorder = serializationRecorder;

    serde = new TsBlockSerde();
    columnSizes = new ArrayList<>();
    size = 0;

    init();
  }

  protected static int calculateCapacity(TSDataType dataType, float memoryLimitInMB) {
    int rowLength = ReadWriteIOUtils.LONG_LEN; // timestamp
    switch (dataType) { // value
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
      case STRING:
      case BLOB:
        rowLength +=
            MIN_OBJECT_HEADER_SIZE
                + MIN_ARRAY_HEADER_SIZE
                + SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL;
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
    rowLength += ReadWriteIOUtils.BIT_LEN;

    int capacity =
        TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (int)
                (memoryLimitInMB * MB / 2 / (rowLength * TSFileConfig.ARRAY_CAPACITY_THRESHOLD));
    if (capacity <= 0) {
      throw new RuntimeException("Memory is not enough for current query.");
    }
    return capacity;
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }

  public int getColumnCount() {
    return columnSizes.size();
  }

  // region single data methods
  public long getTime(int index) {
    checkState(index < size);

    long ret = 0;
    int total = 0;
    for (Column column : timeColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getLong(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getInt(int index) {
    checkState(index < size);

    int ret = 0;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getInt(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public long getLong(int index) {
    checkState(index < size);

    long ret = 0;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getLong(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public float getFloat(int index) {
    checkState(index < size);

    float ret = 0;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getFloat(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public double getDouble(int index) {
    checkState(index < size);

    double ret = 0;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getDouble(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public boolean getBoolean(int index) {
    checkState(index < size);

    boolean ret = false;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getBoolean(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public Binary getBinary(int index) {
    checkState(index < size);

    Binary ret = null;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.getBinary(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  public boolean isNull(int index) {
    checkState(index < size);

    boolean ret = false;
    int total = 0;
    for (Column column : valueColumns) {
      int length = column.getPositionCount();
      if (index < total + length) {
        int offset = index - total;
        ret = column.isNull(offset);
        break;
      }
      total += length;
    }

    return ret;
  }

  // endregion

  // region batch data method
  public Column getTimeColumn(int index) {
    checkState(index < timeColumns.size());
    return timeColumns.get(index);
  }

  public Column getValueColumn(int index) {
    checkState(index < valueColumns.size());
    return valueColumns.get(index);
  }

  public void putColumns(Column timeColumn, Column valueColumn) {
    timeColumns.add(timeColumn);
    valueColumns.add(valueColumn);

    int columnSize = timeColumn.getPositionCount();
    columnSizes.add(columnSize);
    size += timeColumn.getPositionCount();
  }

  // endregion

  public int getColumnSize(int index) {
    checkState(index < columnSizes.size());
    return columnSizes.get(index);
  }

  public int getColumnIndex(int pointIndex) {
    checkState(pointIndex < size);

    int ret = -1;
    int total = 0;
    for (int i = 0; i < columnSizes.size(); i++) {
      int length = columnSizes.get(i);
      if (pointIndex < total + length) {
        ret = i;
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getTVOffsetInColumns(int index) {
    checkState(index < size);

    int ret = -1;
    int total = 0;
    for (int length : columnSizes) {
      if (index < total + length) {
        ret = index - total;
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getLastPointIndex(int columnIndex) {
    int total = 0;
    for (int i = 0; i <= columnIndex; i++) {
      total += columnSizes.get(i);
    }

    return total;
  }

  @Override
  public void release() {
    timeColumns = null;
    valueColumns = null;
  }

  @Override
  public void init() {
    timeColumns = new ArrayList<>();
    valueColumns = new ArrayList<>();
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    int bufferSize = 0;

    // Write TsBlocks count
    bufferSize += ReadWriteIOUtils.write(timeColumns.size(), outputStream);

    for (int i = 0; i < timeColumns.size(); i++) {
      // Construct TsBlock
      Column timeColumn = timeColumns.get(i);
      Column valueColumn = valueColumns.get(i);
      TsBlock tsBlock = new TsBlock(timeColumn, valueColumn);

      ByteBuffer buffer = serde.serialize(tsBlock);
      byte[] byteArray = buffer.array();
      // Write TsBlock data
      outputStream.write(byteArray);
      bufferSize += byteArray.length;
    }

    serializationRecorder.setSerializedByteLength(bufferSize);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    // Read TsBlocks count
    int blockCount = ReadWriteIOUtils.readInt(byteBuffer);
    timeColumns = new ArrayList<>(blockCount);
    valueColumns = new ArrayList<>(blockCount);

    for (int i = 0; i < blockCount; i++) {
      // Read TsBlocks data
      TsBlock tsBlock = serde.deserialize(byteBuffer);

      // Unpack columns
      Column timeColumn = tsBlock.getTimeColumn();
      Column valueColumn = tsBlock.getColumn(0);

      timeColumns.add(timeColumn);
      valueColumns.add(valueColumn);
    }
  }

  @TestOnly
  public List<Column> getTimeColumns() {
    return timeColumns;
  }

  @TestOnly
  public List<Column> getValueColumns() {
    return valueColumns;
  }
}
