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
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_ARRAY_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.BinaryUtils.MIN_OBJECT_HEADER_SIZE;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.constructTimeColumnBuilder;
import static org.apache.iotdb.db.queryengine.transformation.datastructure.util.RowColumnConverter.constructValueColumnBuilder;

public class SerializableTVList implements SerializableList {
  public static SerializableTVList construct(TSDataType dataType, String queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    return new SerializableTVList(dataType, recorder);
  }

  protected static int calculateCapacity(TSDataType dataType, float memoryLimitInMB) {
    int rowLength = ReadWriteIOUtils.LONG_LEN; // timestamp
    switch (dataType) { // value
      case INT32:
        rowLength += ReadWriteIOUtils.INT_LEN;
        break;
      case INT64:
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

  protected final SerializationRecorder serializationRecorder;

  private TSDataType dataType;

  private List<Column> valueColumns;

  private List<TimeColumn> timeColumns;

  private int size;

  protected SerializableTVList(TSDataType dataType, SerializationRecorder serializationRecorder) {
    this.dataType = dataType;
    this.serializationRecorder = serializationRecorder;

    size = 0;

    init();
  }

  public int getColumnIndex(int pointIndex) {
    assert pointIndex < size;

    int ret = -1;
    int total = 0;
    for (int i = 0; i < timeColumns.size(); i++) {
      int length = timeColumns.get(i).getPositionCount();
      if (pointIndex < total + length) {
        ret = i;
        break;
      }
      total += length;
    }

    return ret;
  }

  public int getFirstPointIndex(int columnIndex) {
    int total = 0;
    for (int i = 0; i < columnIndex; i++) {
      total += timeColumns.get(i).getPositionCount();
    }

    return total;
  }

  public long getTime(int index) {
    assert index < size;

    long ret = 0;
    int total = 0;
    for (TimeColumn column : timeColumns) {
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
    assert index < size;

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
    assert index < size;

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
    assert index < size;

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
    assert index < size;

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
    assert index < size;

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
    assert index < size;

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
    assert index < size;

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

  public TimeColumn getTimeColumn(int index) {
    assert index < timeColumns.size();
    return timeColumns.get(index);
  }

  public Column getValueColumn(int index) {
    assert index < valueColumns.size();
    return valueColumns.get(index);
  }

  public void putColumns(TimeColumn timeColumn, Column valueColumn) {
    timeColumns.add(timeColumn);
    valueColumns.add(valueColumn);
    size += timeColumn.getPositionCount();
  }

  public int getColumnCount() {
    return timeColumns.size();
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }

  @Override
  public void serialize(PublicBAOS outputStream) throws IOException {
    serializationRecorder.setSerializedElementSize(size);

    int totalByteLength = 0;
    for (int i = 0; i < timeColumns.size(); i++) {
      totalByteLength += serializeByDataType(timeColumns.get(i), valueColumns.get(i), outputStream);
    }
    serializationRecorder.setSerializedByteLength(totalByteLength);
  }

  private int serializeByDataType(Column times, Column values, PublicBAOS outputStream)
      throws IOException {
    assert times.getPositionCount() == values.getPositionCount();

    int byteLength = 0;
    int count = times.getPositionCount();
    switch (dataType) {
      case BOOLEAN:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getBoolean(i), outputStream);
        }
        break;
      case INT32:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getInt(i), outputStream);
        }
        break;
      case INT64:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getLong(i), outputStream);
        }
        break;
      case FLOAT:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getFloat(i), outputStream);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getDouble(i), outputStream);
        }
        break;
      case TEXT:
        for (int i = 0; i < count; i++) {
          byteLength += ReadWriteIOUtils.write(times.getLong(i), outputStream);
          byteLength += ReadWriteIOUtils.write(values.getBinary(i), outputStream);
        }
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }

    return byteLength;
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    size = serializationRecorder.getSerializedElementSize();

    TimeColumnBuilder timeBuilder = constructTimeColumnBuilder(size);
    ColumnBuilder valueBuilder = constructValueColumnBuilder(dataType, size);
    deserializeByDataType(timeBuilder, valueBuilder, byteBuffer);

    timeColumns = new ArrayList<>();
    valueColumns = new ArrayList<>();
    timeColumns.add((TimeColumn) timeBuilder.build());
    valueColumns.add(valueBuilder.build());
  }

  private void deserializeByDataType(
      ColumnBuilder timeBuilder, ColumnBuilder valueBuilder, ByteBuffer byteBuffer) {
    switch (dataType) {
      case BOOLEAN:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeBoolean(ReadWriteIOUtils.readBool(byteBuffer));
        }
        break;
      case INT32:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeInt(ReadWriteIOUtils.readInt(byteBuffer));
        }
        break;
      case INT64:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
        }
        break;
      case FLOAT:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeFloat(ReadWriteIOUtils.readFloat(byteBuffer));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeDouble(ReadWriteIOUtils.readDouble(byteBuffer));
        }
        break;
      case TEXT:
        for (int i = 0; i < size; i++) {
          timeBuilder.writeLong(ReadWriteIOUtils.readLong(byteBuffer));
          valueBuilder.writeBinary(ReadWriteIOUtils.readBinary(byteBuffer));
        }
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
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

  @TestOnly
  public ForwardIterator getIterator() {
    return new ForwardIterator();
  }

  public class ForwardIterator {
    int index;
    int offset;

    public boolean hasNext() {
      return index < timeColumns.size();
    }

    public long currentTime() {
      return timeColumns.get(index).getLong(offset);
    }

    public boolean currentBoolean() {
      return valueColumns.get(index).getBoolean(offset);
    }

    public int currentInt() {
      return valueColumns.get(index).getInt(offset);
    }

    public long currentLong() {
      return valueColumns.get(index).getLong(offset);
    }

    public float currentFloat() {
      return valueColumns.get(index).getFloat(offset);
    }

    public double currentDouble() {
      return valueColumns.get(index).getDouble(offset);
    }

    public Binary currentBinary() {
      return valueColumns.get(index).getBinary(offset);
    }

    public void next() {
      if (offset == timeColumns.get(index).getPositionCount() - 1) {
        offset = 0;
        index++;
      } else {
        offset++;
      }
    }
  }
}
