/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("java:S1135") // ignore todos
public class InsertTabletPlan extends InsertPlan implements WALEntryValue {
  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  private long[] times; // times should be sorted. It is done in the session API.

  private BitMap[] bitMaps;
  private Object[] columns;
  private int rowCount = 0;
  private List<PartialPath> paths;
  // when this plan is sub-plan split from another InsertTabletPlan, this indicates the original
  // positions of values in
  // this plan. For example, if the plan contains 5 timestamps, and range = [1,4,10,12], then it
  // means that the first 3
  // timestamps in this plan are from range[1,4) of the parent plan, and the last 2 timestamps are
  // from range[10,12)
  // of the parent plan.
  // this is usually used to back-propagate exceptions to the parent plan without losing their
  // proper positions.
  private List<Integer> range;

  private List<Object> failedColumns;

  public InsertTabletPlan() {
    super(OperatorType.BATCH_INSERT);
  }

  public InsertTabletPlan(PartialPath prefixPath, List<String> measurements) {
    super(OperatorType.BATCH_INSERT);
    this.devicePath = prefixPath;
    this.measurements = measurements.toArray(new String[0]);
    this.canBeSplit = true;
  }

  public InsertTabletPlan(PartialPath prefixPath, String[] measurements) {
    super(OperatorType.BATCH_INSERT);
    this.devicePath = prefixPath;
    this.measurements = measurements;
    this.canBeSplit = true;
  }

  public InsertTabletPlan(PartialPath prefixPath, String[] measurements, List<Integer> dataTypes) {
    super(OperatorType.BATCH_INSERT);
    this.devicePath = prefixPath;
    this.measurements = measurements;
    setDataTypes(dataTypes);
    this.canBeSplit = true;
  }

  public InsertTabletPlan(
      PartialPath prefixPath, String[] measurements, List<Integer> dataTypes, boolean isAligned) {
    super(OperatorType.BATCH_INSERT);
    this.devicePath = prefixPath;
    this.measurements = measurements;
    setDataTypes(dataTypes);
    this.canBeSplit = true;
    this.isAligned = isAligned;
  }

  public List<Integer> getRange() {
    return range;
  }

  public void setRange(List<Integer> range) {
    this.range = range;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (paths != null) {
      return paths;
    }
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatNode(m);
      ret.add(fullPath);
    }
    paths = ret;
    return ret;
  }

  @Override
  public int serializedSize() {
    int size = Byte.BYTES;
    return size + subSerializeSize(0, rowCount);
  }

  public int serializedSize(int start, int end) {
    int size = Byte.BYTES;
    return size + subSerializeSize(start, end);
  }

  int subSerializeSize(int start, int end) {
    int size = 0;
    size += ReadWriteIOUtils.sizeToWrite(devicePath.getFullPath());
    // measurements size
    size += Integer.BYTES;
    for (String m : measurements) {
      if (m != null) {
        size += ReadWriteIOUtils.sizeToWrite(m);
      }
    }
    // data types size
    size += Integer.BYTES;
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] != null) {
        size += Byte.BYTES;
      }
    }
    // times size
    size += Integer.BYTES;
    size += Long.BYTES * (end - start);
    // bitmaps size
    size += Byte.BYTES;
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        size += Byte.BYTES;
        if (bitMap != null) {
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          size += partBitMap.getByteArray().length;
        }
      }
    }
    // values size
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] != null) {
        size += getColumnSize(dataTypes[i], columns[i], start, end);
      }
    }
    size += Long.BYTES;
    size += Byte.BYTES;
    return size;
  }

  private int getColumnSize(TSDataType dataType, Object column, int start, int end) {
    int size = 0;
    switch (dataType) {
      case INT32:
        size += Integer.BYTES * (end - start);
        break;
      case INT64:
        size += Long.BYTES * (end - start);
        break;
      case FLOAT:
        size += Float.BYTES * (end - start);
        break;
      case DOUBLE:
        size += Double.BYTES * (end - start);
        break;
      case BOOLEAN:
        size += Byte.BYTES * (end - start);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          size += ReadWriteIOUtils.sizeToWrite(binaryValues[j]);
        }
        break;
    }
    return size;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    stream.writeByte((byte) type);
    subSerialize(stream, 0, rowCount);
  }

  void subSerialize(DataOutputStream stream, int start, int end) throws IOException {
    putString(stream, devicePath.getFullPath());
    writeMeasurements(stream);
    writeDataTypes(stream);
    writeTimes(stream, start, end);
    writeBitMaps(stream, start, end);
    writeValues(stream, start, end);
    stream.write((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurements(DataOutputStream stream) throws IOException {
    stream.writeInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (String m : measurements) {
      if (m == null) {
        continue;
      }
      putString(stream, m);
    }
  }

  private void writeDataTypes(DataOutputStream stream) throws IOException {
    stream.writeInt(dataTypes.length);
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      TSDataType dataType = dataTypes[i];
      stream.write(dataType.serialize());
    }
  }

  private void writeTimes(DataOutputStream stream, int start, int end) throws IOException {
    stream.writeInt(end - start);
    for (int i = start; i < end; i++) {
      stream.writeLong(times[i]);
    }
  }

  private void writeBitMaps(DataOutputStream stream, int start, int end) throws IOException {
    stream.writeBoolean(bitMaps != null);
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap == null) {
          stream.writeBoolean(false);
        } else {
          stream.writeBoolean(true);
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          stream.write(partBitMap.getByteArray());
        }
      }
    }
  }

  private void writeValues(DataOutputStream stream, int start, int end) throws IOException {
    serializeValues(stream, start, end);
    stream.writeLong(index);
  }

  private void serializeValues(DataOutputStream outputStream, int start, int end)
      throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], outputStream, start, end);
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, DataOutputStream outputStream, int start, int end)
      throws IOException {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeByte(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          outputStream.writeInt(binaryValues[j].getLength());
          outputStream.write(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);
    subSerialize(buffer, 0, rowCount);
  }

  void subSerialize(ByteBuffer buffer, int start, int end) {
    putString(buffer, devicePath.getFullPath());
    writeMeasurements(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer, 0, rowCount);
    writeBitMaps(buffer, 0, rowCount);
    writeValues(buffer, 0, rowCount);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurements(ByteBuffer buffer) {
    buffer.putInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (String m : measurements) {
      if (m != null) {
        putString(buffer, m);
      }
    }
  }

  private void writeDataTypes(ByteBuffer buffer) {
    buffer.putInt(dataTypes.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (int i = 0, dataTypesLength = dataTypes.length; i < dataTypesLength; i++) {
      TSDataType dataType = dataTypes[i];
      if (columns[i] == null) {
        continue;
      }
      dataType.serializeTo(buffer);
    }
  }

  private void writeTimes(ByteBuffer buffer, int start, int end) {
    buffer.putInt(rowCount);
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  private void writeBitMaps(ByteBuffer buffer, int start, int end) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap == null) {
          buffer.put(BytesUtils.boolToByte(false));
        } else {
          buffer.put(BytesUtils.boolToByte(true));
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          buffer.put(partBitMap.getByteArray());
        }
      }
    }
  }

  private void writeValues(ByteBuffer buffer, int start, int end) {
    serializeValues(buffer, start, end);
    buffer.putLong(index);
  }

  private void serializeValues(ByteBuffer buffer, int start, int end) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, ByteBuffer buffer, int start, int end) {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = start; j < end; j++) {
          buffer.putLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = start; j < end; j++) {
          buffer.putFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = start; j < end; j++) {
          buffer.putDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = start; j < end; j++) {
          buffer.put(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(binaryValues[j].getLength());
          buffer.put(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    serializeToWAL(buffer, 0, rowCount);
  }

  public void serializeToWAL(IWALByteBufferView buffer, int start, int end) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);
    subSerialize(buffer, start, end);
  }

  void subSerialize(IWALByteBufferView buffer, int start, int end) {
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    writeMeasurements(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer, start, end);
    writeBitMaps(buffer, start, end);
    writeValues(buffer, start, end);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void writeMeasurements(IWALByteBufferView buffer) {
    buffer.putInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (String m : measurements) {
      if (m != null) {
        WALWriteUtils.write(m, buffer);
      }
    }
  }

  private void writeDataTypes(IWALByteBufferView buffer) {
    buffer.putInt(dataTypes.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (int i = 0, dataTypesLength = dataTypes.length; i < dataTypesLength; i++) {
      TSDataType dataType = dataTypes[i];
      if (columns[i] == null) {
        continue;
      }
      WALWriteUtils.write(dataType, buffer);
    }
  }

  private void writeTimes(IWALByteBufferView buffer, int start, int end) {
    buffer.putInt(end - start);
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  private void writeBitMaps(IWALByteBufferView buffer, int start, int end) {
    buffer.put(BytesUtils.boolToByte(bitMaps != null));
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap == null) {
          buffer.put(BytesUtils.boolToByte(false));
        } else {
          buffer.put(BytesUtils.boolToByte(true));
          int len = end - start;
          BitMap partBitMap = new BitMap(len);
          BitMap.copyOfRange(bitMap, start, partBitMap, 0, len);
          buffer.put(partBitMap.getByteArray());
        }
      }
    }
  }

  private void writeValues(IWALByteBufferView buffer, int start, int end) {
    serializeValues(buffer, start, end);
    buffer.putLong(index);
  }

  private void serializeValues(IWALByteBufferView buffer, int start, int end) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (columns[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, IWALByteBufferView buffer, int start, int end) {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = start; j < end; j++) {
          buffer.putLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = start; j < end; j++) {
          buffer.putFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = start; j < end; j++) {
          buffer.putDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = start; j < end; j++) {
          buffer.put(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = start; j < end; j++) {
          buffer.putInt(binaryValues[j].getLength());
          buffer.put(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public void deserialize(DataInputStream stream) throws IOException, IllegalPathException {
    this.devicePath = new PartialPath(ReadWriteIOUtils.readString(stream));

    int measurementSize = stream.readInt();
    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = ReadWriteIOUtils.readString(stream);
    }

    int dataTypeSize = stream.readInt();
    this.dataTypes = new TSDataType[dataTypeSize];
    for (int i = 0; i < dataTypeSize; i++) {
      dataTypes[i] = TSDataType.deserialize(stream.readByte());
    }

    int rows = stream.readInt();
    rowCount = rows;
    this.times = new long[rows];
    times = QueryDataSetUtils.readTimesFromStream(stream, rows);

    boolean hasBitMaps = BytesUtils.byteToBool(stream.readByte());
    if (hasBitMaps) {
      bitMaps = QueryDataSetUtils.readBitMapsFromStream(stream, dataTypeSize, rows);
    }
    columns = QueryDataSetUtils.readTabletValuesFromStream(stream, dataTypes, dataTypeSize, rows);
    this.index = stream.readLong();
    this.isAligned = stream.readByte() == 1;
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.devicePath = new PartialPath(readString(buffer));

    int measurementSize = buffer.getInt();
    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    int dataTypeSize = buffer.getInt();
    this.dataTypes = new TSDataType[dataTypeSize];
    for (int i = 0; i < dataTypeSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.get());
    }

    int rows = buffer.getInt();
    rowCount = rows;
    this.times = new long[rows];
    times = QueryDataSetUtils.readTimesFromBuffer(buffer, rows);

    boolean hasBitMaps = BytesUtils.byteToBool(buffer.get());
    if (hasBitMaps) {
      bitMaps = QueryDataSetUtils.readBitMapsFromBuffer(buffer, dataTypeSize, rows);
    }
    columns = QueryDataSetUtils.readTabletValuesFromBuffer(buffer, dataTypes, dataTypeSize, rows);
    this.index = buffer.getLong();
    this.isAligned = buffer.get() == 1;
  }

  public void setDataTypes(List<Integer> dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = TSDataType.values()[dataTypes.get(i)];
    }
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public void setColumn(int index, Object column) {
    columns[index] = column;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  @Override
  public long getMinTime() {
    return times.length != 0 ? times[0] : Long.MIN_VALUE;
  }

  public long getMaxTime() {
    return times.length != 0 ? times[times.length - 1] : Long.MAX_VALUE;
  }

  public TimeValuePair composeLastTimeValuePair(int measurementIndex) {
    if (measurementIndex >= columns.length) {
      return null;
    }

    // get non-null value
    int lastIdx = rowCount - 1;
    if (bitMaps != null && bitMaps[measurementIndex] != null) {
      BitMap bitMap = bitMaps[measurementIndex];
      while (lastIdx >= 0) {
        if (!bitMap.isMarked(lastIdx)) {
          break;
        }
        lastIdx--;
      }
    }
    if (lastIdx < 0) {
      return null;
    }

    TsPrimitiveType value;
    switch (dataTypes[measurementIndex]) {
      case INT32:
        int[] intValues = (int[]) columns[measurementIndex];
        value = new TsInt(intValues[lastIdx]);
        break;
      case INT64:
        long[] longValues = (long[]) columns[measurementIndex];
        value = new TsLong(longValues[lastIdx]);
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[measurementIndex];
        value = new TsFloat(floatValues[lastIdx]);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[measurementIndex];
        value = new TsDouble(doubleValues[lastIdx]);
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[measurementIndex];
        value = new TsBoolean(boolValues[lastIdx]);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) columns[measurementIndex];
        value = new TsBinary(binaryValues[lastIdx]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[measurementIndex]));
    }
    return new TimeValuePair(times[lastIdx], value);
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int size) {
    this.rowCount = size;
  }

  @Override
  public String toString() {
    return "InsertTabletPlan {"
        + "prefixPath:"
        + devicePath
        + ", timesRange["
        + times[0]
        + ","
        + times[times.length - 1]
        + "]"
        + ", isAligned:"
        + isAligned
        + '}';
  }

  @Override
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    super.markFailedMeasurementInsertion(index, e);
    if (failedColumns == null) {
      failedColumns = new ArrayList<>();
    }
    failedColumns.add(columns[index]);
    columns[index] = null;
  }

  @Override
  public InsertPlan getPlanFromFailed() {
    if (super.getPlanFromFailed() == null) {
      return null;
    }
    // TODO anything else?
    columns = failedColumns.toArray(new Object[0]);
    failedColumns = null;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertTabletPlan that = (InsertTabletPlan) o;

    return rowCount == that.rowCount
        && Objects.equals(devicePath, that.devicePath)
        && Arrays.equals(times, that.times)
        && Objects.equals(paths, that.paths)
        && Objects.equals(range, that.range)
        && Objects.equals(isAligned, that.isAligned);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(rowCount, paths, range);
    result = 31 * result + Arrays.hashCode(times);
    return result;
  }

  @Override
  public void recoverFromFailure() {
    if (failedMeasurements == null) {
      return;
    }

    for (int i = 0; i < failedMeasurements.size(); i++) {
      int index = failedIndices.get(i);
      columns[index] = failedColumns.get(i);
    }
    super.recoverFromFailure();

    failedColumns = null;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    super.checkIntegrity();
    if (columns == null || columns.length == 0) {
      throw new QueryProcessException("Values are null");
    }
    if (dataTypes.length != columns.length) {
      throw new QueryProcessException(
          String.format(
              "Measurements length [%d] does not match " + "columns length [%d]",
              measurements.length, columns.length));
    }
    for (Object value : columns) {
      if (value == null) {
        throw new QueryProcessException("Columns contain null: " + Arrays.toString(columns));
      }
    }
  }
}
