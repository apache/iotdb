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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

public class BatchInsertPlan extends PhysicalPlan {

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  private String deviceId;
  private String[] measurements;
  private TSDataType[] dataTypes;

  private long[] times; // times should be sorted. It is done in the session API.
  private ByteBuffer timeBuffer;

  private Object[] columns;
  private ByteBuffer valueBuffer;
  private Set<Integer> index;
  private int rowCount = 0;
  // cached values
  private Long maxTime = null;
  private Long minTime = null;
  private List<Path> paths;
  private int start;
  private int end;

  public BatchInsertPlan() {
    super(false, OperatorType.BATCHINSERT);
  }

  public BatchInsertPlan(String deviceId, List<String> measurements) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    setMeasurements(measurements);
  }
  public BatchInsertPlan(String deviceId, String[] measurements) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    setMeasurements(measurements);
  }

  public BatchInsertPlan(String deviceId, String[] measurements, List<Integer> dataTypes) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  public Set<Integer> getIndex() {
    return index;
  }

  public void setIndex(Set<Integer> index) {
    this.index = index;
  }

  @Override
  public List<Path> getPaths() {
    if (paths != null) {
      return paths;
    }
    List<Path> ret = new ArrayList<>();
    for (String m : measurements) {
      ret.add(new Path(deviceId, m));
    }
    paths = ret;
    return ret;
  }

  @Override
  public void serializeTo(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    stream.writeByte((byte) type);

    putString(stream, deviceId);

    stream.writeInt(measurements.length);
    for (String m : measurements) {
      putString(stream, m);
    }

    for (TSDataType dataType : dataTypes) {
      stream.writeShort(dataType.serialize());
    }

    stream.writeInt(index.size());

    if (timeBuffer == null) {
      for(int loc : index){
        stream.writeLong(times[loc]);
      }
    } else {
      stream.write(timeBuffer.array());
      timeBuffer = null;
    }

    if (valueBuffer == null) {
      serializeValues(stream);
    } else {
      stream.write(valueBuffer.array());
      valueBuffer = null;
    }
  }

  private void serializeValues(DataOutputStream stream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      serializeColumn(dataTypes[i], columns[i], stream, index);
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, DataOutputStream stream,
      Set<Integer> index)
      throws IOException {
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for(int loc : index){
          stream.writeInt(intValues[loc]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for(int loc : index){
          stream.writeLong(longValues[loc]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for(int loc : index){
          stream.writeFloat(floatValues[loc]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for(int loc : index){
          stream.writeDouble(doubleValues[loc]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for(int loc : index){
          stream.write(BytesUtils.boolToByte(boolValues[loc]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for(int loc : index){
          stream.writeInt(binaryValues[loc].getLength());
          stream.write(binaryValues[loc].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);

    putString(buffer, deviceId);

    buffer.putInt(measurements.length);
    for (String m : measurements) {
      putString(buffer, m);
    }

    for (TSDataType dataType : dataTypes) {
      buffer.putShort(dataType.serialize());
    }

    buffer.putInt(end - start);

    if (timeBuffer == null) {
      for (int i = start; i < end; i++) {
        buffer.putLong(times[i]);
      }
    } else {
      buffer.put(timeBuffer.array());
      timeBuffer = null;
    }

    if (valueBuffer == null) {
      serializeValues(buffer);
    } else {
      buffer.put(valueBuffer.array());
      valueBuffer = null;
    }
  }

  private void serializeValues(ByteBuffer buffer) {
    for (int i = 0; i < measurements.length; i++) {
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, ByteBuffer buffer,
      int start, int end) {
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
          buffer.putInt(BytesUtils.boolToByte(boolValues[j]));
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
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  public void setTimeBuffer(ByteBuffer timeBuffer) {
    this.timeBuffer = timeBuffer;
    this.timeBuffer.position(0);
  }

  public void setValueBuffer(ByteBuffer valueBuffer) {
    this.valueBuffer = valueBuffer;
    this.timeBuffer.position(0);
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    this.deviceId = readString(buffer);

    int measurementSize = buffer.getInt();
    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    this.dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.getShort());
    }

    int rows = buffer.getInt();
    rowCount = rows;
    this.times = new long[rows];
    times = QueryDataSetUtils.readTimesFromBuffer(buffer, rows);

    columns = QueryDataSetUtils.readValuesFromBuffer(buffer, dataTypes, measurementSize, rows);
  }


  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = new String[measurements.size()];
    measurements.toArray(this.measurements);
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<Integer> dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = TSDataType.values()[dataTypes.get(i)];
    }
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
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

  public long getMinTime() {
    if (minTime != null) {
      return minTime;
    }
    minTime = Long.MAX_VALUE;
    for (Long time : times) {
      if (time < minTime) {
        minTime = time;
      }
    }
    return minTime;
  }

  public long getMaxTime() {
    if (maxTime != null) {
      return maxTime;
    }
    long tmpMaxTime = Long.MIN_VALUE;
    for (Long time : times) {
      if (time > tmpMaxTime) {
        tmpMaxTime = time;
      }
    }
    return tmpMaxTime;
  }

  public TimeValuePair composeLastTimeValuePair(int measurementIndex) {
    if (measurementIndex >= columns.length) {
      return null;
    }
    TsPrimitiveType value;
    switch (dataTypes[measurementIndex]) {
      case INT32:
        int[] intValues = (int[]) columns[measurementIndex];
        value = new TsInt(intValues[end - 1]);
        break;
      case INT64:
        long[] longValues = (long[]) columns[measurementIndex];
        value = new TsLong(longValues[end - 1]);
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[measurementIndex];
        value = new TsFloat(floatValues[end - 1]);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[measurementIndex];
        value = new TsDouble(doubleValues[end - 1]);
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[measurementIndex];
        value = new TsBoolean(boolValues[end - 1]);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) columns[measurementIndex];
        value = new TsBinary(binaryValues[end - 1]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[measurementIndex]));
    }
    return new TimeValuePair(times[end - 1], value);
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

}
