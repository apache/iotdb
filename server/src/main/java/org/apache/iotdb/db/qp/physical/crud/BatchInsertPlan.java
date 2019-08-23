/**
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
package org.apache.iotdb.db.qp.physical.crud;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class BatchInsertPlan extends PhysicalPlan {

  private String deviceId;
  private String[] measurements;
  private TSDataType[] dataTypes;
  private List<IoTDBDataType> dataTypeList;

  private long[] times;
  private ByteBuffer timeBuffer;

  private Object[] columns;
  private ByteBuffer valueBuffer;

  private int rowCount = 0;

  // cached values
  private Long maxTime = null;
  private Long minTime = null;
  private List<Path> paths;

  public BatchInsertPlan() {
    super(false, OperatorType.BATCHINSERT);
  }

  public BatchInsertPlan(String deviceId, List<String> measurements) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    setMeasurements(measurements);
  }

  public BatchInsertPlan(String deviceId, String[] measurements, List<IoTDBDataType> dataTypes) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
  }

  public BatchInsertPlan(String deviceId, String[] measurements, IoTDBDataType[] dataTypes) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
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
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);

    putString(buffer, deviceId);

    buffer.putInt(measurements.length);
    for (String m : measurements) {
      putString(buffer, m);
    }

    for (TSDataType dataType: dataTypes) {
      buffer.putShort(dataType.serialize());
    }

    buffer.putInt(times.length);

    if (timeBuffer == null) {
      for (long time: times) {
        buffer.putLong(time);
      }
    } else {
      buffer.put(timeBuffer.array());
      timeBuffer = null;
    }

    if (valueBuffer == null) {
      for (int i = 0; i < measurements.length; i++) {
        TSDataType dataType = dataTypes[i];
        switch (dataType) {
          case INT32:
            int[] intValues = (int[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.putInt(intValues[index]);
            }
            break;
          case INT64:
            long[] longValues = (long[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.putLong(longValues[index]);
            }
            break;
          case FLOAT:
            float[] floatValues = (float[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.putFloat(floatValues[index]);
            }
            break;
          case DOUBLE:
            double[] doubleValues = (double[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.putDouble(doubleValues[index]);
            }
            break;
          case BOOLEAN:
            boolean[] boolValues = (boolean[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.put(BytesUtils.boolToByte(boolValues[index]));
            }
            break;
          case TEXT:
            Binary[] binaryValues = (Binary[]) columns[i];
            for (int index = 0; index < rowCount; index++) {
              buffer.putInt(binaryValues[index].getLength());
              buffer.put(binaryValues[index].getValues());
            }
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataType));
        }
      }
    } else {
      buffer.put(valueBuffer.array());
      valueBuffer = null;
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
    this.times = new long[rows];
    QueryDataSetUtils.readTimesFromBuffer(buffer, rows);

    QueryDataSetUtils.readValuesFromBuffer(buffer, dataTypeList, measurementSize, rows);
  }

  public void setDataTypeList(List<IoTDBDataType> dataTypeList) {
    this.dataTypeList = dataTypeList;
    setDataTypes(dataTypeList);
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

  public void setDataTypes(List<IoTDBDataType> dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = QueryDataSetUtils.getTSDataTypeByIoTDBDataType(dataTypes.get(i));
    }
  }

  public void setDataTypes(IoTDBDataType[] dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      this.dataTypes[i] = QueryDataSetUtils.getTSDataTypeByIoTDBDataType(dataTypes[i]);
    }
  }

  public Object[] getColumns() {
    return columns;
  }

  public long getMinTime() {
    if (minTime != null) {
      return minTime;
    }
    minTime = Long.MAX_VALUE;
    for (Long time: times) {
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
    long maxTime = Long.MIN_VALUE;
    for (Long time: times) {
      if (time > maxTime) {
        maxTime = time;
      }
    }
    return maxTime;
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

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }
}
