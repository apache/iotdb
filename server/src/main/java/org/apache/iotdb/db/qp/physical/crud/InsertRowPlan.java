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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class InsertRowPlan extends InsertPlan {

  private static final Logger logger = LoggerFactory.getLogger(InsertRowPlan.class);
  private static final byte TYPE_RAW_STRING = -1;
  private static final byte TYPE_NULL = -2;

  private long time;
  private Object[] values;

  // if isNeedInferType is true, the values must be String[], so we could infer types from them
  // if values is object[], we could use the raw type of them, and we should set this to false
  private boolean isNeedInferType = false;

  private List<Object> failedValues;
  private List<PartialPath> paths;

  public InsertRowPlan() {
    super(OperatorType.INSERT);
  }

  public InsertRowPlan(InsertRowPlan another) {
    super(OperatorType.INSERT);
    this.prefixPath = another.prefixPath;
    this.time = another.time;
    this.measurements = new String[another.measurements.length];
    System.arraycopy(another.measurements, 0, this.measurements, 0, another.measurements.length);
    this.values = new Object[another.values.length];
    System.arraycopy(another.values, 0, this.values, 0, another.values.length);
    this.dataTypes = new TSDataType[another.dataTypes.length];
    System.arraycopy(another.dataTypes, 0, this.dataTypes, 0, another.dataTypes.length);
  }

  public InsertRowPlan(
      PartialPath prefixPath, long insertTime, String[] measurementList, String[] insertValues) {
    super(Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.prefixPath = prefixPath;
    this.measurements = measurementList;
    this.dataTypes = new TSDataType[insertValues.length];
    // We need to create an Object[] for the data type casting, because we can not set Float, Long
    // to String[i]
    this.values = new Object[insertValues.length];
    System.arraycopy(insertValues, 0, values, 0, insertValues.length);
    isNeedInferType = true;
  }

  public InsertRowPlan(
      PartialPath prefixPath,
      long insertTime,
      String[] measurementList,
      ByteBuffer values,
      boolean isAligned)
      throws QueryProcessException {
    super(Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.prefixPath = prefixPath;
    this.measurements = measurementList;
    this.dataTypes = new TSDataType[measurementList.length];
    this.values = new Object[measurementList.length];
    this.fillValues(values);
    isNeedInferType = false;
    this.isAligned = isAligned;
  }

  @TestOnly
  public InsertRowPlan(
      PartialPath prefixPath,
      long insertTime,
      String[] measurements,
      TSDataType[] dataTypes,
      String[] insertValues) {
    super(OperatorType.INSERT);
    this.time = insertTime;
    this.prefixPath = prefixPath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.values = new Object[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      try {
        values[i] = CommonUtils.parseValueForTest(dataTypes[i], insertValues[i]);
      } catch (QueryProcessException e) {
        e.printStackTrace();
      }
    }
  }

  @TestOnly
  public InsertRowPlan(
      PartialPath prefixPath,
      long insertTime,
      String[] measurements,
      TSDataType[] dataTypes,
      String[] insertValues,
      boolean isAligned) {
    super(OperatorType.INSERT);
    this.time = insertTime;
    this.prefixPath = prefixPath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.values = new Object[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      try {
        values[i] = CommonUtils.parseValueForTest(dataTypes[i], insertValues[i]);
      } catch (QueryProcessException e) {
        e.printStackTrace();
      }
    }
    this.isAligned = isAligned;
  }

  @TestOnly
  public InsertRowPlan(
      PartialPath prefixPath,
      long insertTime,
      String measurement,
      TSDataType type,
      String insertValue) {
    super(OperatorType.INSERT);
    this.time = insertTime;
    this.prefixPath = prefixPath;
    this.measurements = new String[] {measurement};
    this.dataTypes = new TSDataType[] {type};
    this.values = new Object[1];
    try {
      values[0] = CommonUtils.parseValueForTest(dataTypes[0], insertValue);
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  @TestOnly
  public InsertRowPlan(TSRecord tsRecord) throws IllegalPathException {
    super(OperatorType.INSERT);
    this.prefixPath = new PartialPath(tsRecord.deviceId);
    this.time = tsRecord.time;
    this.measurements = new String[tsRecord.dataPointList.size()];
    this.measurementMNodes = new IMeasurementMNode[tsRecord.dataPointList.size()];
    this.dataTypes = new TSDataType[tsRecord.dataPointList.size()];
    this.values = new Object[tsRecord.dataPointList.size()];
    for (int i = 0; i < tsRecord.dataPointList.size(); i++) {
      measurements[i] = tsRecord.dataPointList.get(i).getMeasurementId();
      measurementMNodes[i] =
          MeasurementMNode.getMeasurementMNode(
              null,
              measurements[i],
              new UnaryMeasurementSchema(
                  measurements[i], tsRecord.dataPointList.get(i).getType(), TSEncoding.PLAIN),
              null);
      dataTypes[i] = tsRecord.dataPointList.get(i).getType();
      values[i] = tsRecord.dataPointList.get(i).getValue();
    }
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public boolean isNeedInferType() {
    return isNeedInferType;
  }

  public void setNeedInferType(boolean inferType) {
    this.isNeedInferType = inferType;
  }

  /**
   * if inferType is true, transfer String[] values to specific data types (Integer, Long, Float,
   * Double, Binary)
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void transferType() throws QueryProcessException {
    if (isNeedInferType) {
      for (int i = 0; i < measurementMNodes.length; i++) {
        if (measurementMNodes[i] == null) {
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            markFailedMeasurementInsertion(
                i,
                new QueryProcessException(
                    new PathNotExistException(
                        prefixPath.getFullPath()
                            + IoTDBConstant.PATH_SEPARATOR
                            + measurements[i])));
          } else {
            throw new QueryProcessException(
                new PathNotExistException(
                    prefixPath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i]));
          }
          continue;
        }
        if (isAligned) {
          dataTypes[i] = measurementMNodes[i].getSchema().getSubMeasurementsTSDataTypeList().get(i);
        } else {
          dataTypes[i] = measurementMNodes[i].getSchema().getType();
        }
        try {
          values[i] = CommonUtils.parseValue(dataTypes[i], values[i].toString());
        } catch (Exception e) {
          logger.warn(
              "{}.{} data type is not consistent, input {}, registered {}",
              prefixPath,
              measurements[i],
              values[i],
              dataTypes[i]);
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            markFailedMeasurementInsertion(i, e);
            measurementMNodes[i] = null;
          } else {
            throw e;
          }
        }
      }
    }
  }

  @Override
  public long getMinTime() {
    return getTime();
  }

  @Override
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    super.markFailedMeasurementInsertion(index, e);
    if (failedValues == null) {
      failedValues = new ArrayList<>();
    }
    failedValues.add(values[index]);
    values[index] = null;
    if (isNeedInferType) {
      dataTypes[index] = null;
    }
  }

  @Override
  public List<PartialPath> getPaths() {
    if (paths != null) {
      return paths;
    }
    paths = new ArrayList<>(measurements.length);
    for (String m : measurements) {
      PartialPath fullPath = prefixPath.concatNode(m);
      paths.add(fullPath);
    }
    return paths;
  }

  public Object[] getValues() {
    return this.values;
  }

  public void setValues(Object[] values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertRowPlan that = (InsertRowPlan) o;
    return time == that.time
        && Objects.equals(prefixPath, that.prefixPath)
        && Arrays.equals(measurements, that.measurements)
        && Arrays.equals(values, that.values)
        && Objects.equals(isAligned, that.isAligned);
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefixPath, time);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.INSERT.ordinal();
    stream.writeByte((byte) type);
    subSerialize(stream);
  }

  public void subSerialize(DataOutputStream stream) throws IOException {
    stream.writeLong(time);
    if (isAligned && originalPrefixPath != null) {
      putString(stream, originalPrefixPath.getFullPath());
    } else {
      putString(stream, prefixPath.getFullPath());
    }
    serializeMeasurementsAndValues(stream);
  }

  void serializeMeasurementsAndValues(DataOutputStream stream) throws IOException {
    stream.writeInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));

    for (String m : measurements) {
      if (m != null) {
        putString(stream, m);
      }
    }

    try {
      stream.writeInt(dataTypes.length);
      putValues(stream);
    } catch (QueryProcessException e) {
      throw new IOException(e);
    }

    // the types are not inferred before the plan is serialized
    stream.write((byte) (isNeedInferType ? 1 : 0));

    stream.writeLong(index);
    stream.write((byte) (isAligned ? 1 : 0));
  }

  private void putValues(DataOutputStream outputStream) throws QueryProcessException, IOException {
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        ReadWriteIOUtils.write(TYPE_NULL, outputStream);
        continue;
      }
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      if (dataTypes == null || dataTypes[i] == null) {
        ReadWriteIOUtils.write(TYPE_RAW_STRING, outputStream);
        ReadWriteIOUtils.write(values[i].toString(), outputStream);
      } else {
        ReadWriteIOUtils.write(dataTypes[i], outputStream);
        switch (dataTypes[i]) {
          case BOOLEAN:
            ReadWriteIOUtils.write((Boolean) values[i], outputStream);
            break;
          case INT32:
            ReadWriteIOUtils.write((Integer) values[i], outputStream);
            break;
          case INT64:
            ReadWriteIOUtils.write((Long) values[i], outputStream);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], outputStream);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], outputStream);
            break;
          case TEXT:
            ReadWriteIOUtils.write((Binary) values[i], outputStream);
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
        }
      }
    }
  }

  private void putValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
        continue;
      }
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      if (dataTypes == null || dataTypes[i] == null) {
        ReadWriteIOUtils.write(TYPE_RAW_STRING, buffer);
        ReadWriteIOUtils.write(values[i].toString(), buffer);
      } else {
        ReadWriteIOUtils.write(dataTypes[i], buffer);
        switch (dataTypes[i]) {
          case BOOLEAN:
            ReadWriteIOUtils.write((Boolean) values[i], buffer);
            break;
          case INT32:
            ReadWriteIOUtils.write((Integer) values[i], buffer);
            break;
          case INT64:
            ReadWriteIOUtils.write((Long) values[i], buffer);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], buffer);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], buffer);
            break;
          case TEXT:
            ReadWriteIOUtils.write((Binary) values[i], buffer);
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
        }
      }
    }
  }

  /** Make sure the values is already inited before calling this */
  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < dataTypes.length; i++) {
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
      if (typeNum == TYPE_RAW_STRING || typeNum == TYPE_NULL) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(buffer) : null;
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.INSERT.ordinal();
    buffer.put((byte) type);
    subSerialize(buffer);
  }

  public void subSerialize(ByteBuffer buffer) {
    buffer.putLong(time);
    if (isAligned && originalPrefixPath != null) {
      putString(buffer, originalPrefixPath.getFullPath());
    } else {
      putString(buffer, prefixPath.getFullPath());
    }
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    buffer.putInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));

    for (String measurement : measurements) {
      if (measurement != null) {
        putString(buffer, measurement);
      }
    }
    try {
      buffer.putInt(dataTypes.length);
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    // the types are not inferred before the plan is serialized
    buffer.put((byte) (isNeedInferType ? 1 : 0));
    buffer.putLong(index);

    buffer.put((byte) (isAligned ? 1 : 0));
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.time = buffer.getLong();
    this.prefixPath = new PartialPath(readString(buffer));
    deserializeMeasurementsAndValues(buffer);
  }

  void deserializeMeasurementsAndValues(ByteBuffer buffer) {
    int measurementSize = buffer.getInt();

    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    int dataTypeSize = buffer.getInt();
    this.dataTypes = new TSDataType[dataTypeSize];
    this.values = new Object[dataTypeSize];
    try {
      fillValues(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    isNeedInferType = buffer.get() == 1;
    this.index = buffer.getLong();
    isAligned = buffer.get() == 1;
  }

  @Override
  public String toString() {
    return "prefixPath: "
        + prefixPath
        + ", time: "
        + time
        + ", measurements: "
        + Arrays.toString(measurements)
        + ", values: "
        + Arrays.toString(values);
  }

  boolean hasFailedValues() {
    return failedValues != null && !failedValues.isEmpty();
  }

  public TimeValuePair composeTimeValuePair(int columnIndex) {
    if (columnIndex >= values.length) {
      return null;
    }
    Object value = values[columnIndex];
    return new TimeValuePair(time, TsPrimitiveType.getByType(dataTypes[columnIndex], value));
  }

  @Override
  public InsertPlan getPlanFromFailed() {
    if (super.getPlanFromFailed() == null) {
      return null;
    }
    values = failedValues.toArray(new Object[0]);
    failedValues = null;
    return this;
  }

  @Override
  public void recoverFromFailure() {
    if (failedMeasurements == null) {
      return;
    }

    for (int i = 0; i < failedMeasurements.size(); i++) {
      int index = failedIndices.get(i);
      values[index] = failedValues.get(i);
    }
    super.recoverFromFailure();

    failedValues = null;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    super.checkIntegrity();
    if (values == null) {
      throw new QueryProcessException("Values are null");
    }
    if (values.length == 0) {
      throw new QueryProcessException("The size of values is 0");
    }
    for (Object value : values) {
      if (value == null) {
        throw new QueryProcessException("Values contain null: " + Arrays.toString(values));
      }
    }
  }
}
