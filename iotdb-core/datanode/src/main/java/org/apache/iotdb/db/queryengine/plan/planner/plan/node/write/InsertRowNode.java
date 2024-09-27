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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class InsertRowNode extends InsertNode implements WALEntryValue {

  private static final byte TYPE_RAW_STRING = -1;

  private static final byte TYPE_NULL_WITHOUT_TYPE = -2;
  private static final byte TYPE_NULL_WITH_TYPE = -3;

  private static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";

  protected static final String DESERIALIZE_ERROR = "Cannot deserialize InsertRowNode";

  private long time;
  private Object[] values;

  private boolean isNeedInferType = false;

  public InsertRowNode(PlanNodeId id) {
    super(id);
  }

  @TestOnly
  public InsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      long time,
      Object[] values,
      boolean isNeedInferType) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.time = time;
    this.values = values;
    this.isNeedInferType = isNeedInferType;
  }

  public InsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      MeasurementSchema[] measurementSchemas,
      long time,
      Object[] values,
      boolean isNeedInferType) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.measurementSchemas = measurementSchemas;
    this.time = time;
    this.values = values;
    this.isNeedInferType = isNeedInferType;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(time);
    this.dataRegionReplicaSet =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(
                getDeviceID(), timePartitionSlot, analysis.getDatabaseName());
    // collect redirectInfo
    analysis.setRedirectNodeList(
        Collections.singletonList(
            dataRegionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint()));
    return Collections.singletonList(this);
  }

  @Override
  public void addChild(PlanNode child) {
    // no child for InsertRowNode
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.INSERT_ROW;
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
  }

  @Override
  public String toString() {
    return "InsertRowNode{" + "time=" + time + ", values=" + Arrays.toString(values) + '}';
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public TSDataType[] getDataTypes() {
    if (isNeedInferType) {
      TSDataType[] predictedDataTypes = new TSDataType[dataTypes.length];
      for (int i = 0; i < dataTypes.length; i++) {
        predictedDataTypes[i] = TypeInferenceUtils.getPredictedDataType(values[i], true);
      }
      return predictedDataTypes;
    }

    return dataTypes;
  }

  @Override
  public TSDataType getDataType(int index) {
    if (isNeedInferType) {
      return TypeInferenceUtils.getPredictedDataType(values[index], true);
    } else {
      return dataTypes[index];
    }
  }

  public Object[] getValues() {
    return values;
  }

  public void setValues(Object[] values) {
    this.values = values;
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

  public void setNeedInferType(boolean needInferType) {
    isNeedInferType = needInferType;
  }

  @TestOnly
  public List<TTimePartitionSlot> getTimePartitionSlots() {
    return Collections.singletonList(TimePartitionUtils.getTimePartitionSlot(time));
  }

  @Override
  public void markFailedMeasurement(int index) {
    if (measurements[index] == null) {
      return;
    }
    measurements[index] = null;
    dataTypes[index] = null;
    values[index] = null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    subSerialize(stream);
  }

  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(time, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void subSerialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(time, stream);
    ReadWriteIOUtils.write(targetPath.getFullPath(), stream);
    serializeMeasurementsAndValues(stream);
  }

  /** Serialize measurements and values, ignoring failed time series. */
  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), buffer);
    serializeMeasurementsOrSchemas(buffer);
    putDataTypesAndValues(buffer);
    ReadWriteIOUtils.write((byte) (isNeedInferType ? 1 : 0), buffer);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), buffer);
  }

  /**
   * Serialize measurements and values, ignoring failed time series.
   *
   * @param stream - DataOutputStream.
   * @throws IOException - If an I/O error occurs.
   */
  void serializeMeasurementsAndValues(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), stream);
    serializeMeasurementsOrSchemas(stream);
    putDataTypesAndValues(stream);
    ReadWriteIOUtils.write((byte) (isNeedInferType ? 1 : 0), stream);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), stream);
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series. */
  private void serializeMeasurementsOrSchemas(ByteBuffer buffer) {
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), buffer);
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        measurementSchemas[i].serializeTo(buffer);
      } else {
        ReadWriteIOUtils.write(measurements[i], buffer);
      }
    }
  }

  /**
   * Serialize measurements or measurement schemas, ignoring failed time series.
   *
   * @param stream - DataOutputStream
   * @throws IOException - If an I/O error occurs.
   */
  private void serializeMeasurementsOrSchemas(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write((byte) (measurementSchemas != null ? 1 : 0), stream);
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize measurement schemas when exist
      if (measurementSchemas != null) {
        measurementSchemas[i].serializeTo(stream);
      } else {
        ReadWriteIOUtils.write(measurements[i], stream);
      }
    }
  }

  /**
   * Serialize data types and values, ignoring failed time series.
   *
   * @param buffer ByteBuffer.
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  private void putDataTypesAndValues(ByteBuffer buffer) {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        ReadWriteIOUtils.write(
            dataTypes[i] == null ? TYPE_NULL_WITHOUT_TYPE : TYPE_NULL_WITH_TYPE, buffer);
        if (dataTypes[i] != null) {
          ReadWriteIOUtils.write(dataTypes[i], buffer);
        }
        continue;
      }
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      if (isNeedInferType) {
        ReadWriteIOUtils.write(TYPE_RAW_STRING, buffer);
        ReadWriteIOUtils.write(values[i].toString(), buffer);
      } else {
        ReadWriteIOUtils.write(dataTypes[i], buffer);
        switch (dataTypes[i]) {
          case BOOLEAN:
            ReadWriteIOUtils.write((Boolean) values[i], buffer);
            break;
          case INT32:
          case DATE:
            ReadWriteIOUtils.write((Integer) values[i], buffer);
            break;
          case INT64:
          case TIMESTAMP:
            ReadWriteIOUtils.write((Long) values[i], buffer);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], buffer);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], buffer);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            ReadWriteIOUtils.write((Binary) values[i], buffer);
            break;
          default:
            throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
        }
      }
    }
  }

  /**
   * Serialize data types and values, ignoring failed time series.
   *
   * @param stream - DataOutputStream.
   * @throws IOException - If an I/O error occurs.
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  private void putDataTypesAndValues(DataOutputStream stream) throws IOException {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        ReadWriteIOUtils.write(
            dataTypes[i] == null ? TYPE_NULL_WITHOUT_TYPE : TYPE_NULL_WITH_TYPE, stream);
        if (dataTypes[i] != null) {
          ReadWriteIOUtils.write(dataTypes[i], stream);
        }
        continue;
      }
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      if (isNeedInferType) {
        ReadWriteIOUtils.write(TYPE_RAW_STRING, stream);
        ReadWriteIOUtils.write(values[i].toString(), stream);
      } else {
        ReadWriteIOUtils.write(dataTypes[i], stream);
        switch (dataTypes[i]) {
          case BOOLEAN:
            ReadWriteIOUtils.write((Boolean) values[i], stream);
            break;
          case INT32:
          case DATE:
            ReadWriteIOUtils.write((Integer) values[i], stream);
            break;
          case INT64:
          case TIMESTAMP:
            ReadWriteIOUtils.write((Long) values[i], stream);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], stream);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], stream);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            ReadWriteIOUtils.write((Binary) values[i], stream);
            break;
          default:
            throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
        }
      }
    }
  }

  public static InsertRowNode deserialize(ByteBuffer byteBuffer) {
    // TODO: (xingtanzjr) remove placeholder
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  void subDeserialize(ByteBuffer byteBuffer) {
    time = byteBuffer.getLong();
    try {
      targetPath = readTargetPath(byteBuffer);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(DESERIALIZE_ERROR, e);
    }
    deserializeMeasurementsAndValues(byteBuffer);
  }

  void deserializeMeasurementsAndValues(ByteBuffer buffer) {
    int measurementSize = buffer.getInt();

    this.measurements = new String[measurementSize];
    boolean hasSchema = buffer.get() == 1;
    if (hasSchema) {
      measurementSchemas = new MeasurementSchema[measurementSize];
      for (int i = 0; i < measurementSize; i++) {
        measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
        measurements[i] = measurementSchemas[i].getMeasurementId();
      }
    } else {
      for (int i = 0; i < measurementSize; i++) {
        measurements[i] = ReadWriteIOUtils.readString(buffer);
      }
    }

    dataTypes = new TSDataType[measurementSize];
    values = new Object[measurementSize];
    fillDataTypesAndValues(buffer);

    isNeedInferType = buffer.get() == 1;
    isAligned = buffer.get() == 1;
  }

  /**
   * Make sure the dataTypes and values have been created before calling this.
   *
   * @param buffer - ByteBuffer
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  private void fillDataTypesAndValues(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      // types are not determined, the situation mainly occurs when the node uses string values
      // and is forwarded to other nodes
      byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
      if (typeNum == TYPE_RAW_STRING
          || typeNum == TYPE_NULL_WITHOUT_TYPE
          || typeNum == TYPE_NULL_WITH_TYPE) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(buffer) : null;
        if (typeNum == TYPE_NULL_WITH_TYPE) {
          dataTypes[i] = ReadWriteIOUtils.readDataType(buffer);
        }
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
        case DATE:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
        case TIMESTAMP:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
        case STRING:
        case BLOB:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }
  }

  @Override
  public long getMinTime() {
    return getTime();
  }

  // region serialize & deserialize methods for WAL
  /** Serialized size for wal. */
  @Override
  public int serializedSize() {
    return Short.BYTES + Long.BYTES + subSerializeSize();
  }

  protected int subSerializeSize() {
    int size = 0;
    size += Long.BYTES;
    size += ReadWriteIOUtils.sizeToWrite(targetPath.getFullPath());
    return size + serializeMeasurementsAndValuesSize();
  }

  private int serializeMeasurementsAndValuesSize() {
    int size = 0;
    size += Integer.BYTES;

    size += serializeMeasurementSchemasSize();

    // putValues
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        size += Byte.BYTES;
        if (dataTypes[i] != null) {
          size += Byte.BYTES;
        }
        continue;
      }
      size += Byte.BYTES;
      switch (dataTypes[i]) {
        case BOOLEAN:
          size += Byte.BYTES;
          break;
        case INT32:
        case DATE:
          size += Integer.BYTES;
          break;
        case INT64:
        case TIMESTAMP:
          size += Long.BYTES;
          break;
        case FLOAT:
          size += Float.BYTES;
          break;
        case DOUBLE:
          size += Double.BYTES;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          size += ReadWriteIOUtils.sizeToWrite((Binary) values[i]);
          break;
        default:
          throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }

    size += Byte.BYTES;
    return size;
  }

  /**
   * Compared with {@link this#serialize(ByteBuffer)}, more info: search index, less info:
   * isNeedInferType
   */
  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(searchIndex);
    subSerialize(buffer);
  }

  protected void subSerialize(IWALByteBufferView buffer) {
    buffer.putLong(time);
    WALWriteUtils.write(targetPath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  /** Serialize measurements and values, ignoring failed time series. */
  private void serializeMeasurementsAndValues(IWALByteBufferView buffer) {
    buffer.putInt(measurements.length - getFailedMeasurementNumber());
    serializeMeasurementSchemasToWAL(buffer);
    putDataTypesAndValues(buffer);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  /**
   * Serialize data types and values, ignoring failed time series.
   *
   * @param buffer - IWALByteBufferView
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  private void putDataTypesAndValues(IWALByteBufferView buffer) {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        WALWriteUtils.write(
            dataTypes[i] == null ? TYPE_NULL_WITHOUT_TYPE : TYPE_NULL_WITH_TYPE, buffer);
        if (dataTypes[i] != null) {
          WALWriteUtils.write(dataTypes[i], buffer);
        }
        continue;
      }
      WALWriteUtils.write(dataTypes[i], buffer);
      switch (dataTypes[i]) {
        case BOOLEAN:
          WALWriteUtils.write((Boolean) values[i], buffer);
          break;
        case INT32:
        case DATE:
          WALWriteUtils.write((Integer) values[i], buffer);
          break;
        case INT64:
        case TIMESTAMP:
          WALWriteUtils.write((Long) values[i], buffer);
          break;
        case FLOAT:
          WALWriteUtils.write((Float) values[i], buffer);
          break;
        case DOUBLE:
          WALWriteUtils.write((Double) values[i], buffer);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          WALWriteUtils.write((Binary) values[i], buffer);
          break;
        default:
          throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }
  }

  /**
   * Deserialize from wal.
   *
   * @param stream - DataInputStream
   * @return InsertRowNode
   * @throws IOException - If an I/O error occurs.
   * @throws IllegalArgumentException - If meets illegal argument.
   */
  public static InsertRowNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    InsertRowNode insertNode = subDeserializeFromWAL(stream);
    insertNode.setSearchIndex(searchIndex);
    return insertNode;
  }

  protected static InsertRowNode subDeserializeFromWAL(DataInputStream stream) throws IOException {
    // we do not store plan node id in wal entry
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.setTime(stream.readLong());
    try {
      insertNode.setTargetPath(insertNode.readTargetPath(stream));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(DESERIALIZE_ERROR, e);
    }
    insertNode.deserializeMeasurementsAndValuesFromWAL(stream);
    return insertNode;
  }

  void deserializeMeasurementsAndValuesFromWAL(DataInputStream stream) throws IOException {
    int measurementSize = stream.readInt();

    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    dataTypes = new TSDataType[measurementSize];
    deserializeMeasurementSchemas(stream);

    values = new Object[measurementSize];
    fillDataTypesAndValuesFromWAL(stream);

    isAligned = stream.readByte() == 1;
  }

  /**
   * Make sure the dataTypes and values have been created before calling this.
   *
   * @param stream - DataInputStream
   * @throws IOException - If an I/O error occurs.
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  public void fillDataTypesAndValuesFromWAL(DataInputStream stream) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      byte typeNum = stream.readByte();
      if (typeNum == TYPE_RAW_STRING
          || typeNum == TYPE_NULL_WITHOUT_TYPE
          || typeNum == TYPE_NULL_WITH_TYPE) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(stream) : null;
        if (typeNum == TYPE_NULL_WITH_TYPE) {
          dataTypes[i] = ReadWriteIOUtils.readDataType(stream);
        }
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(stream);
          break;
        case INT32:
        case DATE:
          values[i] = ReadWriteIOUtils.readInt(stream);
          break;
        case INT64:
        case TIMESTAMP:
          values[i] = ReadWriteIOUtils.readLong(stream);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(stream);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(stream);
          break;
        case TEXT:
        case STRING:
        case BLOB:
          values[i] = ReadWriteIOUtils.readBinary(stream);
          break;
        default:
          throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }
  }

  /**
   * Deserialize from wal.
   *
   * @param buffer - ByteBuffer
   * @return InsertRowNode
   * @throws IllegalArgumentException - If meets illegal argument
   */
  public static InsertRowNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    InsertRowNode insertNode = subDeserializeFromWAL(buffer);
    insertNode.setSearchIndex(searchIndex);
    return insertNode;
  }

  protected static InsertRowNode subDeserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.setTime(buffer.getLong());
    try {
      insertNode.setTargetPath(insertNode.readTargetPath(buffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(DESERIALIZE_ERROR, e);
    }
    insertNode.deserializeMeasurementsAndValuesFromWAL(buffer);

    return insertNode;
  }

  void deserializeMeasurementsAndValuesFromWAL(ByteBuffer buffer) {
    int measurementSize = buffer.getInt();

    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchemas(buffer);

    dataTypes = new TSDataType[measurementSize];
    values = new Object[measurementSize];
    fillDataTypesAndValuesFromWAL(buffer);

    isAligned = buffer.get() == 1;
  }

  /**
   * Make sure the dataTypes and values have been created before calling this.
   *
   * @param buffer - ByteBuffer
   * @throws UnSupportedDataTypeException - If meets unsupported data type.
   */
  public void fillDataTypesAndValuesFromWAL(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      byte typeNum = buffer.get();
      if (typeNum == TYPE_RAW_STRING
          || typeNum == TYPE_NULL_WITHOUT_TYPE
          || typeNum == TYPE_NULL_WITH_TYPE) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(buffer) : null;
        if (typeNum == TYPE_NULL_WITH_TYPE) {
          dataTypes[i] = ReadWriteIOUtils.readDataType(buffer);
        }
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
        case DATE:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
        case TIMESTAMP:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
        case STRING:
        case BLOB:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new UnSupportedDataTypeException(UNSUPPORTED_DATA_TYPE + dataTypes[i]);
      }
    }
  }

  // endregion

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InsertRowNode that = (InsertRowNode) o;
    return time == that.time
        && isNeedInferType == that.isNeedInferType
        && Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), time, isNeedInferType);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRow(this, context);
  }

  public TimeValuePair composeTimeValuePair(int columnIndex) {
    if (columnIndex >= values.length) {
      return null;
    }
    Object value = values[columnIndex];
    return new TimeValuePair(time, TsPrimitiveType.getByType(dataTypes[columnIndex], value));
  }
}
