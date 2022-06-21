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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class InsertRowNode extends InsertNode implements WALEntryValue {

  private static final Logger logger = LoggerFactory.getLogger(InsertRowNode.class);

  private static final byte TYPE_RAW_STRING = -1;

  private static final byte TYPE_NULL = -2;

  private long time;
  private Object[] values;

  private boolean isNeedInferType = false;

  public InsertRowNode(PlanNodeId id) {
    super(id);
  }

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

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    TTimePartitionSlot timePartitionSlot = StorageEngineV2.getTimePartitionSlot(time);
    this.dataRegionReplicaSet =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionSlot);
    return Collections.singletonList(this);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of Insert is not implemented");
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
    return Collections.singletonList(StorageEngineV2.getTimePartitionSlot(time));
  }

  @Override
  public boolean validateAndSetSchema(SchemaTree schemaTree) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));
    if (deviceSchemaInfo.isAligned() != isAligned) {
      return false;
    }
    this.measurementSchemas =
        deviceSchemaInfo.getMeasurementSchemaList().toArray(new MeasurementSchema[0]);

    // transfer data types from string values when necessary
    try {
      transferType();
    } catch (QueryProcessException e) {
      return false;
    }

    // validate whether data types are matched
    return selfCheckDataTypes();
  }

  /**
   * transfer String[] values to specific data types when isNeedInferType is true. <br>
   * Notice: measurementSchemas must be initialized before calling this method
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void transferType() throws QueryProcessException {
    if (!isNeedInferType) {
      return;
    }

    for (int i = 0; i < measurementSchemas.length; i++) {
      // null when time series doesn't exist
      if (measurementSchemas[i] == null) {
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw new QueryProcessException(
              new PathNotExistException(
                  devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i]));
        } else {
          markFailedMeasurement(
              i,
              new QueryProcessException(
                  new PathNotExistException(
                      devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i])));
        }
        continue;
      }
      // parse string value to specific type
      dataTypes[i] = measurementSchemas[i].getType();
      try {
        values[i] = CommonUtils.parseValue(dataTypes[i], values[i].toString());
      } catch (Exception e) {
        logger.warn(
            "data type of {}.{} is not consistent, registered type {}, inserting timestamp {}, value {}",
            devicePath,
            measurements[i],
            dataTypes[i],
            time,
            values[i]);
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw e;
        } else {
          markFailedMeasurement(i, e);
        }
      }
    }
    isNeedInferType = false;
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (measurements[index] == null) {
      return;
    }

    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }

    FailedMeasurementInfo failedMeasurementInfo =
        new FailedMeasurementInfo(measurements[index], dataTypes[index], values[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
    values[index] = null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_ROW.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INSERT_ROW.serialize(stream);
    subSerialize(stream);
  }

  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(time, buffer);
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void subSerialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(time, stream);
    ReadWriteIOUtils.write(devicePath.getFullPath(), stream);
    serializeMeasurementsAndValues(stream);
  }

  /** Serialize measurements and values, ignoring failed time series */
  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), buffer);
    serializeMeasurementsOrSchemas(buffer);
    putDataTypesAndValues(buffer);
    ReadWriteIOUtils.write((byte) (isNeedInferType ? 1 : 0), buffer);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), buffer);
  }

  /** Serialize measurements and values, ignoring failed time series */
  void serializeMeasurementsAndValues(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(measurements.length - getFailedMeasurementNumber(), stream);
    serializeMeasurementsOrSchemas(stream);
    putDataTypesAndValues(stream);
    ReadWriteIOUtils.write((byte) (isNeedInferType ? 1 : 0), stream);
    ReadWriteIOUtils.write((byte) (isAligned ? 1 : 0), stream);
  }

  /** Serialize measurements or measurement schemas, ignoring failed time series */
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

  /** Serialize measurements or measurement schemas, ignoring failed time series */
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

  /** Serialize data types and values, ignoring failed time series */
  private void putDataTypesAndValues(ByteBuffer buffer) {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
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
            throw new RuntimeException("Unsupported data type:" + dataTypes[i]);
        }
      }
    }
  }

  /** Serialize data types and values, ignoring failed time series */
  private void putDataTypesAndValues(DataOutputStream stream) throws IOException {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        ReadWriteIOUtils.write(TYPE_NULL, stream);
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
            ReadWriteIOUtils.write((Integer) values[i], stream);
            break;
          case INT64:
            ReadWriteIOUtils.write((Long) values[i], stream);
            break;
          case FLOAT:
            ReadWriteIOUtils.write((Float) values[i], stream);
            break;
          case DOUBLE:
            ReadWriteIOUtils.write((Double) values[i], stream);
            break;
          case TEXT:
            ReadWriteIOUtils.write((Binary) values[i], stream);
            break;
          default:
            throw new RuntimeException("Unsupported data type:" + dataTypes[i]);
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
      devicePath = new PartialPath(ReadWriteIOUtils.readString(byteBuffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
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

  /** Make sure the dataTypes and values have been created before calling this */
  private void fillDataTypesAndValues(ByteBuffer buffer) {
    for (int i = 0; i < dataTypes.length; i++) {
      // types are not determined, the situation mainly occurs when the node uses string values
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
          throw new RuntimeException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  @Override
  public long getMinTime() {
    return getTime();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    return values[index];
  }

  // region serialize & deserialize methods for WAL
  /** Serialized size for wal */
  @Override
  public int serializedSize() {
    return Short.BYTES + subSerializeSize();
  }

  private int subSerializeSize() {
    int size = 0;
    size += Long.BYTES * 2;
    size += ReadWriteIOUtils.sizeToWrite(devicePath.getFullPath());
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
        continue;
      }
      size += Byte.BYTES;
      switch (dataTypes[i]) {
        case BOOLEAN:
          size += Byte.BYTES;
          break;
        case INT32:
          size += Integer.BYTES;
          break;
        case INT64:
          size += Long.BYTES;
          break;
        case FLOAT:
          size += Float.BYTES;
          break;
        case DOUBLE:
          size += Double.BYTES;
          break;
        case TEXT:
          size += ReadWriteIOUtils.sizeToWrite((Binary) values[i]);
          break;
      }
    }

    size += Byte.BYTES;
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort((short) PlanNodeType.INSERT_ROW.ordinal());
    subSerialize(buffer);
  }

  private void subSerialize(IWALByteBufferView buffer) {
    buffer.putLong(searchIndex);
    buffer.putLong(time);
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  /** Serialize measurements and values, ignoring failed time series */
  private void serializeMeasurementsAndValues(IWALByteBufferView buffer) {
    buffer.putInt(measurementSchemas.length - getFailedMeasurementNumber());
    serializeMeasurementSchemasToWAL(buffer);
    putDataTypesAndValues(buffer);
    buffer.put((byte) (isAligned ? 1 : 0));
  }

  /** Serialize data types and values, ignoring failed time series */
  private void putDataTypesAndValues(IWALByteBufferView buffer) {
    for (int i = 0; i < values.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      // serialize null value
      if (values[i] == null) {
        WALWriteUtils.write(TYPE_NULL, buffer);
        continue;
      }
      WALWriteUtils.write(dataTypes[i], buffer);
      switch (dataTypes[i]) {
        case BOOLEAN:
          WALWriteUtils.write((Boolean) values[i], buffer);
          break;
        case INT32:
          WALWriteUtils.write((Integer) values[i], buffer);
          break;
        case INT64:
          WALWriteUtils.write((Long) values[i], buffer);
          break;
        case FLOAT:
          WALWriteUtils.write((Float) values[i], buffer);
          break;
        case DOUBLE:
          WALWriteUtils.write((Double) values[i], buffer);
          break;
        case TEXT:
          WALWriteUtils.write((Binary) values[i], buffer);
          break;
        default:
          throw new RuntimeException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  /** Deserialize from wal */
  public static InsertRowNode deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    // we do not store plan node id in wal entry
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.setSearchIndex(stream.readLong());
    insertNode.setTime(stream.readLong());
    insertNode.setDevicePath(new PartialPath(ReadWriteIOUtils.readString(stream)));
    insertNode.deserializeMeasurementsAndValues(stream);

    return insertNode;
  }

  void deserializeMeasurementsAndValues(DataInputStream stream) throws IOException {
    int measurementSize = stream.readInt();

    measurements = new String[measurementSize];
    measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchemas(stream);

    dataTypes = new TSDataType[measurementSize];
    values = new Object[measurementSize];
    fillDataTypesAndValues(stream);

    isAligned = stream.readByte() == 1;
  }

  /** Make sure the dataTypes and values have been created before calling this */
  public void fillDataTypesAndValues(DataInputStream stream) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      byte typeNum = stream.readByte();
      if (typeNum == TYPE_NULL) {
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(stream);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(stream);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(stream);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(stream);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(stream);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(stream);
          break;
        default:
          throw new RuntimeException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }
  // endregion

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
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
