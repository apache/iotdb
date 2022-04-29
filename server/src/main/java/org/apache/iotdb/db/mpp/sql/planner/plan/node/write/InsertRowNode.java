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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
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

  @Override
  public boolean validateSchema(SchemaTree schemaTree) {
    DeviceSchemaInfo deviceSchemaInfo =
        schemaTree.searchDeviceSchemaInfo(devicePath, Arrays.asList(measurements));

    List<MeasurementSchema> measurementSchemas = deviceSchemaInfo.getMeasurementSchemaList();

    if (isNeedInferType) {
      try {
        transferType(measurementSchemas);
      } catch (QueryProcessException e) {
        return false;
      }
    } else {
      // todo partial insert
      if (deviceSchemaInfo.isAligned() != isAligned) {
        return false;
      }

      for (int i = 0; i < measurementSchemas.size(); i++) {
        if (dataTypes[i] != measurementSchemas.get(i).getType()) {
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            return false;
          } else {
            markFailedMeasurementInsertion(
                i,
                new DataTypeMismatchException(
                    devicePath.getFullPath(),
                    measurements[i],
                    measurementSchemas.get(i).getType(),
                    dataTypes[i]));
          }
        }
      }
    }

    // filter failed measurements
    measurements = Arrays.stream(measurements).filter(Objects::nonNull).toArray(String[]::new);
    dataTypes = Arrays.stream(dataTypes).filter(Objects::nonNull).toArray(TSDataType[]::new);
    values = Arrays.stream(values).filter(Objects::nonNull).toArray(Object[]::new);

    return true;
  }

  @Override
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    super.markFailedMeasurementInsertion(index, e);
    values[index] = null;
    dataTypes[index] = null;
  }

  /**
   * if inferType is true, transfer String[] values to specific data types (Integer, Long, Float,
   * Double, Binary)
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void transferType(List<MeasurementSchema> measurementSchemas)
      throws QueryProcessException {
    if (isNeedInferType) {
      for (int i = 0; i < measurementSchemas.size(); i++) {
        if (measurementSchemas.get(i) == null) {
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            markFailedMeasurementInsertion(
                i,
                new QueryProcessException(
                    new PathNotExistException(
                        devicePath.getFullPath()
                            + IoTDBConstant.PATH_SEPARATOR
                            + measurements[i])));
          } else {
            throw new QueryProcessException(
                new PathNotExistException(
                    devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i]));
          }
          continue;
        }

        dataTypes[i] = measurementSchemas.get(i).getType();
        try {
          values[i] = CommonUtils.parseValue(dataTypes[i], values[i].toString());
        } catch (Exception e) {
          logger.warn(
              "{}.{} data type is not consistent, input {}, registered {}",
              devicePath,
              measurements[i],
              values[i],
              dataTypes[i]);
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            markFailedMeasurementInsertion(i, e);
          } else {
            throw e;
          }
        }
      }
      isNeedInferType = false;
    }
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Short.BYTES;
    return size + subSerializeSize();
  }

  int subSerializeSize() {
    int size = 0;
    size += Long.BYTES;
    size += ReadWriteIOUtils.sizeToWrite(devicePath.getFullPath());
    return size + serializeMeasurementsAndValuesSize();
  }

  int serializeMeasurementsAndValuesSize() {
    int size = 0;
    size += Integer.BYTES;

    size += serializeMeasurementSchemaSize();

    // putValues
    for (int i = 0; i < values.length; i++) {
      if (dataTypes[i] != null) {
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
    }

    size += Byte.BYTES;
    return size;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_ROW.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  void subSerialize(ByteBuffer buffer) {
    buffer.putLong(time);
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    buffer.putInt(measurements.length);

    // check whether has measurement schemas or not
    buffer.put((byte) (measurementSchemas != null ? 1 : 0));
    if (measurementSchemas != null) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        measurementSchema.serializeTo(buffer);
      }
    } else {
      for (String measurement : measurements) {
        ReadWriteIOUtils.write(measurement, buffer);
      }
    }

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    buffer.put((byte) (isNeedInferType ? 1 : 0));
    buffer.put((byte) (isAligned ? 1 : 0));
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

  public static InsertRowNode deserialize(ByteBuffer byteBuffer) {
    // TODO: (xingtanzjr) remove placeholder
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  public void subDeserialize(ByteBuffer byteBuffer) {
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
      this.measurementSchemas = new MeasurementSchema[measurementSize];
      for (int i = 0; i < measurementSize; i++) {
        measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
        measurements[i] = measurementSchemas[i].getMeasurementId();
      }
    } else {
      for (int i = 0; i < measurementSize; i++) {
        measurements[i] = ReadWriteIOUtils.readString(buffer);
      }
    }

    this.dataTypes = new TSDataType[measurementSize];
    this.values = new Object[measurementSize];
    try {
      fillValues(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    isNeedInferType = buffer.get() == 1;
    isAligned = buffer.get() == 1;
  }

  /** Make sure the values is already inited before calling this */
  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
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
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort((short) PlanNodeType.INSERT_ROW.ordinal());
    subSerialize(buffer);
  }

  void subSerialize(IWALByteBufferView buffer) {
    buffer.putLong(time);
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(IWALByteBufferView buffer) {
    buffer.putInt(measurementSchemas.length);

    serializeMeasurementSchemaToWAL(buffer);

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void putValues(IWALByteBufferView buffer) throws QueryProcessException {
    // todo remove serialize datatype after serializing measurement schema
    for (int i = 0; i < values.length; i++) {
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
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  public static InsertRowNode deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    // This method is used for deserialize from wal
    // we do not store plan node id in wal entry
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId(""));
    insertNode.setTime(stream.readLong());
    insertNode.setDevicePath(new PartialPath(ReadWriteIOUtils.readString(stream)));
    insertNode.deserializeMeasurementsAndValues(stream);

    return insertNode;
  }

  void deserializeMeasurementsAndValues(DataInputStream stream) throws IOException {
    int measurementSize = stream.readInt();

    this.measurements = new String[measurementSize];
    this.measurementSchemas = new MeasurementSchema[measurementSize];
    deserializeMeasurementSchema(stream);

    this.dataTypes = new TSDataType[measurementSize];
    this.values = new Object[measurementSize];
    try {
      fillValues(stream);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    isAligned = stream.readByte() == 1;
  }

  /** Make sure the values is already inited before calling this */
  public void fillValues(DataInputStream stream) throws QueryProcessException, IOException {
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
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

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
}
