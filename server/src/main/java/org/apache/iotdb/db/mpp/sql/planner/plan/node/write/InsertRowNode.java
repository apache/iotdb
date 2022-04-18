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

import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.WritePlanNode;
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

  private static final byte TYPE_NULL = -2;

  private long time;
  private Object[] values;

  public InsertRowNode(PlanNodeId id) {
    super(id);
  }

  public InsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurements,
      TSDataType[] dataTypes,
      long time,
      Object[] values) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.time = time;
    this.values = values;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    TimePartitionSlot timePartitionSlot = StorageEngine.getTimePartitionSlot(time);
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
  public int serializedSize() {
    int size = 0;
    size += Short.BYTES;
    size += this.getPlanNodeId().serializedSize();
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
    for (String m : measurements) {
      if (m != null) {
        size += ReadWriteIOUtils.sizeToWrite(m);
      }
    }

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
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), time);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INSERT_ROW.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  public static InsertRowNode deserialize(ByteBuffer byteBuffer) {
    // TODO: (xingtanzjr) remove placeholder
    InsertRowNode insertNode = new InsertRowNode(new PlanNodeId("1"));
    insertNode.setTime(byteBuffer.getLong());
    try {
      insertNode.setDevicePath(new PartialPath(ReadWriteIOUtils.readString(byteBuffer)));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
    }
    insertNode.deserializeMeasurementsAndValues(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  void subSerialize(ByteBuffer buffer) {
    buffer.putLong(time);
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    buffer.putInt(measurementSchemas.length - countFailedMeasurements());

    for (MeasurementSchema measurement : measurementSchemas) {
      if (measurement != null) {
        measurement.serializeTo(buffer);
      }
    }

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void putValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < values.length; i++) {
      if (dataTypes[i] != null) {
        if (values[i] == null) {
          ReadWriteIOUtils.write(TYPE_NULL, buffer);
          continue;
        }
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

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort((short) PlanNodeType.INSERT_ROW.ordinal());
    getPlanNodeId().serializeToWAL(buffer);
    subSerialize(buffer);
  }

  void subSerialize(IWALByteBufferView buffer) {
    buffer.putLong(time);
    WALWriteUtils.write(devicePath.getFullPath(), buffer);
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(IWALByteBufferView buffer) {
    buffer.putInt(measurementSchemas.length - countFailedMeasurements());

    for (String measurement : measurements) {
      if (measurement != null) {
        WALWriteUtils.write(measurement, buffer);
      }
    }

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    buffer.put((byte) (isAligned ? 1 : 0));
  }

  private void putValues(IWALByteBufferView buffer) throws QueryProcessException {
    for (int i = 0; i < values.length; i++) {
      if (dataTypes[i] != null) {
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

  void deserializeMeasurementsAndValues(ByteBuffer buffer) {
    int measurementSize = buffer.getInt();

    this.measurements = new String[measurementSize];
    this.measurementSchemas = new MeasurementSchema[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
      measurements[i] = measurementSchemas[i].getMeasurementId();
    }

    this.dataTypes = new TSDataType[measurementSize];
    this.values = new Object[measurementSize];
    try {
      fillValues(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    isAligned = buffer.get() == 1;
  }

  /** Make sure the values is already inited before calling this */
  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < dataTypes.length; i++) {
      byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
      if (typeNum == TYPE_NULL) {
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

  public static InsertRowNode deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    InsertRowNode insertNode = new InsertRowNode(PlanNodeId.deserialize(stream));
    insertNode.setTime(stream.readLong());
    insertNode.setDevicePath(new PartialPath(ReadWriteIOUtils.readString(stream)));
    insertNode.deserializeMeasurementsAndValues(stream);

    return insertNode;
  }

  void deserializeMeasurementsAndValues(DataInputStream stream) throws IOException {
    int measurementSize = stream.readInt();

    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = ReadWriteIOUtils.readString(stream);
    }

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
    return time == that.time && Arrays.equals(values, that.values);
  }
}
