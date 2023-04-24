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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FastInsertRowNode extends InsertRowNode {

  private ByteBuffer rawValues;

  public FastInsertRowNode(PlanNodeId id) {
    super(id);
  }

  public FastInsertRowNode(PlanNodeId id, PartialPath devicePath, long time, ByteBuffer values) {
    super(id, devicePath, true, null, null, time, null, false);
    this.rawValues = values;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFastInsertRow(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FAST_INSERT_ROW.serialize(byteBuffer);
    subSerialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FAST_INSERT_ROW.serialize(stream);
    subSerialize(stream);
  }

  // TODO: (FASTWRITE) (侯昊男) 增加 byteBuffer 字段后，相应的序列化反序列化方法要改一下
  void subSerialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(getTime(), buffer);
    ReadWriteIOUtils.write(devicePath.getFullPath(), buffer);
    serializeValues(buffer);
  }

  void subSerialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getTime(), stream);
    ReadWriteIOUtils.write(devicePath.getFullPath(), stream);
    serializeValues(stream);
  }

  /** Serialize measurements and values, ignoring failed time series */
  void serializeValues(ByteBuffer buffer) {
    ReadWriteIOUtils.write(rawValues, buffer);
  }

  /** Serialize measurements and values, ignoring failed time series */
  void serializeValues(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(rawValues, stream);
  }

  public static FastInsertRowNode deserialize(ByteBuffer byteBuffer) {
    // TODO: (xingtanzjr) remove placeholder
    FastInsertRowNode insertNode = new FastInsertRowNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  void subDeserialize(ByteBuffer byteBuffer) {
    setTime(byteBuffer.getLong());
    try {
      devicePath = new PartialPath(ReadWriteIOUtils.readString(byteBuffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Cannot deserialize InsertRowNode", e);
    }
    deserializeValues(byteBuffer);
  }

  void deserializeValues(ByteBuffer byteBuffer) {
    int length = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] bytes = ReadWriteIOUtils.readBytes(byteBuffer, length);
    this.rawValues = ByteBuffer.wrap(bytes);
  }

  public boolean hasFailedMeasurements() {
    return false;
  }

  @Override
  public void initMeasurementSchemaContainer(int size, String[] measurements) {
    this.measurementSchemas = new MeasurementSchema[size];
    this.measurements = measurements;
    this.dataTypes = new TSDataType[size];
  }

  @Override
  public void validateDeviceSchema(boolean isAligned) {
    this.isAligned = isAligned;
  }

  @Override
  public void updateAfterSchemaValidation() throws QueryProcessException {}

  @Override
  public void validateMeasurementSchema(int index, IMeasurementSchemaInfo measurementSchemaInfo) {
    measurementSchemas[index] = measurementSchemaInfo.getSchema();
    this.dataTypes[index] = measurementSchemaInfo.getSchema().getType();
  }

  public void fillValues() throws QueryProcessException {
    this.values = new Object[measurements.length];
    for (int i = 0; i < dataTypes.length; i++) {
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(this.rawValues);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(this.rawValues);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(this.rawValues);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(this.rawValues);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(this.rawValues);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(this.rawValues);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }
}
