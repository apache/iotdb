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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RelationalInsertRowNode extends InsertRowNode {

  public RelationalInsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      long time,
      Object[] values,
      boolean isNeedInferType,
      TsTableColumnCategory[] columnCategories) {
    super(id, devicePath, isAligned, measurements, dataTypes, time, values, isNeedInferType);
    setColumnCategories(columnCategories);
  }

  public RelationalInsertRowNode(PlanNodeId id) {
    super(id);
  }

  public RelationalInsertRowNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      MeasurementSchema[] measurementSchemas,
      long time,
      Object[] values,
      boolean isNeedInferType,
      TsTableColumnCategory[] columnCategories) {
    super(
        id,
        devicePath,
        isAligned,
        measurements,
        dataTypes,
        measurementSchemas,
        time,
        values,
        isNeedInferType);
    setColumnCategories(columnCategories);
  }

  @Override
  public IDeviceID getDeviceID() {
    if (deviceID == null) {
      String[] deviceIdSegments = new String[idColumnIndices.size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < idColumnIndices.size(); i++) {
        final Integer columnIndex = idColumnIndices.get(i);
        deviceIdSegments[i + 1] =
            getValues()[columnIndex] != null ? getValues()[columnIndex].toString() : null;
      }
      deviceID = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    }

    return deviceID;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRelationalInsertRow(this, context);
  }

  public static RelationalInsertRowNode deserialize(ByteBuffer byteBuffer) {
    RelationalInsertRowNode insertNode = new RelationalInsertRowNode(new PlanNodeId(""));
    insertNode.subDeserialize(byteBuffer);
    insertNode.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return insertNode;
  }

  /**
   * Deserialize from wal.
   *
   * @param stream - DataInputStream
   * @return RelationalInsertRowNode
   * @throws IOException - If an I/O error occurs.
   * @throws IllegalArgumentException - If meets illegal argument.
   */
  public static RelationalInsertRowNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    long searchIndex = stream.readLong();
    RelationalInsertRowNode insertNode = subDeserializeFromWAL(stream);
    insertNode.setSearchIndex(searchIndex);
    return insertNode;
  }

  public static RelationalInsertRowNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    RelationalInsertRowNode insertNode = subDeserializeFromWAL(buffer);
    insertNode.setSearchIndex(searchIndex);
    return insertNode;
  }

  protected static RelationalInsertRowNode subDeserializeFromWAL(DataInputStream stream)
      throws IOException {
    // we do not store plan node id in wal entry
    RelationalInsertRowNode insertNode = new RelationalInsertRowNode(new PlanNodeId(""));
    insertNode.setTime(stream.readLong());
    try {
      insertNode.setTargetPath(insertNode.readTargetPath(stream));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(DESERIALIZE_ERROR, e);
    }
    insertNode.deserializeMeasurementsAndValuesFromWAL(stream);
    TsTableColumnCategory[] newColumnCategories =
        new TsTableColumnCategory[insertNode.dataTypes.length];
    for (int i = 0; i < insertNode.dataTypes.length; i++) {
      newColumnCategories[i] = TsTableColumnCategory.deserialize(stream);
    }
    insertNode.setColumnCategories(newColumnCategories);
    return insertNode;
  }

  protected static RelationalInsertRowNode subDeserializeFromWAL(ByteBuffer buffer) {
    // we do not store plan node id in wal entry
    RelationalInsertRowNode insertNode = new RelationalInsertRowNode(new PlanNodeId(""));
    insertNode.setTime(buffer.getLong());
    try {
      insertNode.setTargetPath(insertNode.readTargetPath(buffer));
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException(DESERIALIZE_ERROR, e);
    }
    insertNode.deserializeMeasurementsAndValuesFromWAL(buffer);
    TsTableColumnCategory[] newColumnCategories =
        new TsTableColumnCategory[insertNode.dataTypes.length];
    for (int i = 0; i < insertNode.dataTypes.length; i++) {
      newColumnCategories[i] = TsTableColumnCategory.deserialize(buffer);
    }
    insertNode.setColumnCategories(newColumnCategories);
    return insertNode;
  }

  @Override
  void subSerialize(ByteBuffer buffer) {
    super.subSerialize(buffer);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        columnCategories[i].serialize(buffer);
      }
    }
  }

  @Override
  void subSerialize(DataOutputStream stream) throws IOException {
    super.subSerialize(stream);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        columnCategories[i].serialize(stream);
      }
    }
  }

  @Override
  protected void subSerialize(IWALByteBufferView buffer) {
    super.subSerialize(buffer);
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        buffer.put(columnCategories[i].getCategory());
      }
    }
  }

  public void subDeserialize(ByteBuffer buffer) {
    super.subDeserialize(buffer);
    TsTableColumnCategory[] newColumnCategories = new TsTableColumnCategory[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      newColumnCategories[i] = TsTableColumnCategory.deserialize(buffer);
    }
    setColumnCategories(newColumnCategories);
  }

  @Override
  protected int subSerializeSize() {
    return super.subSerializeSize() + columnCategories.length * Byte.BYTES;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_ROW;
  }

  public String getTableName() {
    return targetPath.getFullPath();
  }

  protected PartialPath readTargetPath(ByteBuffer buffer) throws IllegalPathException {
    return new PartialPath(ReadWriteIOUtils.readString(buffer), false);
  }

  protected PartialPath readTargetPath(DataInputStream stream)
      throws IllegalPathException, IOException {
    return new PartialPath(ReadWriteIOUtils.readString(stream), false);
  }

  @Override
  public void updateLastCache(String databaseName) {
    String[] rawMeasurements = getRawMeasurements();
    TimeValuePair[] timeValuePairs = new TimeValuePair[rawMeasurements.length];
    for (int i = 0; i < rawMeasurements.length; i++) {
      timeValuePairs[i] = composeTimeValuePair(i);
    }
    TableDeviceSchemaCache.getInstance()
        .updateLastCacheIfExists(databaseName, getDeviceID(), rawMeasurements, timeValuePairs);
  }
}
