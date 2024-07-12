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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RelationalInsertRowNode extends InsertRowNode {
  // deviceId cache for Table-view insertion
  private IDeviceID deviceID;

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

  public IDeviceID getDeviceID() {
    if (deviceID == null) {
      String[] deviceIdSegments = new String[idColumnIndices.size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < idColumnIndices.size(); i++) {
        final Integer columnIndex = idColumnIndices.get(i);
        deviceIdSegments[i + 1] = getValues()[columnIndex].toString();
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

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    super.serializeAttributes(byteBuffer);
    for (int i = 0; i < dataTypes.length; i++) {
      columnCategories[i].serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    super.serializeAttributes(stream);
    for (int i = 0; i < dataTypes.length; i++) {
      columnCategories[i].serialize(stream);
    }
  }

  public void subDeserialize(ByteBuffer buffer) {
    super.subDeserialize(buffer);
    columnCategories = new TsTableColumnCategory[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      columnCategories[i] = TsTableColumnCategory.deserialize(buffer);
    }
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + columnCategories.length * Byte.BYTES;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RELATIONAL_INSERT_TABLET;
  }

  public String getTableName() {
    return devicePath.getFullPath();
  }
}
