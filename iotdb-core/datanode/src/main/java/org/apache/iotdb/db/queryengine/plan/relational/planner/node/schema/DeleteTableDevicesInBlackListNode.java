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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;

public class DeleteTableDevicesInBlackListNode extends AbstractTableDevicesDeletionNode {
  public static final DeleteTableDevicesInBlackListNode MOCK_INSTANCE =
      new DeleteTableDevicesInBlackListNode(new PlanNodeId(""), null, new byte[0]);

  public DeleteTableDevicesInBlackListNode(
      final PlanNodeId id, final String tableName, final @Nonnull byte[] patternInfo) {
    super(id, tableName, patternInfo);
  }

  @Override
  public PlanNode clone() {
    return new DeleteTableDevicesInBlackListNode(id, tableName, patternInfo);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DELETE_TABLE_DEVICES_IN_BLACK_LIST;
  }

  public static DeleteTableDevicesInBlackListNode deserialize(final ByteBuffer buffer) {
    final String tableName = ReadWriteIOUtils.readString(buffer);
    final byte[] updateBytes = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(updateBytes);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeleteTableDevicesInBlackListNode(planNodeId, tableName, updateBytes);
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitDeleteTableDevicesInBlackList(this, context);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.DELETE_TABLE_DEVICES_IN_BLACK_LIST;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitDeleteTableDevicesInBlackList(this, context);
  }
}
