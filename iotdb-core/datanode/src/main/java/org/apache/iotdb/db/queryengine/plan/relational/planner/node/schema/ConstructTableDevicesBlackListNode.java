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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ConstructTableDevicesBlackListNode extends AbstractTableDevicesDeletionNode {
  public static final ConstructTableDevicesBlackListNode MOCK_INSTANCE =
      new ConstructTableDevicesBlackListNode(new PlanNodeId(""), null, new byte[0], new byte[0]);

  private final byte[] filterInfo;

  public ConstructTableDevicesBlackListNode(
      final PlanNodeId id,
      final String tableName,
      final @Nonnull byte[] patternInfo,
      final @Nonnull byte[] filterInfo) {
    super(id, tableName, patternInfo);
    this.filterInfo = filterInfo;
  }

  public byte[] getFilterInfo() {
    return filterInfo;
  }

  @Override
  public PlanNode clone() {
    return new ConstructTableDevicesBlackListNode(id, tableName, patternInfo, filterInfo);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CONSTRUCT_TABLE_DEVICES_BLACK_LIST;
  }

  public static ConstructTableDevicesBlackListNode deserialize(final ByteBuffer buffer) {
    final String tableName = ReadWriteIOUtils.readString(buffer);
    final byte[] patternInfo = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(patternInfo);
    final byte[] filterInfo = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(filterInfo);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ConstructTableDevicesBlackListNode(planNodeId, tableName, patternInfo, filterInfo);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    super.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(filterInfo.length, byteBuffer);
    byteBuffer.put(filterInfo);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    super.serializeAttributes(stream);
    ReadWriteIOUtils.write(filterInfo.length, stream);
    stream.write(filterInfo);
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitConstructTableDevicesBlackList(this, context);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CONSTRUCT_TABLE_DEVICES_BLACK_LIST;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitConstructTableDevicesBlackList(this, context);
  }
}
