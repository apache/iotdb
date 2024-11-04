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
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ConstructDevicesBlackListNode extends PlanNode implements ISchemaRegionPlan {
  private final String tableName;
  private final byte[] updateBytes;

  public static final ConstructDevicesBlackListNode MOCK_INSTANCE =
      new ConstructDevicesBlackListNode(new PlanNodeId(""), null, new byte[0]);

  protected ConstructDevicesBlackListNode(
      final PlanNodeId id, final String tableName, final @Nonnull byte[] updateBytes) {
    super(id);
    this.tableName = tableName;
    this.updateBytes = updateBytes;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(final PlanNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlanNode clone() {
    return new ConstructDevicesBlackListNode(id, tableName, updateBytes);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CONSTRUCT_TABLE_DEVICES_BLACK_LIST;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);
    ReadWriteIOUtils.write(updateBytes.length, byteBuffer);
    byteBuffer.put(updateBytes);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(updateBytes.length, stream);
    stream.write(updateBytes);
  }

  public static ConstructDevicesBlackListNode deserialize(final ByteBuffer buffer) {
    final String tableName = ReadWriteIOUtils.readString(buffer);
    final byte[] updateBytes = new byte[ReadWriteIOUtils.readInt(buffer)];
    buffer.get(updateBytes);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ConstructDevicesBlackListNode(planNodeId, tableName, updateBytes);
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
