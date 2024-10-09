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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TableNodeLocationAddNode extends PlanNode implements ISchemaRegionPlan {
  private final TDataNodeLocation location;

  public static final TableNodeLocationAddNode MOCK_INSTANCE =
      new TableNodeLocationAddNode(new PlanNodeId(""), null);

  public TableNodeLocationAddNode(final PlanNodeId id, final TDataNodeLocation location) {
    super(id);
    this.location = location;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(final PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNode clone() {
    return new TableNodeLocationAddNode(id, location);
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
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(location.getDataNodeId(), byteBuffer);
    ThriftCommonsSerDeUtils.serializeTEndPoint(location.getInternalEndPoint(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(location.getDataNodeId(), stream);
    ThriftCommonsSerDeUtils.serializeTEndPoint(location.getInternalEndPoint(), stream);
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final TDataNodeLocation location =
        new TDataNodeLocation(
            ReadWriteIOUtils.readInt(buffer),
            null,
            ThriftCommonsSerDeUtils.deserializeTEndPoint(buffer),
            null,
            null,
            null);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableNodeLocationAddNode(planNodeId, location);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_LOCATION_ADD;
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.ADD_NODE_LOCATION;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitAddNodeLocation(this, context);
  }
}
