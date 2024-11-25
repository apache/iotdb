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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class TableAttributeColumnDropNode extends PlanNode implements ISchemaRegionPlan {
  private final String tableName;
  private final int columnId;
  public static final TableAttributeColumnDropNode MOCK_INSTANCE =
      new TableAttributeColumnDropNode(new PlanNodeId(""), null, 0);

  public TableAttributeColumnDropNode(
      final PlanNodeId id, final String tableName, final int columnId) {
    super(id);
    this.tableName = tableName;
    this.columnId = columnId;
  }

  public String getTableName() {
    return tableName;
  }

  public int getColumnId() {
    return columnId;
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
    return new TableAttributeColumnDropNode(id, tableName, columnId);
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
    return PlanNodeType.TABLE_ATTRIBUTE_COLUMN_DROP;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);
    ReadWriteIOUtils.write(columnId, byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(columnId, stream);
  }

  public static TableAttributeColumnDropNode deserialize(final ByteBuffer buffer) {
    final String tableName = ReadWriteIOUtils.readString(buffer);
    final int columnId = ReadWriteIOUtils.readInt(buffer);
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableAttributeColumnDropNode(planNodeId, tableName, columnId);
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitTableAttributeColumnDrop(this, context);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.DROP_TABLE_ATTRIBUTE;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitDropTableAttribute(this, context);
  }
}
