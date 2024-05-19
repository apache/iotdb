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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class DeleteTableDeviceNode extends PlanNode {

  private final String table;

  public DeleteTableDeviceNode(PlanNodeId id, String table) {
    super(id);
    this.table = table;
  }

  public String getTable() {
    return table;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeleteTableDevice(this, context);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new DeleteTableDeviceNode(getPlanNodeId(), table);
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_TABLE_DEVICE.serialize(byteBuffer);
    ReadWriteIOUtils.write(table, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DELETE_TABLE_DEVICE.serialize(stream);
    ReadWriteIOUtils.write(table, stream);
  }

  public static DeleteTableDeviceNode deserialize(ByteBuffer buffer) {
    String table = ReadWriteIOUtils.readString(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeleteTableDeviceNode(planNodeId, table);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeleteTableDeviceNode)) return false;
    if (!super.equals(o)) return false;
    DeleteTableDeviceNode that = (DeleteTableDeviceNode) o;
    return Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), table);
  }
}
