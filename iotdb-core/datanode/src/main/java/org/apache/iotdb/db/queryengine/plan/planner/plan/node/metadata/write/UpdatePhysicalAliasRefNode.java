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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UpdatePhysicalAliasRefNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath physicalPath; // Physical path to update
  private final PartialPath newAliasPath; // New alias path to set in ALIAS_PATH
  private final PartialPath oldAliasPath; // Old alias path (for rollback)
  private final boolean isRollback; // True if this is a rollback operation

  public UpdatePhysicalAliasRefNode(
      PlanNodeId id, PartialPath physicalPath, PartialPath newAliasPath) {
    super(id);
    this.physicalPath = physicalPath;
    this.newAliasPath = newAliasPath;
    this.oldAliasPath = null;
    this.isRollback = false; // Default to false for backward compatibility
  }

  public UpdatePhysicalAliasRefNode(
      PlanNodeId id, PartialPath physicalPath, PartialPath newAliasPath, boolean isRollback) {
    super(id);
    this.physicalPath = physicalPath;
    this.newAliasPath = newAliasPath;
    this.oldAliasPath = null;
    this.isRollback = isRollback;
  }

  public UpdatePhysicalAliasRefNode(
      PlanNodeId id,
      PartialPath physicalPath,
      PartialPath newAliasPath,
      PartialPath oldAliasPath,
      boolean isRollback) {
    super(id);
    this.physicalPath = physicalPath;
    this.newAliasPath = newAliasPath;
    this.oldAliasPath = oldAliasPath;
    this.isRollback = isRollback;
  }

  public PartialPath getPhysicalPath() {
    return physicalPath;
  }

  public PartialPath getNewAliasPath() {
    return newAliasPath;
  }

  public PartialPath getOldAliasPath() {
    return oldAliasPath;
  }

  public boolean isRollback() {
    return isRollback;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("UpdatePhysicalAliasRefNode does not support children");
  }

  @Override
  public PlanNode clone() {
    return new UpdatePhysicalAliasRefNode(
        getPlanNodeId(), physicalPath, newAliasPath, oldAliasPath, isRollback);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.UPDATE_PHYSICAL_ALIAS_REF;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitUpdatePhysicalAliasRef(this, context);
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitUpdatePhysicalAliasRef(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.UPDATE_PHYSICAL_ALIAS_REF.serialize(byteBuffer);
    physicalPath.serialize(byteBuffer);
    newAliasPath.serialize(byteBuffer);
    // Serialize oldAliasPath
    if (oldAliasPath != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, byteBuffer);
      oldAliasPath.serialize(byteBuffer);
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
    // Serialize isRollback
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.UPDATE_PHYSICAL_ALIAS_REF.serialize(stream);
    physicalPath.serialize(stream);
    newAliasPath.serialize(stream);
    // Serialize oldAliasPath
    if (oldAliasPath != null) {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 1, stream);
      oldAliasPath.serialize(stream);
    } else {
      org.apache.tsfile.utils.ReadWriteIOUtils.write((byte) 0, stream);
    }
    // Serialize isRollback
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, stream);
  }

  public static UpdatePhysicalAliasRefNode deserialize(ByteBuffer byteBuffer) {
    PartialPath physicalPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    PartialPath newAliasPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    // Deserialize oldAliasPath
    PartialPath oldAliasPath = null;
    byte hasOldAliasPath = org.apache.tsfile.utils.ReadWriteIOUtils.readByte(byteBuffer);
    if (hasOldAliasPath == 1) {
      oldAliasPath =
          (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    }
    // Deserialize isRollback
    boolean isRollback = org.apache.tsfile.utils.ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new UpdatePhysicalAliasRefNode(
        planNodeId, physicalPath, newAliasPath, oldAliasPath, isRollback);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.UPDATE_PHYSICAL_ALIAS_REF;
  }

  @Override
  public String toString() {
    return String.format(
        "UpdatePhysicalAliasRefNode-%s: update %s ALIAS_PATH to %s",
        getPlanNodeId(), physicalPath.getFullPath(), newAliasPath.getFullPath());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    UpdatePhysicalAliasRefNode that = (UpdatePhysicalAliasRefNode) o;
    return isRollback == that.isRollback
        && Objects.equals(physicalPath, that.physicalPath)
        && Objects.equals(newAliasPath, that.newAliasPath)
        && Objects.equals(oldAliasPath, that.oldAliasPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), physicalPath, newAliasPath, oldAliasPath, isRollback);
  }
}
