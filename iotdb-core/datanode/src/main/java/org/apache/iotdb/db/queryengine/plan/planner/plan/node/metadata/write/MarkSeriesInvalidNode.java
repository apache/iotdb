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

public class MarkSeriesInvalidNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath oldPath; // Physical path to mark as invalid
  private final PartialPath newPath; // Alias path to set in ALIAS_PATH
  private final boolean isRollback; // Whether this is a rollback operation

  public MarkSeriesInvalidNode(PlanNodeId id, PartialPath oldPath, PartialPath newPath) {
    super(id);
    this.oldPath = oldPath;
    this.newPath = newPath;
    this.isRollback = false;
  }

  public MarkSeriesInvalidNode(
      PlanNodeId id, PartialPath oldPath, PartialPath newPath, boolean isRollback) {
    super(id);
    this.oldPath = oldPath;
    this.newPath = newPath;
    this.isRollback = isRollback;
  }

  public PartialPath getOldPath() {
    return oldPath;
  }

  public PartialPath getNewPath() {
    return newPath;
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
    throw new UnsupportedOperationException("MarkSeriesInvalidNode does not support children");
  }

  @Override
  public PlanNode clone() {
    return new MarkSeriesInvalidNode(getPlanNodeId(), oldPath, newPath, isRollback);
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
    return PlanNodeType.MARK_SERIES_INVALID;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitMarkSeriesInvalid(this, context);
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitMarkSeriesInvalid(this, context);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.MARK_SERIES_INVALID;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.MARK_SERIES_INVALID.serialize(byteBuffer);
    oldPath.serialize(byteBuffer);
    newPath.serialize(byteBuffer);
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.MARK_SERIES_INVALID.serialize(stream);
    oldPath.serialize(stream);
    newPath.serialize(stream);
    org.apache.tsfile.utils.ReadWriteIOUtils.write(isRollback, stream);
  }

  public static MarkSeriesInvalidNode deserialize(ByteBuffer byteBuffer) {
    PartialPath oldPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    PartialPath newPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    boolean isRollback = org.apache.tsfile.utils.ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new MarkSeriesInvalidNode(planNodeId, oldPath, newPath, isRollback);
  }

  @Override
  public String toString() {
    return String.format(
        "MarkSeriesInvalidNode-%s: mark %s as invalid, set ALIAS_PATH to %s",
        getPlanNodeId(), oldPath.getFullPath(), newPath.getFullPath());
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
    MarkSeriesInvalidNode that = (MarkSeriesInvalidNode) o;
    return isRollback == that.isRollback
        && Objects.equals(oldPath, that.oldPath)
        && Objects.equals(newPath, that.newPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), oldPath, newPath, isRollback);
  }
}
