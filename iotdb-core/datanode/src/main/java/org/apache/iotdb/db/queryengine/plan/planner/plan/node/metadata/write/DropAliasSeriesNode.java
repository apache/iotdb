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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class DropAliasSeriesNode extends PlanNode implements ISchemaRegionPlan {

  private final PartialPath aliasPath; // Alias path to drop

  public DropAliasSeriesNode(PlanNodeId id, PartialPath aliasPath) {
    super(id);
    this.aliasPath = aliasPath;
  }

  public PartialPath getAliasPath() {
    return aliasPath;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new DropAliasSeriesNode(getPlanNodeId(), aliasPath);
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
    return PlanNodeType.DROP_ALIAS_SERIES;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDropAliasSeries(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DROP_ALIAS_SERIES.serialize(byteBuffer);
    aliasPath.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DROP_ALIAS_SERIES.serialize(stream);
    aliasPath.serialize(stream);
  }

  public static DropAliasSeriesNode deserialize(ByteBuffer byteBuffer) {
    PartialPath aliasPath =
        (PartialPath) org.apache.iotdb.commons.path.PathDeserializeUtil.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DropAliasSeriesNode(planNodeId, aliasPath);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.DROP_ALIAS_SERIES;
  }

  @Override
  public <R, C> R accept(SchemaRegionPlanVisitor<R, C> visitor, C context) {
    return visitor.visitDropAliasSeries(this, context);
  }

  @Override
  public String toString() {
    return String.format(
        "DropAliasSeriesNode-%s: drop alias series %s", getPlanNodeId(), aliasPath.getFullPath());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DropAliasSeriesNode that = (DropAliasSeriesNode) o;
    return Objects.equals(aliasPath, that.aliasPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aliasPath);
  }
}
