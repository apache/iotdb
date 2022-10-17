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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ActivateTemplateNode extends WritePlanNode implements IActivateTemplateInClusterPlan {

  private PartialPath activatePath;
  private int templateSetLevel;
  private int templateId;

  private boolean isAligned;

  private TRegionReplicaSet regionReplicaSet;

  public ActivateTemplateNode(
      PlanNodeId id, PartialPath activatePath, int templateSetLevel, int templateId) {
    super(id);
    this.activatePath = activatePath;
    this.templateSetLevel = templateSetLevel;
    this.templateId = templateId;
  }

  public PartialPath getActivatePath() {
    return activatePath;
  }

  public void setActivatePath(PartialPath activatePath) {
    this.activatePath = activatePath;
  }

  public int getTemplateSetLevel() {
    return templateSetLevel;
  }

  public void setTemplateSetLevel(int templateSetLevel) {
    this.templateSetLevel = templateSetLevel;
  }

  public int getTemplateId() {
    return templateId;
  }

  public void setTemplateId(int templateId) {
    this.templateId = templateId;
  }

  @Override
  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public void setAligned(boolean aligned) {
    this.isAligned = aligned;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new ActivateTemplateNode(getPlanNodeId(), activatePath, templateSetLevel, templateId);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ACTIVATE_TEMPLATE.serialize(byteBuffer);

    activatePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(templateSetLevel, byteBuffer);
    ReadWriteIOUtils.write(templateId, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ACTIVATE_TEMPLATE.serialize(stream);

    activatePath.serialize(stream);
    ReadWriteIOUtils.write(templateSetLevel, stream);
    ReadWriteIOUtils.write(templateId, stream);
  }

  public static ActivateTemplateNode deserialize(ByteBuffer buffer) {

    PartialPath activatePath = (PartialPath) PathDeserializeUtil.deserialize(buffer);
    int templateSetLevel = ReadWriteIOUtils.readInt(buffer);
    int templateId = ReadWriteIOUtils.readInt(buffer);

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);

    return new ActivateTemplateNode(planNodeId, activatePath, templateSetLevel, templateId);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    TRegionReplicaSet regionReplicaSet =
        analysis.getSchemaPartitionInfo().getSchemaRegionReplicaSet(activatePath.getFullPath());
    setRegionReplicaSet(regionReplicaSet);
    return ImmutableList.of(this);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitActivateTemplate(this, context);
  }
}
