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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BatchActivateTemplateNode extends WritePlanNode {

  // devicePath -> <templateId, templateSetLevel>
  private final Map<PartialPath, Pair<Integer, Integer>> templateActivationMap;

  private TRegionReplicaSet regionReplicaSet;

  public BatchActivateTemplateNode(
      PlanNodeId id, Map<PartialPath, Pair<Integer, Integer>> templateActivationMap) {
    super(id);
    this.templateActivationMap = templateActivationMap;
  }

  private BatchActivateTemplateNode(
      PlanNodeId id,
      Map<PartialPath, Pair<Integer, Integer>> templateActivationMap,
      TRegionReplicaSet regionReplicaSet) {
    super(id);
    this.templateActivationMap = templateActivationMap;
    this.regionReplicaSet = regionReplicaSet;
  }

  public Map<PartialPath, Pair<Integer, Integer>> getTemplateActivationMap() {
    return templateActivationMap;
  }

  public PartialPath getPathSetTemplate(PartialPath devicePath) {
    return new PartialPath(
        Arrays.copyOf(devicePath.getNodes(), templateActivationMap.get(devicePath).right + 1));
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {
    // Do nothing
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.BATCH_ACTIVATE_TEMPLATE;
  }

  @Override
  public PlanNode clone() {
    return new BatchActivateTemplateNode(getPlanNodeId(), templateActivationMap, regionReplicaSet);
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
    PlanNodeType.BATCH_ACTIVATE_TEMPLATE.serialize(byteBuffer);

    int size = templateActivationMap.size();
    ReadWriteIOUtils.write(size, byteBuffer);
    for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry : templateActivationMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().left, byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().right, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.BATCH_ACTIVATE_TEMPLATE.serialize(stream);

    int size = templateActivationMap.size();
    ReadWriteIOUtils.write(size, stream);
    for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry : templateActivationMap.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().left, stream);
      ReadWriteIOUtils.write(entry.getValue().right, stream);
    }
  }

  public static BatchActivateTemplateNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      templateActivationMap.put(
          (PartialPath) PathDeserializeUtil.deserialize(byteBuffer),
          new Pair<>(ReadWriteIOUtils.readInt(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer)));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new BatchActivateTemplateNode(planNodeId, templateActivationMap);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    // gather devices to same target region
    Map<TRegionReplicaSet, Map<PartialPath, Pair<Integer, Integer>>> splitMap = new HashMap<>();
    for (Map.Entry<PartialPath, Pair<Integer, Integer>> entry : templateActivationMap.entrySet()) {
      TRegionReplicaSet regionReplicaSet =
          analysis
              .getSchemaPartitionInfo()
              .getSchemaRegionReplicaSet(entry.getKey().getIDeviceIDAsFullDevice());
      splitMap
          .computeIfAbsent(regionReplicaSet, k -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
    }

    List<WritePlanNode> result = new ArrayList<>();
    for (Map.Entry<TRegionReplicaSet, Map<PartialPath, Pair<Integer, Integer>>> entry :
        splitMap.entrySet()) {
      result.add(new BatchActivateTemplateNode(getPlanNodeId(), entry.getValue(), entry.getKey()));
    }

    return result;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitBatchActivateTemplate(this, context);
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
    BatchActivateTemplateNode that = (BatchActivateTemplateNode) o;
    return Objects.equals(templateActivationMap, that.templateActivationMap)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), templateActivationMap, regionReplicaSet);
  }
}
