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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreDeactivateTemplateNode extends PlanNode implements IPreDeactivateTemplatePlan {

  private Map<PartialPath, List<Integer>> templateSetInfo;

  public PreDeactivateTemplateNode(PlanNodeId id, Map<PartialPath, List<Integer>> templateSetInfo) {
    super(id);
    this.templateSetInfo = templateSetInfo;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new PreDeactivateTemplateNode(getPlanNodeId(), templateSetInfo);
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitPreDeactivateTemplate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.PRE_DEACTIVATE_TEMPLATE_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(templateSetInfo.size(), byteBuffer);
    templateSetInfo.forEach(
        (k, v) -> {
          k.serialize(byteBuffer);
          ReadWriteIOUtils.write(v.size(), byteBuffer);
          for (int templateId : v) {
            ReadWriteIOUtils.write(templateId, byteBuffer);
          }
        });
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.PRE_DEACTIVATE_TEMPLATE_NODE.serialize(stream);
    ReadWriteIOUtils.write(templateSetInfo.size(), stream);
    for (Map.Entry<PartialPath, List<Integer>> entry : templateSetInfo.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (int templateId : entry.getValue()) {
        ReadWriteIOUtils.write(templateId, stream);
      }
    }
  }

  public static PreDeactivateTemplateNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    Map<PartialPath, List<Integer>> templateSetInfo = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      PartialPath pattern = (PartialPath) PathDeserializeUtil.deserialize(buffer);
      int templateNum = ReadWriteIOUtils.readInt(buffer);
      List<Integer> templateIdList = new ArrayList<>(templateNum);
      for (int j = 0; j < templateNum; j++) {
        templateIdList.add(ReadWriteIOUtils.readInt(buffer));
      }
      templateSetInfo.put(pattern, templateIdList);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new PreDeactivateTemplateNode(planNodeId, templateSetInfo);
  }

  @Override
  public Map<PartialPath, List<Integer>> getTemplateSetInfo() {
    return templateSetInfo;
  }

  @Override
  public void setTemplateSetInfo(Map<PartialPath, List<Integer>> templateSetInfo) {
    this.templateSetInfo = templateSetInfo;
  }
}
