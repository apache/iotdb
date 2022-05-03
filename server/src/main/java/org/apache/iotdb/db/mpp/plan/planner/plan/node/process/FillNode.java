/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** FillNode is used to fill the empty field in one row. */
public class FillNode extends ProcessNode {

  // descriptions of how null values are filled
  private List<FillDescriptor> fillDescriptorList;

  private PlanNode child;

  public FillNode(PlanNodeId id) {
    super(id);
  }

  public FillNode(PlanNodeId id, List<FillDescriptor> fillDescriptorList) {
    this(id);
    this.fillDescriptorList = fillDescriptorList;
  }

  public FillNode(PlanNodeId id, PlanNode child, List<FillDescriptor> fillDescriptorList) {
    this(id, fillDescriptorList);
    this.child = child;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public PlanNode clone() {
    return new FillNode(getPlanNodeId(), fillDescriptorList);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFill(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FILL.serialize(byteBuffer);
    ReadWriteIOUtils.write(fillDescriptorList.size(), byteBuffer);
    for (FillDescriptor fillDescriptor : fillDescriptorList) {
      fillDescriptor.serialize(byteBuffer);
    }
  }

  public static FillNode deserialize(ByteBuffer byteBuffer) {
    int fillDescriptorsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<FillDescriptor> fillDescriptorList = new ArrayList<>();
    while (fillDescriptorsSize > 0) {
      fillDescriptorList.add(FillDescriptor.deserialize(byteBuffer));
      fillDescriptorsSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new FillNode(planNodeId, fillDescriptorList);
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
    FillNode fillNode = (FillNode) o;
    return fillDescriptorList.equals(fillNode.fillDescriptorList) && child.equals(fillNode.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), fillDescriptorList, child);
  }
}
