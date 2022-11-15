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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.object;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ProcessNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class ObjectSerializeNode extends ProcessNode {

  private PlanNode child;

  public ObjectSerializeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.singletonList(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNode clone() {
    return new ObjectSerializeNode(getPlanNodeId());
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitObjectSerialize(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.OBJECT_SERIALIZE.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.OBJECT_SERIALIZE.serialize(stream);
  }

  public static ObjectSerializeNode deserialize(ByteBuffer buffer) {
    return new ObjectSerializeNode(PlanNodeId.deserialize(buffer));
  }
}
