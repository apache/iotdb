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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.sink;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.Validate;

import java.nio.ByteBuffer;
import java.util.List;

public class FragmentSinkNode extends SinkNode {
  private PlanNode child;

  private Endpoint downStreamEndpoint;
  private FragmentInstanceId downStreamInstanceId;
  private PlanNodeId downStreamPlanNodeId;

  public FragmentSinkNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public PlanNode clone() {
    FragmentSinkNode sinkNode = new FragmentSinkNode(getPlanNodeId());
    sinkNode.setDownStream(downStreamEndpoint, downStreamInstanceId, downStreamPlanNodeId);
    return sinkNode;
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    Validate.isTrue(
        children == null || children.size() == 1,
        "Children size of FragmentSinkNode should be 0 or 1");
    FragmentSinkNode sinkNode = (FragmentSinkNode) clone();
    if (children != null) {
      sinkNode.setChild(children.get(0));
    }
    return sinkNode;
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  public static FragmentSinkNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public void send() {}

  @Override
  public void close() throws Exception {}

  public PlanNode getChild() {
    return child;
  }

  public void setChild(PlanNode child) {
    this.child = child;
  }

  public String toString() {
    return String.format(
        "FragmentSinkNode-%s:[SendTo: (%s)]", getPlanNodeId(), getDownStreamAddress());
  }

  public String getDownStreamAddress() {
    if (getDownStreamEndpoint() == null) {
      return "Not assigned";
    }
    return String.format(
        "%s/%s/%s",
        getDownStreamEndpoint().getIp(), getDownStreamInstanceId(), getDownStreamPlanNodeId());
  }

  public void setDownStream(Endpoint endPoint, FragmentInstanceId instanceId, PlanNodeId nodeId) {
    this.downStreamEndpoint = endPoint;
    this.downStreamInstanceId = instanceId;
    this.downStreamPlanNodeId = nodeId;
  }

  public void setDownStreamPlanNodeId(PlanNodeId downStreamPlanNodeId) {
    this.downStreamPlanNodeId = downStreamPlanNodeId;
  }

  public Endpoint getDownStreamEndpoint() {
    return downStreamEndpoint;
  }

  public FragmentInstanceId getDownStreamInstanceId() {
    return downStreamInstanceId;
  }

  public PlanNodeId getDownStreamPlanNodeId() {
    return downStreamPlanNodeId;
  }
}
