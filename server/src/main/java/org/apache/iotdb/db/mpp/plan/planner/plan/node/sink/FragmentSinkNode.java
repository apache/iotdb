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
package org.apache.iotdb.db.mpp.plan.planner.plan.node.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.Validate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class FragmentSinkNode extends SinkNode {
  private PlanNode child;

  private TEndPoint downStreamEndpoint;
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitFragmentSink(this, context);
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
  public List<String> getOutputColumnNames() {
    return child.getOutputColumnNames();
  }

  public static FragmentSinkNode deserialize(ByteBuffer byteBuffer) {
    TEndPoint downStreamEndpoint =
        new TEndPoint(
            ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    FragmentInstanceId downStreamInstanceId = FragmentInstanceId.deserialize(byteBuffer);
    PlanNodeId downStreamPlanNodeId = PlanNodeId.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);

    FragmentSinkNode fragmentSinkNode = new FragmentSinkNode(planNodeId);
    fragmentSinkNode.downStreamEndpoint = downStreamEndpoint;
    fragmentSinkNode.downStreamInstanceId = downStreamInstanceId;
    fragmentSinkNode.downStreamPlanNodeId = downStreamPlanNodeId;
    return fragmentSinkNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FRAGMENT_SINK.serialize(byteBuffer);
    ReadWriteIOUtils.write(downStreamEndpoint.getIp(), byteBuffer);
    ReadWriteIOUtils.write(downStreamEndpoint.getPort(), byteBuffer);
    downStreamInstanceId.serialize(byteBuffer);
    downStreamPlanNodeId.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FRAGMENT_SINK.serialize(stream);
    ReadWriteIOUtils.write(downStreamEndpoint.getIp(), stream);
    ReadWriteIOUtils.write(downStreamEndpoint.getPort(), stream);
    downStreamInstanceId.serialize(stream);
    downStreamPlanNodeId.serialize(stream);
  }

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
        "%s:%d/%s/%s",
        getDownStreamEndpoint().getIp(),
        getDownStreamEndpoint().getPort(),
        getDownStreamInstanceId(),
        getDownStreamPlanNodeId());
  }

  public void setDownStream(TEndPoint endPoint, FragmentInstanceId instanceId, PlanNodeId nodeId) {
    this.downStreamEndpoint = endPoint;
    this.downStreamInstanceId = instanceId;
    this.downStreamPlanNodeId = nodeId;
  }

  public void setDownStreamPlanNodeId(PlanNodeId downStreamPlanNodeId) {
    this.downStreamPlanNodeId = downStreamPlanNodeId;
  }

  public TEndPoint getDownStreamEndpoint() {
    return downStreamEndpoint;
  }

  public FragmentInstanceId getDownStreamInstanceId() {
    return downStreamInstanceId;
  }

  public PlanNodeId getDownStreamPlanNodeId() {
    return downStreamPlanNodeId;
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
    FragmentSinkNode that = (FragmentSinkNode) o;
    return Objects.equals(child, that.child)
        && Objects.equals(downStreamEndpoint, that.downStreamEndpoint)
        && Objects.equals(downStreamInstanceId, that.downStreamInstanceId)
        && Objects.equals(downStreamPlanNodeId, that.downStreamPlanNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), child, downStreamEndpoint, downStreamInstanceId, downStreamPlanNodeId);
  }
}
