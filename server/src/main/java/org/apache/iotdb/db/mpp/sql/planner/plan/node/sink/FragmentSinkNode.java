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
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;

public class FragmentSinkNode extends SinkNode {
  private PlanNode child;
  private ExchangeNode downStreamNode;

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
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNode clone() {
    FragmentSinkNode sinkNode = new FragmentSinkNode(getId());
    sinkNode.setDownStream(downStreamEndpoint, downStreamInstanceId, downStreamPlanNodeId);
    sinkNode.setDownStreamNode(downStreamNode);
    return sinkNode;
  }

  @Override
  public int allowedChildCount() {
    return ONE_CHILD;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static FragmentSinkNode deserialize(ByteBuffer byteBuffer) {
    Endpoint downStreamEndpoint =
        new Endpoint(ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
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
    return String.format("FragmentSinkNode-%s:[SendTo: (%s)]", getId(), getDownStreamAddress());
  }

  public String getDownStreamAddress() {
    if (getDownStreamEndpoint() == null) {
      return "Not assigned";
    }
    return String.format(
        "%s/%s/%s",
        getDownStreamEndpoint().getIp(), getDownStreamInstanceId(), getDownStreamPlanNodeId());
  }

  public ExchangeNode getDownStreamNode() {
    return downStreamNode;
  }

  public void setDownStreamNode(ExchangeNode downStreamNode) {
    this.downStreamNode = downStreamNode;
  }

  public void setDownStream(Endpoint endPoint, FragmentInstanceId instanceId, PlanNodeId nodeId) {
    this.downStreamEndpoint = endPoint;
    this.downStreamInstanceId = instanceId;
    this.downStreamPlanNodeId = nodeId;
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
