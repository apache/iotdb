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

package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.commons.partition.Endpoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;

public class ExchangeNode extends PlanNode {
  private PlanNode child;
  // The remoteSourceNode is used to record the remote source info for current ExchangeNode
  // It is not the child of current ExchangeNode
  private FragmentSinkNode remoteSourceNode;

  // In current version, one ExchangeNode will only have one source.
  // And the fragment which the sourceNode belongs to will only have one instance.
  // Thus, by nodeId and endpoint, the ExchangeNode can know where its source from.
  private Endpoint upstreamEndpoint;
  private FragmentInstanceId upstreamInstanceId;
  private PlanNodeId upstreamPlanNodeId;

  public ExchangeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    if (this.child == null) {
      return ImmutableList.of();
    }
    return ImmutableList.of(child);
  }

  @Override
  public void addChild(PlanNode child) {
    this.child = child;
  }

  @Override
  public PlanNode clone() {
    ExchangeNode node = new ExchangeNode(getId());
    if (remoteSourceNode != null) {
      FragmentSinkNode remoteSourceNodeClone = (FragmentSinkNode) remoteSourceNode.clone();
      remoteSourceNodeClone.setDownStreamNode(node);
      node.setRemoteSourceNode(remoteSourceNode);
    }
    return node;
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  public void setUpstream(Endpoint endPoint, FragmentInstanceId instanceId, PlanNodeId nodeId) {
    this.upstreamEndpoint = endPoint;
    this.upstreamInstanceId = instanceId;
    this.upstreamPlanNodeId = nodeId;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static ExchangeNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  public PlanNode getChild() {
    return child;
  }

  public void setChild(PlanNode child) {
    this.child = child;
  }

  public String toString() {
    return String.format("ExchangeNode-%s: [SourceAddress:%s]", getId(), getSourceAddress());
  }

  public String getSourceAddress() {
    if (getUpstreamEndpoint() == null) {
      return "Not assigned";
    }
    return String.format(
        "%s/%s/%s",
        getUpstreamEndpoint().getIp(), getUpstreamInstanceId(), getUpstreamPlanNodeId());
  }

  public FragmentSinkNode getRemoteSourceNode() {
    return remoteSourceNode;
  }

  public void setRemoteSourceNode(FragmentSinkNode remoteSourceNode) {
    this.remoteSourceNode = remoteSourceNode;
  }

  public void cleanChildren() {
    this.child = null;
  }

  public Endpoint getUpstreamEndpoint() {
    return upstreamEndpoint;
  }

  public FragmentInstanceId getUpstreamInstanceId() {
    return upstreamInstanceId;
  }

  public PlanNodeId getUpstreamPlanNodeId() {
    return upstreamPlanNodeId;
  }
}
