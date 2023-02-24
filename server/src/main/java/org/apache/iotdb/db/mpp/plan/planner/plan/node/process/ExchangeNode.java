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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ExchangeNode extends SingleChildProcessNode {
  // In current version, one ExchangeNode will only have one source.
  // And the fragment which the sourceNode belongs to will only have one instance.
  // Thus, by nodeId and endpoint, the ExchangeNode can know where its source from.
  private TEndPoint upstreamEndpoint;
  private FragmentInstanceId upstreamInstanceId;
  private PlanNodeId upstreamPlanNodeId;

  private List<String> outputColumnNames = new ArrayList<>();

  /** Exchange needs to know which child of IdentitySinkNode/ShuffleSinkNode it matches */
  private int indexOfUpstreamSinkHandle = 0;

  public ExchangeNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitExchange(this, context);
  }

  @Override
  public PlanNode clone() {
    ExchangeNode node = new ExchangeNode(getPlanNodeId());
    node.setOutputColumnNames(outputColumnNames);
    node.setIndexOfUpstreamSinkHandle(indexOfUpstreamSinkHandle);
    return node;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public void setUpstream(TEndPoint endPoint, FragmentInstanceId instanceId, PlanNodeId nodeId) {
    this.upstreamEndpoint = endPoint;
    this.upstreamInstanceId = instanceId;
    this.upstreamPlanNodeId = nodeId;
  }

  public static ExchangeNode deserialize(ByteBuffer byteBuffer) {
    TEndPoint endPoint =
        new TEndPoint(
            ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    FragmentInstanceId fragmentInstanceId = FragmentInstanceId.deserialize(byteBuffer);
    PlanNodeId upstreamPlanNodeId = PlanNodeId.deserialize(byteBuffer);
    int outputColumnNamesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumnNames = new ArrayList<>(outputColumnNamesSize);
    while (outputColumnNamesSize > 0) {
      outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      outputColumnNamesSize--;
    }
    int index = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    ExchangeNode exchangeNode = new ExchangeNode(planNodeId);
    exchangeNode.setUpstream(endPoint, fragmentInstanceId, upstreamPlanNodeId);
    exchangeNode.setOutputColumnNames(outputColumnNames);
    exchangeNode.setIndexOfUpstreamSinkHandle(index);
    return exchangeNode;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.EXCHANGE.serialize(byteBuffer);
    ReadWriteIOUtils.write(upstreamEndpoint.getIp(), byteBuffer);
    ReadWriteIOUtils.write(upstreamEndpoint.getPort(), byteBuffer);
    upstreamInstanceId.serialize(byteBuffer);
    upstreamPlanNodeId.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
    for (String outputColumnName : outputColumnNames) {
      ReadWriteIOUtils.write(outputColumnName, byteBuffer);
    }
    ReadWriteIOUtils.write(indexOfUpstreamSinkHandle, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.EXCHANGE.serialize(stream);
    ReadWriteIOUtils.write(upstreamEndpoint.getIp(), stream);
    ReadWriteIOUtils.write(upstreamEndpoint.getPort(), stream);
    upstreamInstanceId.serialize(stream);
    upstreamPlanNodeId.serialize(stream);
    ReadWriteIOUtils.write(outputColumnNames.size(), stream);
    for (String outputColumnName : outputColumnNames) {
      ReadWriteIOUtils.write(outputColumnName, stream);
    }
    ReadWriteIOUtils.write(indexOfUpstreamSinkHandle, stream);
  }

  @Override
  public String toString() {
    return String.format(
        "ExchangeNode-%s: [SourceAddress:%s]", getPlanNodeId(), getSourceAddress());
  }

  public String getSourceAddress() {
    if (getUpstreamEndpoint() == null) {
      return "Not assigned";
    }
    return String.format(
        "%s/%s/%s",
        getUpstreamEndpoint().getIp(), getUpstreamInstanceId(), getUpstreamPlanNodeId());
  }

  public int getIndexOfUpstreamSinkHandle() {
    return indexOfUpstreamSinkHandle;
  }

  public void setIndexOfUpstreamSinkHandle(int indexOfUpstreamSinkHandle) {
    this.indexOfUpstreamSinkHandle = indexOfUpstreamSinkHandle;
  }

  public TEndPoint getUpstreamEndpoint() {
    return upstreamEndpoint;
  }

  public FragmentInstanceId getUpstreamInstanceId() {
    return upstreamInstanceId;
  }

  public PlanNodeId getUpstreamPlanNodeId() {
    return upstreamPlanNodeId;
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
    ExchangeNode that = (ExchangeNode) o;
    return Objects.equals(upstreamEndpoint, that.upstreamEndpoint)
        && Objects.equals(upstreamInstanceId, that.upstreamInstanceId)
        && Objects.equals(upstreamPlanNodeId, that.upstreamPlanNodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), upstreamEndpoint, upstreamInstanceId, upstreamPlanNodeId);
  }
}
