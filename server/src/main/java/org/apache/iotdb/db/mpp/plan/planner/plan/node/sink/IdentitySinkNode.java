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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.sink;

import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
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
import java.util.stream.Collectors;

public class IdentitySinkNode extends MultiChildrenSinkNode {

  public IdentitySinkNode(PlanNodeId id) {
    super(id);
  }

  public IdentitySinkNode(
      PlanNodeId id, List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id, downStreamChannelLocationList);
  }

  public IdentitySinkNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<DownStreamChannelLocation> downStreamChannelLocationList) {
    super(id, children, downStreamChannelLocationList);
  }

  @Override
  public PlanNode clone() {
    return new IdentitySinkNode(getPlanNodeId(), getDownStreamChannelLocationList());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .map(PlanNode::getOutputColumnNames)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitIdentitySink(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.IDENTITY_SINK.serialize(byteBuffer);
    ReadWriteIOUtils.write(downStreamChannelLocationList.size(), byteBuffer);
    for (DownStreamChannelLocation downStreamChannelLocation : downStreamChannelLocationList) {
      downStreamChannelLocation.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.IDENTITY_SINK.serialize(stream);
    ReadWriteIOUtils.write(downStreamChannelLocationList.size(), stream);
    for (DownStreamChannelLocation downStreamChannelLocation : downStreamChannelLocationList) {
      downStreamChannelLocation.serialize(stream);
    }
  }

  public static IdentitySinkNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DownStreamChannelLocation> downStreamChannelLocationList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      downStreamChannelLocationList.add(DownStreamChannelLocation.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new IdentitySinkNode(planNodeId, downStreamChannelLocationList);
  }
}
