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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class StreamSortNode extends SortNode {

  // means that the first 0~streamCompareKeyEndIndex sort keys in OrderingScheme are all IDs or
  // Attributes
  private final int streamCompareKeyEndIndex;

  public StreamSortNode(
      PlanNodeId id,
      PlanNode child,
      OrderingScheme scheme,
      boolean partial,
      boolean orderByAllIdsAndTime,
      int streamCompareKeyEndIndex) {
    super(id, child, scheme, partial, orderByAllIdsAndTime);
    this.streamCompareKeyEndIndex = streamCompareKeyEndIndex;
  }

  public int getStreamCompareKeyEndIndex() {
    return streamCompareKeyEndIndex;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new StreamSortNode(
        id,
        Iterables.getOnlyElement(newChildren),
        orderingScheme,
        partial,
        orderByAllIdsAndTime,
        streamCompareKeyEndIndex);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitStreamSort(this, context);
  }

  @Override
  public PlanNode clone() {
    return new StreamSortNode(
        id, null, orderingScheme, partial, orderByAllIdsAndTime, streamCompareKeyEndIndex);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_STREAM_SORT_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
    ReadWriteIOUtils.write(streamCompareKeyEndIndex, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_STREAM_SORT_NODE.serialize(stream);
    orderingScheme.serialize(stream);
    ReadWriteIOUtils.write(streamCompareKeyEndIndex, stream);
  }

  public static SortNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    int streamCompareKeyEndIndex = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new StreamSortNode(
        planNodeId, null, orderingScheme, false, false, streamCompareKeyEndIndex);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    StreamSortNode streamSortNode = (StreamSortNode) o;
    return Objects.equal(streamCompareKeyEndIndex, streamSortNode.streamCompareKeyEndIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), streamCompareKeyEndIndex);
  }

  @Override
  public String toString() {
    return "StreamSortNode-" + this.getPlanNodeId();
  }
}
