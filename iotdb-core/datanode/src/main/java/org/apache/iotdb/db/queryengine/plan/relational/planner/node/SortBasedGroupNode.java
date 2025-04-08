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

import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * SortBasedGroupNode is a auxiliary node that is used to group data. Currently, it is implemented
 * based on SortNode. It will only be generated some special node that required grouping source,
 * such as FillNode and TableFunctionNode.
 *
 * <p>SortBasedGroupNode's ordering schema consists of two parts: PartitionKey and OrderKey. It
 * guarantees to return data grouped by PartitionKey and sorted by OrderKey. For example, PARTITION
 * BY device_id ORDER BY time will return data grouped by device_id, and in each group, data will be
 * sorted by time.
 */
public class SortBasedGroupNode extends SortNode {

  /**
   * orderingScheme may include two parts: PartitionKey and OrderKey. It marks the number of
   * PartitionKey.
   */
  private int partitionKeyCount;

  /** SortBasedGroupNode can be pushed down for paralleled execution. */
  private boolean enableParalleled = false;

  public SortBasedGroupNode(
      PlanNodeId id, PlanNode child, OrderingScheme scheme, int partitionKeyCount) {
    super(id, child, scheme, false, false);
    this.partitionKeyCount = partitionKeyCount;
  }

  public SortBasedGroupNode(
      PlanNodeId id,
      PlanNode child,
      OrderingScheme scheme,
      boolean partial,
      boolean orderByAllIdsAndTime,
      boolean enableParalleled,
      int partitionKeyCount) {
    super(id, child, scheme, partial, orderByAllIdsAndTime);
    this.enableParalleled = enableParalleled;
    this.partitionKeyCount = partitionKeyCount;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new SortBasedGroupNode(
        id,
        Iterables.getOnlyElement(newChildren),
        orderingScheme,
        partial,
        orderByAllIdsAndTime,
        enableParalleled,
        partitionKeyCount);
  }

  public boolean isEnableParalleled() {
    return enableParalleled;
  }

  public void setEnableParalleled(boolean enableParalleled) {
    this.enableParalleled = enableParalleled;
  }

  public int getPartitionKeyCount() {
    return partitionKeyCount;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSortBasedGroup(this, context);
  }

  @Override
  public PlanNode clone() {
    return new SortBasedGroupNode(
        id,
        null,
        orderingScheme,
        partial,
        orderByAllIdsAndTime,
        enableParalleled,
        partitionKeyCount);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_SORT_BASED_GROUP_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
    ReadWriteIOUtils.write(enableParalleled, byteBuffer);
    ReadWriteIOUtils.write(partitionKeyCount, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_SORT_BASED_GROUP_NODE.serialize(stream);
    orderingScheme.serialize(stream);
    ReadWriteIOUtils.write(enableParalleled, stream);
    ReadWriteIOUtils.write(partitionKeyCount, stream);
  }

  public static SortBasedGroupNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    boolean enableParalleled = ReadWriteIOUtils.readBoolean(byteBuffer);
    int partitionColumnCount = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SortBasedGroupNode(
        planNodeId, null, orderingScheme, false, false, enableParalleled, partitionColumnCount);
  }

  @Override
  public String toString() {
    return "AuxSortNode-" + this.getPlanNodeId();
  }
}
