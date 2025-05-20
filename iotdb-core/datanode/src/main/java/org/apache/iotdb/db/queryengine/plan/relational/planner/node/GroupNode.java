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
 * GroupNode is a auxiliary node that is used to group data. Currently, it is implemented based on
 * SortNode. It will only be generated some special node that required grouping source, such as
 * FillNode and TableFunctionNode.
 *
 * <p>GroupNode's ordering schema consists of two parts: PartitionKey and OrderKey. It guarantees to
 * return data grouped by PartitionKey and sorted by OrderKey. For example, PARTITION BY device_id
 * ORDER BY time will return data grouped by device_id, and in each group, data will be sorted by
 * time.
 */
public class GroupNode extends SortNode {

  /**
   * orderingScheme may include two parts: PartitionKey and OrderKey. It marks the number of
   * PartitionKey.
   */
  private final int partitionKeyCount;

  public GroupNode(PlanNodeId id, PlanNode child, OrderingScheme scheme, int partitionKeyCount) {
    super(id, child, scheme, false, false);
    this.partitionKeyCount = partitionKeyCount;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new GroupNode(
        id, Iterables.getOnlyElement(newChildren), orderingScheme, partitionKeyCount);
  }

  public int getPartitionKeyCount() {
    return partitionKeyCount;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitGroup(this, context);
  }

  @Override
  public PlanNode clone() {
    return new GroupNode(id, null, orderingScheme, partitionKeyCount);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_GROUP_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
    ReadWriteIOUtils.write(partitionKeyCount, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_GROUP_NODE.serialize(stream);
    orderingScheme.serialize(stream);
    ReadWriteIOUtils.write(partitionKeyCount, stream);
  }

  public static GroupNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    int partitionColumnCount = ReadWriteIOUtils.readInt(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GroupNode(planNodeId, null, orderingScheme, partitionColumnCount);
  }

  @Override
  public String toString() {
    return "GroupNode-" + this.getPlanNodeId();
  }
}
