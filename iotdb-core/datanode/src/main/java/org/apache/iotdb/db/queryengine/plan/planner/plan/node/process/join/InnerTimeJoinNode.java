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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This node is responsible for joining two or more TsBlock.
 *
 * <p>The join algorithm is <b>inner join</b> on timestamp column —— take the <b>intersection</b> of
 * all child timestamps as the time column of the result. The output result is sorted by timestamp.
 *
 * <p>e.g.
 *
 * <pre>
 *   [series1]  [series2]  [series1 join series2]
 *   time, s1   time, s2   time, s1, s2
 *      1,  1
 *      2,  2      2,  2      2,  2,  2
 *                 3,  3
 * </pre>
 */
public class InnerTimeJoinNode extends MultiChildProcessNode {

  // This parameter indicates the order when executing multiway merge sort.
  private final Ordering mergeOrder;

  // null for all time partitions
  // empty for zero time partitions
  private List<Long> timePartitions;

  // in most cases, it will be null, if we've got more than one root InnerTimeJoinNode, and these
  // InnerTimeJoinNodes' result will be merged by MergeSortNode
  // we will make sure that all these InnerTimeJoinNodes output same column names;
  // for logical planner, it will also be null
  private List<String> outputColumnNames;

  public InnerTimeJoinNode(PlanNodeId id, List<PlanNode> children, Ordering mergeOrder) {
    this(id, children, mergeOrder, null, null);
  }

  public InnerTimeJoinNode(
      PlanNodeId id,
      Ordering mergeOrder,
      List<Long> timePartitions,
      List<String> outputColumnNames) {
    super(id);
    this.mergeOrder = mergeOrder;
    this.timePartitions = timePartitions;
    this.outputColumnNames = outputColumnNames;
  }

  public InnerTimeJoinNode(
      PlanNodeId id,
      List<PlanNode> children,
      Ordering mergeOrder,
      List<Long> timePartitions,
      List<String> outputColumnNames) {
    super(id, children);
    this.mergeOrder = mergeOrder;
    this.timePartitions = timePartitions;
    this.outputColumnNames = outputColumnNames;
  }

  public Ordering getMergeOrder() {
    return mergeOrder;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.INNER_TIME_JOIN;
  }

  @Override
  public PlanNode clone() {
    return new InnerTimeJoinNode(
        getPlanNodeId(), getMergeOrder(), timePartitions, outputColumnNames);
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new InnerTimeJoinNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)),
        getMergeOrder(),
        timePartitions,
        null);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames != null
        ? outputColumnNames
        : children.stream()
            .map(PlanNode::getOutputColumnNames)
            .flatMap(List::stream)
            .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitInnerTimeJoin(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.INNER_TIME_JOIN.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), byteBuffer);
    if (timePartitions == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(timePartitions.size(), byteBuffer);
      for (Long timePartitionId : timePartitions) {
        ReadWriteIOUtils.write(timePartitionId, byteBuffer);
      }
    }
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.INNER_TIME_JOIN.serialize(stream);
    ReadWriteIOUtils.write(mergeOrder.ordinal(), stream);
    if (timePartitions == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(timePartitions.size(), stream);
      for (Long timePartitionId : timePartitions) {
        ReadWriteIOUtils.write(timePartitionId, stream);
      }
    }
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(outputColumnNames.size(), stream);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, stream);
      }
    }
  }

  public static InnerTimeJoinNode deserialize(ByteBuffer byteBuffer) {
    Ordering mergeOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    List<Long> timePartitionIds = null;
    boolean hasTimePartitionIds = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasTimePartitionIds) {
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      timePartitionIds = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        timePartitionIds.add(ReadWriteIOUtils.readLong(byteBuffer));
      }
    }
    List<String> outputColumnNames = null;
    boolean hasOutputColumnNames = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasOutputColumnNames) {
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      outputColumnNames = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new InnerTimeJoinNode(
        planNodeId, new ArrayList<>(), mergeOrder, timePartitionIds, outputColumnNames);
  }

  @Override
  public String toString() {
    return "InnerTimeJoinNode-" + this.getPlanNodeId();
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
    InnerTimeJoinNode that = (InnerTimeJoinNode) o;
    return mergeOrder == that.mergeOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrder);
  }

  public void setTimePartitions(List<Long> timePartitions) {
    this.timePartitions = timePartitions;
  }

  // null for all time partitions
  // empty for zero time partitions
  public Optional<List<Long>> getTimePartitions() {
    return Optional.ofNullable(timePartitions);
  }

  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public boolean outputColumnNamesIsNull() {
    return outputColumnNames == null;
  }
}
