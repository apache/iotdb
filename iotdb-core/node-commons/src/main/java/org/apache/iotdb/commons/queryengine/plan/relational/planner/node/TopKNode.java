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

package org.apache.iotdb.commons.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import com.google.common.base.Objects;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class TopKNode extends MultiChildProcessNode {

  private final OrderingScheme orderingScheme;

  private final long count;

  private final List<Symbol> outputSymbols;

  private final boolean childrenDataInOrder;

  private final transient boolean topKRuntimeFilterAscending;

  // Root TopK plan node id under which the shared runtime filter is registered/looked up; a
  // non-null value also marks this TopK as a runtime filter producer (set during distributed
  // optimize). All per-region TopK producers and their scan consumers within the same query use
  // this id so that fragments of the same query on one DataNode share a single filter instance.
  @Nullable private String topKRuntimeFilterSourceId;

  public TopKNode(
      PlanNodeId id,
      OrderingScheme scheme,
      long count,
      List<Symbol> outputSymbols,
      boolean childrenDataInOrder) {
    super(id);
    this.orderingScheme = scheme;
    this.count = count;
    this.outputSymbols = outputSymbols;
    this.childrenDataInOrder = childrenDataInOrder;
    this.topKRuntimeFilterAscending = computeTopKRuntimeFilterAscending(scheme);
  }

  public TopKNode(
      PlanNodeId id,
      List<PlanNode> children,
      OrderingScheme scheme,
      long count,
      List<Symbol> outputSymbols,
      boolean childrenDataInOrder) {
    super(id, children);
    this.orderingScheme = scheme;
    this.count = count;
    this.outputSymbols = outputSymbols;
    this.childrenDataInOrder = childrenDataInOrder;
    this.topKRuntimeFilterAscending = computeTopKRuntimeFilterAscending(scheme);
  }

  private static boolean computeTopKRuntimeFilterAscending(OrderingScheme orderingScheme) {
    Symbol orderBy = orderingScheme.getOrderBy().get(0);
    return orderingScheme.getOrdering(orderBy).isAscending();
  }

  @Override
  public PlanNode clone() {
    TopKNode cloned =
        new TopKNode(getPlanNodeId(), orderingScheme, count, outputSymbols, childrenDataInOrder);
    cloned.topKRuntimeFilterSourceId = topKRuntimeFilterSourceId;
    return cloned;
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((ICoreQueryPlanVisitor<R, C>) visitor).visitTopK(this, context);
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_TOPK_NODE.serialize(byteBuffer);
    orderingScheme.serialize(byteBuffer);
    ReadWriteIOUtils.write(count, byteBuffer);
    ReadWriteIOUtils.write(outputSymbols.size(), byteBuffer);
    for (Symbol symbol : outputSymbols) {
      Symbol.serialize(symbol, byteBuffer);
    }
    ReadWriteIOUtils.write(childrenDataInOrder, byteBuffer);
    ReadWriteIOUtils.write(topKRuntimeFilterSourceId, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_TOPK_NODE.serialize(stream);
    orderingScheme.serialize(stream);
    ReadWriteIOUtils.write(count, stream);
    ReadWriteIOUtils.write(outputSymbols.size(), stream);
    for (Symbol symbol : outputSymbols) {
      Symbol.serialize(symbol, stream);
    }
    ReadWriteIOUtils.write(childrenDataInOrder, stream);
    ReadWriteIOUtils.write(topKRuntimeFilterSourceId, stream);
  }

  public static TopKNode deserialize(ByteBuffer byteBuffer) {
    OrderingScheme orderingScheme = OrderingScheme.deserialize(byteBuffer);
    long count = ReadWriteIOUtils.readLong(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<Symbol> outputSymbols = new ArrayList<>(size);
    while (size-- > 0) {
      outputSymbols.add(Symbol.deserialize(byteBuffer));
    }
    boolean childrenDataInOrder = ReadWriteIOUtils.readBool(byteBuffer);
    String topKRuntimeFilterSourceId = ReadWriteIOUtils.readString(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    TopKNode topKNode =
        new TopKNode(planNodeId, orderingScheme, count, outputSymbols, childrenDataInOrder);
    topKNode.topKRuntimeFilterSourceId = topKRuntimeFilterSourceId;
    return topKNode;
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return outputSymbols;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    checkArgument(
        children.size() == newChildren.size(),
        QueryMessages.EXCEPTION_WRONG_NUMBER_OF_NEW_CHILDREN_817AF800);
    TopKNode topKNode =
        new TopKNode(id, newChildren, orderingScheme, count, outputSymbols, childrenDataInOrder);
    topKNode.topKRuntimeFilterSourceId = topKRuntimeFilterSourceId;
    return topKNode;
  }

  public OrderingScheme getOrderingScheme() {
    return orderingScheme;
  }

  public long getCount() {
    return count;
  }

  public boolean isChildrenDataInOrder() {
    return childrenDataInOrder;
  }

  public boolean isTopKRuntimeFilterAscending() {
    return topKRuntimeFilterAscending;
  }

  /** A non-null source id marks this TopK as a runtime filter producer. */
  @Nullable
  public String getTopKRuntimeFilterSourceId() {
    return topKRuntimeFilterSourceId;
  }

  public void setTopKRuntimeFilterSourceId(@Nullable String topKRuntimeFilterSourceId) {
    this.topKRuntimeFilterSourceId = topKRuntimeFilterSourceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    TopKNode sortNode = (TopKNode) o;
    return Objects.equal(orderingScheme, sortNode.orderingScheme)
        && Objects.equal(outputSymbols, sortNode.outputSymbols)
        && Objects.equal(count, sortNode.count)
        && Objects.equal(topKRuntimeFilterSourceId, sortNode.topKRuntimeFilterSourceId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), orderingScheme, outputSymbols, count, topKRuntimeFilterSourceId);
  }

  @Override
  public String toString() {
    return "TopKNode-" + this.getPlanNodeId();
  }
}
